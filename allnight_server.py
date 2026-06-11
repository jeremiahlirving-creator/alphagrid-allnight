import asyncio, os, json, logging, uuid
from contextlib import asynccontextmanager
from datetime import datetime, date, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import aiohttp

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("allnight_bot")

EST = ZoneInfo("America/New_York")

# ── PROP FIRM CONFIG ──────────────────────────────────────────────────────────
ACCOUNT_SIZE   = 50_000
MAX_RISK       = 500.0
MAX_DRAWDOWN   = -1_800
PROFIT_TARGET  = 3_000
MAX_DAY_PROFIT = 1_500

# ── AUTONOMOUS TRADE CONFIG ───────────────────────────────────────────────────
# Two-leg split per trade signal:
#   Leg 1 — 3 contracts, TP $200, SL $100  (base profit lock)
#   Leg 2 — 2 contracts, TP $300, SL $100  (runner)
LEG1_CONTRACTS = 3
LEG1_TP        = 200.0
LEG1_SL        = 100.0

LEG2_CONTRACTS = 2
LEG2_TP        = 300.0
LEG2_SL        = 100.0

MAX_DAY_LOSS   = -300.0   # bot goes silent after -$300 on the day

# Max theoretical win per signal: (3 * $200) + (2 * $300) = $1,200 (per-contract scaling via PMT dollar fields)
# Max theoretical loss per signal: (3 * $100) + (2 * $100) = $500

# ── WIN RATE ADAPTIVE FILTER ──────────────────────────────────────────────────
WIN_RATE_THRESHOLD  = 0.60   # below 60% triggers tight mode
WIN_RATE_MIN_TRADES = 10     # don't enforce until at least 10 trades

# ── INSTRUMENTS — MICRO CONTRACTS ────────────────────────────────────────────
INSTRUMENTS = {
    "MES": {"pmt": "MES", "point_value": 5.0,  "contracts": 5,
            "sweep_buf_normal": 1.5, "sweep_buf_tight": 2.5,
            "level_proximity_tight": 3.0,  "name": "Micro E-mini S&P"},
    "MNQ": {"pmt": "MNQ", "point_value": 2.0,  "contracts": 5,
            "sweep_buf_normal": 4.0, "sweep_buf_tight": 6.0,
            "level_proximity_tight": 8.0,  "name": "Micro E-mini Nasdaq"},
}

# ── SESSION WINDOWS (EST) ─────────────────────────────────────────────────────
SESSIONS = {
    "Asia":   (time(20, 0),  time(23, 59)),
    "London": (time(2, 0),   time(5, 0)),
    "NY_KZ":  (time(8, 0),   time(10, 30)),
}

# ── ENV ───────────────────────────────────────────────────────────────────────
PMT_URL     = os.getenv("PMT_WEBHOOK_URL",  "https://api.pickmytrade.trade/v2/add-trade-data-latest?t=18504")
PMT_TOKEN   = os.getenv("PMT_TOKEN",        "")
PMT_ACCOUNT = os.getenv("PMT_ACCOUNT_ID",   "53430171")
TG_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT     = os.getenv("TELEGRAM_CHAT_ID",   "")
TRADOVATE_ACCT = os.getenv("TRADOVATE_ACCOUNT_ID", "MFFUEVRPD505461063")

# ── STATE ─────────────────────────────────────────────────────────────────────
prices         = {"MES": 0.0, "MNQ": 0.0}
signals        = []
trades         = []
ws_clients     = []
last_signal    = {"MES": 0, "MNQ": 0}   # epoch timestamp

session_levels = {
    inst: {"PDH": None, "PDL": None, "AsiaH": None, "AsiaL": None,
           "LonH": None, "LonL": None, "PMH": None, "PML": None,
           "NYOpenH": None, "NYOpenL": None}
    for inst in INSTRUMENTS
}


# ── PROP + WIN-RATE STATS ─────────────────────────────────────────────────────
class PropStats:
    def __init__(self):
        self.total_pnl  = 0.0
        self.day_pnl    = 0.0
        self.day_date   = date.today()
        self.peak_pnl   = 0.0
        self.wins       = 0
        self.losses     = 0
        self.tight_mode = False   # adaptive filter active

    def _check_day_reset(self):
        today = date.today()
        if self.day_date != today:
            self.day_pnl  = 0.0
            self.day_date = today

    @property
    def total_trades(self):
        return self.wins + self.losses

    @property
    def win_rate(self):
        if self.total_trades == 0:
            return 1.0
        return self.wins / self.total_trades

    def update_tight_mode(self):
        if self.total_trades >= WIN_RATE_MIN_TRADES:
            was_tight = self.tight_mode
            self.tight_mode = self.win_rate < WIN_RATE_THRESHOLD
            if self.tight_mode and not was_tight:
                logger.warning(f"⚠️  Tight mode ON — win rate {self.win_rate:.1%} ({self.wins}W/{self.losses}L)")
            elif not self.tight_mode and was_tight:
                logger.info(f"✅  Tight mode OFF — win rate {self.win_rate:.1%} recovered")
        else:
            self.tight_mode = False

    def record(self, pnl: float) -> bool:
        """Record a closed trade. Returns True if account should lock."""
        self._check_day_reset()
        self.total_pnl += pnl
        self.day_pnl   += pnl
        if pnl > 0:
            self.wins += 1
        else:
            self.losses += 1
        self.update_tight_mode()
        if self.total_pnl > self.peak_pnl:
            self.peak_pnl = self.total_pnl
        return self.total_pnl <= MAX_DRAWDOWN

    def can_trade(self) -> tuple[bool, str]:
        """Returns (allowed, reason). Checks all kill conditions."""
        self._check_day_reset()
        if self.total_pnl <= MAX_DRAWDOWN:
            return False, f"Account drawdown limit hit (${self.total_pnl:.0f})"
        if self.day_pnl >= MAX_DAY_PROFIT:
            return False, f"Daily profit cap hit (${self.day_pnl:.0f})"
        if self.day_pnl <= MAX_DAY_LOSS:
            return False, f"Daily loss limit hit (${self.day_pnl:.0f})"
        return True, "ok"

    def status(self):
        return {
            "total_pnl":    round(self.total_pnl, 2),
            "day_pnl":      round(self.day_pnl, 2),
            "peak_pnl":     round(self.peak_pnl, 2),
            "wins":         self.wins,
            "losses":       self.losses,
            "win_rate":     round(self.win_rate * 100, 1),
            "total_trades": self.total_trades,
            "tight_mode":   self.tight_mode,
            "day_loss_remaining": round(MAX_DAY_LOSS - self.day_pnl, 2),
            "day_profit_remaining": round(MAX_DAY_PROFIT - self.day_pnl, 2),
        }


stats = PropStats()


# ── HELPERS ───────────────────────────────────────────────────────────────────
def get_session(now_est: datetime) -> Optional[str]:
    t = now_est.time()
    for name, (start, end) in SESSIONS.items():
        if start <= t <= end:
            return name
    return None


def get_nearest_level(inst: str, price: float) -> Optional[float]:
    """Return nearest session level to current price, or None."""
    lvls = [v for v in session_levels[inst].values() if v is not None]
    if not lvls:
        return None
    return min(lvls, key=lambda x: abs(x - price))


def level_proximity_ok(inst: str, price: float) -> bool:
    """In tight mode: price must be within level_proximity_tight of a key level."""
    if not stats.tight_mode:
        return True
    nearest = get_nearest_level(inst, price)
    if nearest is None:
        return False
    cfg = INSTRUMENTS[inst]
    return abs(price - nearest) <= cfg["level_proximity_tight"]


async def broadcast(msg: dict):
    dead = []
    for ws in ws_clients:
        try:
            await ws.send_text(json.dumps(msg))
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            ws_clients.remove(ws)
        except ValueError:
            pass


async def send_telegram(text: str, parse_mode="Markdown"):
    if not TG_TOKEN or not TG_CHAT:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json={
                "chat_id": TG_CHAT, "text": text, "parse_mode": parse_mode
            }, timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logger.warning(f"Telegram error: {e}")


# ── PMT WEBHOOK ───────────────────────────────────────────────────────────────
async def _send_pmt(inst: str, direction: str, qty: int,
                    dollar_tp: float, dollar_sl: float,
                    strategy_suffix: str) -> tuple[bool, str]:
    """Fire a single PMT webhook for one leg."""
    cfg = INSTRUMENTS[inst]
    payload = {
        "token":                 PMT_TOKEN,
        "strategy_name":         f"AlphaGrid_{inst}_{strategy_suffix}",
        "ticker":                cfg["pmt"],
        "action":                direction.lower(),
        "quantity_multiplier":   1,
        "breakeven_offset":      0,
        "same_direction_ignore": False,   # both legs must open independently
        "dollar_tp":             dollar_tp,
        "dollar_sl":             dollar_sl,
        "multiple_accounts": [{
            "account_id":     TRADOVATE_ACCT,
            "pmt_account_id": PMT_ACCOUNT,
            "quantity":       qty,
        }]
    }
    headers = {
        "Content-Type": "application/json",
        "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Origin":       "https://www.pickmytrade.trade",
        "Referer":      "https://www.pickmytrade.trade/",
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(PMT_URL, json=payload, headers=headers,
                              timeout=aiohttp.ClientTimeout(total=10)) as r:
                body = await r.text()
                ok   = r.status == 200 and "success" in body.lower()
                return ok, body
    except Exception as e:
        return False, str(e)


async def fire_trade_legs(sig: dict) -> tuple[bool, bool, str, str]:
    """
    Fire both legs simultaneously.
    Returns (leg1_ok, leg2_ok, leg1_body, leg2_body).
    """
    inst      = sig["inst"]
    direction = sig["direction"]
    leg1, leg2 = await asyncio.gather(
        _send_pmt(inst, direction, LEG1_CONTRACTS, LEG1_TP, LEG1_SL, "L1"),
        _send_pmt(inst, direction, LEG2_CONTRACTS, LEG2_TP, LEG2_SL, "L2"),
    )
    return leg1[0], leg2[0], leg1[1], leg2[1]


# ── SIGNAL ENGINE ─────────────────────────────────────────────────────────────
async def check_signals(inst: str, price: float, now: datetime):
    session = get_session(now)
    if not session:
        return

    cfg      = INSTRUMENTS[inst]
    levels   = session_levels[inst]
    cooldown = 1200 if stats.tight_mode else 600   # 20min tight / 10min normal
    buf      = cfg["sweep_buf_tight"] if stats.tight_mode else cfg["sweep_buf_normal"]

    # Cooldown gate
    if (now.timestamp() - last_signal[inst]) < cooldown:
        return

    # Proximity gate (tight mode only)
    if not level_proximity_ok(inst, price):
        return

    direction = None
    swept_level = None

    # ── Bullish sweep: price dips below a LOW level then recovers
    low_levels = {k: v for k, v in levels.items()
                  if v is not None and k in ("AsiaL", "LonL", "PDL", "PML", "NYOpenL")}
    for k, lvl in low_levels.items():
        if price < lvl - buf:          # swept below
            direction   = "BUY"
            swept_level = (k, lvl)
            break

    # ── Bearish sweep: price spikes above a HIGH level then rejects
    if not direction:
        high_levels = {k: v for k, v in levels.items()
                       if v is not None and k in ("AsiaH", "LonH", "PDH", "PMH", "NYOpenH")}
        for k, lvl in high_levels.items():
            if price > lvl + buf:
                direction   = "SELL"
                swept_level = (k, lvl)
                break

    if not direction or not swept_level:
        return

    # Build signal
    stop_pts = 10 if inst == "MES" else 20
    tp_pts   = stop_pts * (DOLLAR_TP / DOLLAR_SL)   # maintain R-multiple geometrically
    if direction == "BUY":
        stop = price - stop_pts
        tp1  = price + tp_pts
    else:
        stop = price + stop_pts
        tp1  = price - tp_pts

    sig = {
        "id":          str(uuid.uuid4())[:8],
        "inst":        inst,
        "direction":   direction,
        "entry":       price,
        "stop":        stop,
        "tp1":         tp1,
        "leg1_tp":     LEG1_TP,
        "leg1_sl":     LEG1_SL,
        "leg1_qty":    LEG1_CONTRACTS,
        "leg2_tp":     LEG2_TP,
        "leg2_sl":     LEG2_SL,
        "leg2_qty":    LEG2_CONTRACTS,
        "contracts":   LEG1_CONTRACTS + LEG2_CONTRACTS,
        "session":     session,
        "swept":       swept_level[0],
        "tight_mode":  stats.tight_mode,
        "ts":          now.strftime("%H:%M:%S"),
    }

    last_signal[inst] = now.timestamp()

    # ── AUTO-EXECUTE ──────────────────────────────────────────────────────────
    allowed, reason = stats.can_trade()
    if not allowed:
        logger.info(f"🚫 Signal blocked [{inst}]: {reason}")
        await send_telegram(
            f"🚫 *Signal blocked* — {inst} {direction}\n"
            f"Reason: {reason}"
        )
        return

    logger.info(
        f"🚀 Auto-executing {inst} {direction} @ {price:.2f} | "
        f"L1: {LEG1_CONTRACTS}ct TP${LEG1_TP}/SL${LEG1_SL}  "
        f"L2: {LEG2_CONTRACTS}ct TP${LEG2_TP}/SL${LEG2_SL} | "
        f"Tight={stats.tight_mode}"
    )
    l1_ok, l2_ok, l1_body, l2_body = await fire_trade_legs(sig)

    if l1_ok or l2_ok:
        trades.insert(0, {**sig, "status": "EXECUTED",
                           "l1_ok": l1_ok, "l2_ok": l2_ok,
                           "executed_at": now.strftime("%H:%M:%S")})
        await broadcast({"type": "executed", "sig": sig, "stats": stats.status()})
        mode_tag = "🔒 TIGHT" if stats.tight_mode else "✅ NORMAL"
        wr_str   = f"{stats.win_rate:.0%} ({stats.wins}W/{stats.losses}L)" if stats.total_trades else "—"
        l1_tag   = "✅" if l1_ok else "❌"
        l2_tag   = "✅" if l2_ok else "❌"
        await send_telegram(
            f"🤖 *Auto-Trade Fired* [{mode_tag}]\n"
            f"`{inst}` {direction} @ `{price:,.2f}`\n"
            f"{l1_tag} Leg 1: `{LEG1_CONTRACTS}ct` TP `+${LEG1_TP:.0f}` SL `-${LEG1_SL:.0f}`\n"
            f"{l2_tag} Leg 2: `{LEG2_CONTRACTS}ct` TP `+${LEG2_TP:.0f}` SL `-${LEG2_SL:.0f}` (runner)\n"
            f"Session: {session} | Swept: {swept_level[0]}\n"
            f"Win Rate: {wr_str} | Day P&L: `${stats.day_pnl:+.0f}`"
        )
        if not l1_ok:
            logger.error(f"Leg 1 failed [{inst}]: {l1_body}")
        if not l2_ok:
            logger.error(f"Leg 2 failed [{inst}]: {l2_body}")
    else:
        logger.error(f"Both legs failed [{inst}]: L1={l1_body} L2={l2_body}")
        await send_telegram(
            f"⚠️ *Both legs failed* — {inst} {direction}\n"
            f"L1: `{l1_body[:100]}`\nL2: `{l2_body[:100]}`"
        )


# ── LIFESPAN ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("AlphaGrid All Night Bot starting — autonomous mode 🤖")
    logger.info(f"L1: {LEG1_CONTRACTS}ct TP${LEG1_TP}/SL${LEG1_SL} | L2: {LEG2_CONTRACTS}ct TP${LEG2_TP}/SL${LEG2_SL} | DayLoss cap=${MAX_DAY_LOSS}")
    yield
    logger.info("Bot shutting down")


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    s = stats.status()
    allowed, reason = stats.can_trade()
    return {
        "status":     "ok",
        "trading":    allowed,
        "reason":     reason,
        "tight_mode": stats.tight_mode,
        **s,
        "prices":     prices,
    }


@app.post("/price-update")
async def price_update(req: Request):
    body = await req.json()
    inst  = body.get("ticker", "").upper().replace("1!", "").replace("!", "")
    price = float(body.get("price", 0))

    if inst not in INSTRUMENTS or price <= 0:
        return {"ok": False, "reason": "unknown instrument"}

    prices[inst] = price
    now = datetime.now(EST)
    await check_signals(inst, price, now)
    await broadcast({"type": "price", "inst": inst, "price": price,
                     "tight_mode": stats.tight_mode, "stats": stats.status()})
    return {"ok": True, "inst": inst, "price": price}


class LevelPayload(BaseModel):
    inst:    str
    PDH:     Optional[float] = None
    PDL:     Optional[float] = None
    AsiaH:   Optional[float] = None
    AsiaL:   Optional[float] = None
    LonH:    Optional[float] = None
    LonL:    Optional[float] = None
    PMH:     Optional[float] = None
    PML:     Optional[float] = None
    NYOpenH: Optional[float] = None
    NYOpenL: Optional[float] = None


@app.post("/levels")
async def set_levels(p: LevelPayload):
    inst = p.inst.upper()
    if inst not in session_levels:
        return {"ok": False, "reason": "unknown instrument"}
    for k in session_levels[inst]:
        v = getattr(p, k, None)
        if v is not None:
            session_levels[inst][k] = v
    logger.info(f"Levels updated [{inst}]: {session_levels[inst]}")
    await broadcast({"type": "levels", "inst": inst, "levels": session_levels[inst]})
    return {"ok": True, "levels": session_levels[inst]}


class ResultPayload(BaseModel):
    pnl: float
    won: bool


@app.post("/result")
async def record_result(p: ResultPayload):
    locked = stats.record(p.pnl)
    s = stats.status()
    await broadcast({"type": "result", "pnl": p.pnl, "stats": s})
    if locked:
        await send_telegram(
            f"⛔ *ACCOUNT LOCKED*\n"
            f"Total drawdown: `${stats.total_pnl:.0f}`\nBot paused until manual reset."
        )
    elif stats.tight_mode:
        await send_telegram(
            f"⚠️ *Tight Mode Active*\n"
            f"Win rate: `{stats.win_rate:.0%}` ({stats.wins}W/{stats.losses}L)\n"
            f"Entry filters tightened until win rate recovers above 60%."
        )
    return s


@app.post("/reset_day")
async def reset_day():
    stats.day_pnl  = 0.0
    stats.day_date = date.today()
    signals.clear()
    logger.info("Day stats reset")
    return {"ok": True}


@app.post("/reset_stats")
async def reset_stats():
    """Full reset — wins, losses, win rate, P&L."""
    stats.__init__()
    logger.info("Full stats reset")
    return {"ok": True}


@app.get("/stats")
async def get_stats():
    allowed, reason = stats.can_trade()
    return {**stats.status(), "trading_allowed": allowed, "reason": reason}


@app.websocket("/ws")
async def ws_ep(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)
    await ws.send_text(json.dumps({
        "type":        "init",
        "prices":      prices,
        "trades":      trades[:20],
        "stats":       stats.status(),
        "levels":      session_levels,
        "config": {
            "leg1_contracts": LEG1_CONTRACTS,
            "leg1_tp":        LEG1_TP,
            "leg1_sl":        LEG1_SL,
            "leg2_contracts": LEG2_CONTRACTS,
            "leg2_tp":        LEG2_TP,
            "leg2_sl":        LEG2_SL,
            "max_day_loss":   MAX_DAY_LOSS,
            "win_rate_threshold": WIN_RATE_THRESHOLD,
        }
    }))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        try:
            ws_clients.remove(ws)
        except ValueError:
            pass
