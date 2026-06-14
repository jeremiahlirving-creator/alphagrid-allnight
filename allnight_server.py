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
MAX_DRAWDOWN   = -1_800
PROFIT_TARGET  =  3_000
MAX_DAY_PROFIT =  1_500

# ── TRADE CONFIG ──────────────────────────────────────────────────────────────
LEG1_CONTRACTS = 3;  LEG1_TP = 200.0;  LEG1_SL = 100.0
LEG2_CONTRACTS = 2;  LEG2_TP = 300.0;  LEG2_SL = 100.0
MAX_DAY_LOSS   = -300.0

# ── WIN RATE ADAPTIVE FILTER ──────────────────────────────────────────────────
WIN_RATE_THRESHOLD  = 0.60
WIN_RATE_MIN_TRADES = 10

# ── INSTRUMENTS ───────────────────────────────────────────────────────────────
INSTRUMENTS = {
    "MES": {"pmt": "MES", "point_value": 5.0,
            "sweep_buf_normal": 1.5, "sweep_buf_tight": 2.5,
            "level_proximity_tight": 3.0},
    "MNQ": {"pmt": "MNQ", "point_value": 2.0,
            "sweep_buf_normal": 4.0, "sweep_buf_tight": 6.0,
            "level_proximity_tight": 8.0},
}

# ── SESSION WINDOWS (ET) ──────────────────────────────────────────────────────
SESSIONS = {
    "Asia":   (time(20, 0),  time(23, 59)),
    "London": (time(2,  0),  time(5,  0)),
    "NY_KZ":  (time(8,  0),  time(10, 30)),
}

# ── ENV ───────────────────────────────────────────────────────────────────────
PMT_URL        = os.getenv("PMT_WEBHOOK_URL",     "https://api.pickmytrade.trade/v2/add-trade-data-latest?t=18504")
PMT_TOKEN      = os.getenv("PMT_TOKEN",           "")
PMT_ACCOUNT    = os.getenv("PMT_ACCOUNT_ID",      "53430171")
TG_TOKEN       = os.getenv("TELEGRAM_BOT_TOKEN",  "")
TG_CHAT        = os.getenv("TELEGRAM_CHAT_ID",    "")
TRADOVATE_ACCT = os.getenv("TRADOVATE_ACCOUNT_ID","MFFUEVBLDR505461067")

# ── RUNTIME STATE ─────────────────────────────────────────────────────────────
prices      = {"MES": 0.0, "MNQ": 0.0}
trades      = []
ws_clients  = []
last_signal = {"MES": 0.0, "MNQ": 0.0}

# ── HTF LEVEL STORE ───────────────────────────────────────────────────────────
# Levels are pushed by TradingView Pine Script alerts — NOT inferred from
# 1-min ticks. PDH/PDL come from 1H chart. Asia/London H/L from 15M chart.
# Source of truth: actual OHLC wicks at the correct timeframe.

class HTFLevelStore:
    """
    Receives confirmed HTF levels from TradingView webhook alerts.
    PDH/PDL  → 1H chart alert fires at midnight ET (prior day range)
    AsiaH/L  → 15M chart alert fires at 12:00 AM ET (Asia session close)
    LonH/L   → 15M chart alert fires at 5:00 AM ET (London session close)

    Manual override via /levels endpoint always respected.
    """
    def __init__(self):
        self.levels = {
            inst: {"PDH": None, "PDL": None,
                   "AsiaH": None, "AsiaL": None,
                   "LonH":  None, "LonL":  None}
            for inst in INSTRUMENTS
        }
        self.last_updated = {inst: {} for inst in INSTRUMENTS}
        self._last_reset  = date.today()

    def midnight_reset(self):
        """Clear intraday levels at midnight — PDH/PDL persist (rolled from yesterday)."""
        for inst in INSTRUMENTS:
            self.levels[inst]["AsiaH"] = None
            self.levels[inst]["AsiaL"] = None
            self.levels[inst]["LonH"]  = None
            self.levels[inst]["LonL"]  = None
        self._last_reset = date.today()
        logger.info("🔄 Midnight reset — Asia/London levels cleared, PDH/PDL retained")

    def set(self, inst: str, key: str, value: float, source: str = "auto"):
        self.levels[inst][key] = value
        self.last_updated[inst][key] = {
            "value":  value,
            "source": source,   # "1H_pine" | "15M_pine" | "manual"
            "ts":     datetime.now(EST).strftime("%H:%M ET"),
        }
        logger.info(f"📐 {inst} {key} = {value:.2f} [{source}]")

    def set_many(self, inst: str, data: dict, source: str):
        for k, v in data.items():
            if v is not None and k in self.levels[inst]:
                self.set(inst, k, v, source)

    def get(self, inst: str) -> dict:
        return self.levels[inst]

    def all_status(self) -> dict:
        return {
            inst: {
                "levels":       self.levels[inst],
                "last_updated": self.last_updated[inst],
            }
            for inst in INSTRUMENTS
        }

    def check_midnight_reset(self):
        today = date.today()
        if today != self._last_reset:
            self.midnight_reset()

store = HTFLevelStore()

# ── PROP + WIN-RATE STATS ─────────────────────────────────────────────────────
class PropStats:
    def __init__(self):
        self.total_pnl        = 0.0
        self.day_pnl          = 0.0
        self.day_date         = date.today()
        self.peak_pnl         = 0.0
        self.wins             = 0
        self.losses           = 0
        self.tight_mode       = False
        self.yesterday_pnl    = 0.0
        self.yesterday_wins   = 0
        self.yesterday_losses = 0
        self.day_trades_log   = []

    def _check_day_reset(self):
        today = date.today()
        if self.day_date != today:
            self.yesterday_pnl    = self.day_pnl
            self.yesterday_wins   = sum(1 for t in self.day_trades_log if t["pnl"] > 0)
            self.yesterday_losses = sum(1 for t in self.day_trades_log if t["pnl"] <= 0)
            self.day_pnl        = 0.0
            self.day_date       = today
            self.day_trades_log = []

    @property
    def total_trades(self): return self.wins + self.losses

    @property
    def win_rate(self):
        return self.wins / self.total_trades if self.total_trades else 1.0

    def update_tight_mode(self):
        if self.total_trades >= WIN_RATE_MIN_TRADES:
            was = self.tight_mode
            self.tight_mode = self.win_rate < WIN_RATE_THRESHOLD
            if self.tight_mode and not was:
                logger.warning(f"⚠️ Tight mode ON — {self.win_rate:.1%} WR")
            elif not self.tight_mode and was:
                logger.info(f"✅ Tight mode OFF — {self.win_rate:.1%} WR")
        else:
            self.tight_mode = False

    def record(self, pnl: float, meta: dict = None) -> bool:
        self._check_day_reset()
        self.total_pnl += pnl
        self.day_pnl   += pnl
        if pnl > 0: self.wins   += 1
        else:       self.losses += 1
        self.update_tight_mode()
        if self.total_pnl > self.peak_pnl:
            self.peak_pnl = self.total_pnl
        if meta:
            self.day_trades_log.append({**meta, "pnl": pnl})
        return self.total_pnl <= MAX_DRAWDOWN

    def can_trade(self) -> tuple[bool, str]:
        self._check_day_reset()
        if self.total_pnl  <= MAX_DRAWDOWN:   return False, f"Account drawdown hit (${self.total_pnl:.0f})"
        if self.day_pnl    >= MAX_DAY_PROFIT:  return False, f"Daily profit cap hit (${self.day_pnl:.0f})"
        if self.day_pnl    <= MAX_DAY_LOSS:    return False, f"Daily loss limit hit (${self.day_pnl:.0f})"
        return True, "ok"

    def status(self):
        return {
            "total_pnl":            round(self.total_pnl, 2),
            "day_pnl":              round(self.day_pnl, 2),
            "peak_pnl":             round(self.peak_pnl, 2),
            "wins":                 self.wins,
            "losses":               self.losses,
            "win_rate":             round(self.win_rate * 100, 1),
            "total_trades":         self.total_trades,
            "tight_mode":           self.tight_mode,
            "day_loss_remaining":   round(MAX_DAY_LOSS   - self.day_pnl, 2),
            "day_profit_remaining": round(MAX_DAY_PROFIT - self.day_pnl, 2),
            "yesterday_pnl":        round(self.yesterday_pnl, 2),
        }

stats = PropStats()

# ── HELPERS ───────────────────────────────────────────────────────────────────
def get_session(now_et: datetime) -> Optional[str]:
    t = now_et.time()
    for name, (start, end) in SESSIONS.items():
        if start <= t <= end:
            return name
    return None

def fmt_level(v) -> str:
    return f"`{v:,.2f}`" if v else "`—`"

async def broadcast(msg: dict):
    dead = []
    for ws in ws_clients:
        try:    await ws.send_text(json.dumps(msg))
        except: dead.append(ws)
    for ws in dead:
        try:    ws_clients.remove(ws)
        except: pass

async def send_telegram(text: str, parse_mode="Markdown"):
    if not TG_TOKEN or not TG_CHAT:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json={"chat_id": TG_CHAT, "text": text,
                                    "parse_mode": parse_mode},
                         timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logger.warning(f"Telegram error: {e}")

def level_proximity_ok(inst: str, price: float) -> bool:
    if not stats.tight_mode:
        return True
    lvls = [v for v in store.get(inst).values() if v is not None]
    if not lvls:
        return False
    nearest = min(lvls, key=lambda x: abs(x - price))
    return abs(price - nearest) <= INSTRUMENTS[inst]["level_proximity_tight"]

# ── SCHEDULED REPORTS ─────────────────────────────────────────────────────────
async def report_6am():
    lvl_mes = store.get("MES")
    lvl_mnq = store.get("MNQ")
    wr_str  = f"{stats.win_rate:.0%} ({stats.wins}W / {stats.losses}L)" if stats.total_trades else "No trades yet"
    allowed, reason = stats.can_trade()
    text = (
        f"☀️ *AlphaGrid Morning Report* — {datetime.now(EST).strftime('%b %d, %Y')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📐 *HTF Levels* _(1H: PDH/PDL · 15M: Asia/London)_\n"
        f"*MES / ES*\n"
        f"  PDH {fmt_level(lvl_mes['PDH'])}  PDL {fmt_level(lvl_mes['PDL'])}\n"
        f"  AsiaH {fmt_level(lvl_mes['AsiaH'])}  AsiaL {fmt_level(lvl_mes['AsiaL'])}\n"
        f"  LonH {fmt_level(lvl_mes['LonH'])}  LonL {fmt_level(lvl_mes['LonL'])}\n\n"
        f"*MNQ / NQ*\n"
        f"  PDH {fmt_level(lvl_mnq['PDH'])}  PDL {fmt_level(lvl_mnq['PDL'])}\n"
        f"  AsiaH {fmt_level(lvl_mnq['AsiaH'])}  AsiaL {fmt_level(lvl_mnq['AsiaL'])}\n"
        f"  LonH {fmt_level(lvl_mnq['LonH'])}  LonL {fmt_level(lvl_mnq['LonL'])}\n\n"
        f"📊 *Yesterday*\n"
        f"  P&L: `${stats.yesterday_pnl:+.2f}` | "
        f"{stats.yesterday_wins + stats.yesterday_losses} trades "
        f"({stats.yesterday_wins}W / {stats.yesterday_losses}L)\n\n"
        f"🎯 *Account*\n"
        f"  Total P&L: `${stats.total_pnl:+.2f}` / `$3,000` target\n"
        f"  Win Rate: `{wr_str}`\n"
        f"  Mode: `{'🔒 TIGHT' if stats.tight_mode else '✅ NORMAL'}`\n\n"
        f"{'✅ Bot ARMED — NY KZ opens 8:00 AM ET' if allowed else f'🚫 Bot PAUSED — {reason}'}"
    )
    await send_telegram(text)
    logger.info("📬 6 AM report sent")

async def report_755am():
    lvl_mes = store.get("MES")
    lvl_mnq = store.get("MNQ")
    allowed, reason = stats.can_trade()
    text = (
        f"⚡ *Pre-Session Brief* — NY Kill Zone in 5 mins\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎯 *Active Levels (NY KZ)*\n"
        f"*MES:* PDH {fmt_level(lvl_mes['PDH'])} PDL {fmt_level(lvl_mes['PDL'])} "
        f"AsiaH {fmt_level(lvl_mes['AsiaH'])} AsiaL {fmt_level(lvl_mes['AsiaL'])} "
        f"LonH {fmt_level(lvl_mes['LonH'])} LonL {fmt_level(lvl_mes['LonL'])}\n"
        f"*MNQ:* PDH {fmt_level(lvl_mnq['PDH'])} PDL {fmt_level(lvl_mnq['PDL'])} "
        f"AsiaH {fmt_level(lvl_mnq['AsiaH'])} AsiaL {fmt_level(lvl_mnq['AsiaL'])} "
        f"LonH {fmt_level(lvl_mnq['LonH'])} LonL {fmt_level(lvl_mnq['LonL'])}\n\n"
        f"🛡️ *Kill Conditions*\n"
        f"  Day P&L: `${stats.day_pnl:+.2f}`\n"
        f"  Loss room: `${MAX_DAY_LOSS - stats.day_pnl:.2f}` remaining\n"
        f"  Profit room: `${MAX_DAY_PROFIT - stats.day_pnl:.2f}` remaining\n"
        f"  Mode: `{'🔒 TIGHT' if stats.tight_mode else '✅ NORMAL'}`\n\n"
        f"{'🟢 BOT LIVE — trading automatically' if allowed else f'🔴 BOT PAUSED — {reason}'}"
    )
    await send_telegram(text)
    logger.info("📬 7:55 AM brief sent")

# ── SCHEDULER ─────────────────────────────────────────────────────────────────
async def scheduler():
    sent_6am = False
    sent_755 = False
    last_date = date.today()
    while True:
        await asyncio.sleep(30)
        now   = datetime.now(EST)
        today = now.date()
        if today != last_date:
            sent_6am  = False
            sent_755  = False
            last_date = today
            store.check_midnight_reset()
        h, m = now.hour, now.minute
        if h == 6  and m == 0  and not sent_6am: sent_6am = True; await report_6am()
        if h == 7  and m == 55 and not sent_755: sent_755 = True; await report_755am()

# ── PMT WEBHOOK ───────────────────────────────────────────────────────────────
async def _send_pmt(inst: str, direction: str, qty: int,
                    dollar_tp: float, dollar_sl: float,
                    suffix: str) -> tuple[bool, str]:
    cfg = INSTRUMENTS[inst]
    payload = {
        "symbol":                f"{cfg['pmt']}1!",
        "strategy_name":         f"AlphaGrid_{inst}_{suffix}",
        "date":                  datetime.now(EST).strftime("%Y-%m-%dT%H:%M:%S"),
        "data":                  direction.lower(),
        "quantity":              str(qty),
        "risk_percentage":       0,
        "price":                 str(prices.get(inst, 0)),
        "tp":                    0,
        "percentage_tp":         0,
        "dollar_tp":             dollar_tp,
        "sl":                    0,
        "dollar_sl":             dollar_sl,
        "percentage_sl":         0,
        "trail":                 0,
        "trail_stop":            0,
        "trail_trigger":         0,
        "trail_freq":            0,
        "update_tp":             False,
        "update_sl":             False,
        "breakeven":             0,
        "breakeven_offset":      0,
        "token":                 PMT_TOKEN,
        "pyramid":               True,
        "same_direction_ignore": False,
        "reverse_order_close":   False,
        "multiple_accounts": [{
            "token":               PMT_TOKEN,
            "account_id":          TRADOVATE_ACCT,
            "risk_percentage":     0,
            "quantity_multiplier": 1,
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
                return r.status == 200 and "success" in body.lower(), body
    except Exception as e:
        return False, str(e)

async def fire_trade_legs(sig: dict) -> tuple[bool, bool, str, str]:
    leg1, leg2 = await asyncio.gather(
        _send_pmt(sig["inst"], sig["direction"], LEG1_CONTRACTS, LEG1_TP, LEG1_SL, "L1"),
        _send_pmt(sig["inst"], sig["direction"], LEG2_CONTRACTS, LEG2_TP, LEG2_SL, "L2"),
    )
    return leg1[0], leg2[0], leg1[1], leg2[1]

# ── SIGNAL ENGINE ─────────────────────────────────────────────────────────────
async def check_signals(inst: str, price: float, now: datetime):
    session = get_session(now)
    if not session:
        return

    cfg    = INSTRUMENTS[inst]
    levels = store.get(inst)
    buf    = cfg["sweep_buf_tight"] if stats.tight_mode else cfg["sweep_buf_normal"]
    cooldown = 1200 if stats.tight_mode else 600

    if (now.timestamp() - last_signal[inst]) < cooldown:
        return
    if not level_proximity_ok(inst, price):
        return

    # PDH/PDL + AsiaH/L always active
    # LonH/L only during NY_KZ — NY hunts London stops after open
    base_lows  = ["AsiaL", "PDL"]
    base_highs = ["AsiaH", "PDH"]
    if session == "NY_KZ":
        base_lows.append("LonL")
        base_highs.append("LonH")

    direction   = None
    swept_level = None

    for k in base_lows:
        lvl = levels.get(k)
        if lvl and price < lvl - buf:
            direction, swept_level = "BUY", (k, lvl)
            break

    if not direction:
        for k in base_highs:
            lvl = levels.get(k)
            if lvl and price > lvl + buf:
                direction, swept_level = "SELL", (k, lvl)
                break

    if not direction:
        return

    stop_pts = 10 if inst == "MES" else 20
    stop = price - stop_pts if direction == "BUY" else price + stop_pts
    tp1  = price + stop_pts * 1.5 if direction == "BUY" else price - stop_pts * 1.5

    sig = {
        "id":         str(uuid.uuid4())[:8],
        "inst":       inst,
        "direction":  direction,
        "entry":      price,
        "stop":       stop,
        "tp1":        tp1,
        "leg1_tp":    LEG1_TP, "leg1_sl": LEG1_SL, "leg1_qty": LEG1_CONTRACTS,
        "leg2_tp":    LEG2_TP, "leg2_sl": LEG2_SL, "leg2_qty": LEG2_CONTRACTS,
        "contracts":  LEG1_CONTRACTS + LEG2_CONTRACTS,
        "session":    session,
        "swept":      swept_level[0],
        "tight_mode": stats.tight_mode,
        "ts":         now.strftime("%H:%M ET"),
    }
    last_signal[inst] = now.timestamp()

    allowed, reason = stats.can_trade()
    if not allowed:
        logger.info(f"🚫 Blocked [{inst}]: {reason}")
        await send_telegram(f"🚫 *Signal blocked* — {inst} {direction}\n_{reason}_")
        return

    logger.info(f"🚀 {inst} {direction} @ {price:.2f} | Swept {swept_level[0]} | Tight={stats.tight_mode}")
    l1_ok, l2_ok, l1_body, l2_body = await fire_trade_legs(sig)

    if l1_ok or l2_ok:
        trades.insert(0, {**sig, "status": "EXECUTED",
                          "l1_ok": l1_ok, "l2_ok": l2_ok,
                          "executed_at": now.strftime("%H:%M ET")})
        await broadcast({"type": "executed", "sig": sig, "stats": stats.status()})
        mode_tag = "🔒 TIGHT" if stats.tight_mode else "✅ NORMAL"
        wr_str   = f"{stats.win_rate:.0%} ({stats.wins}W/{stats.losses}L)" if stats.total_trades else "—"
        await send_telegram(
            f"🤖 *Auto-Trade Fired* [{mode_tag}]\n"
            f"`{inst}` {direction} @ `{price:,.2f}`\n"
            f"{'✅' if l1_ok else '❌'} L1: `{LEG1_CONTRACTS}ct` TP `+${LEG1_TP:.0f}` SL `-${LEG1_SL:.0f}`\n"
            f"{'✅' if l2_ok else '❌'} L2: `{LEG2_CONTRACTS}ct` TP `+${LEG2_TP:.0f}` SL `-${LEG2_SL:.0f}` (runner)\n"
            f"Session: {session} | Swept: {swept_level[0]}\n"
            f"Win Rate: {wr_str} | Day P&L: `${stats.day_pnl:+.0f}`"
        )
        if not l1_ok: logger.error(f"L1 failed [{inst}]: {l1_body}")
        if not l2_ok: logger.error(f"L2 failed [{inst}]: {l2_body}")
    else:
        logger.error(f"Both legs failed [{inst}]")
        await send_telegram(
            f"⚠️ *Both legs failed* — {inst} {direction}\n"
            f"L1: `{l1_body[:100]}`\nL2: `{l2_body[:100]}`"
        )

# ── LIFESPAN ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(scheduler())
    logger.info("AlphaGrid All Night Bot — fully autonomous 🤖")
    logger.info("Levels: 1H Pine → PDH/PDL | 15M Pine → Asia/London H/L")
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    allowed, reason = stats.can_trade()
    now = datetime.now(EST)
    return {
        "status":     "ok",
        "trading":    allowed,
        "reason":     reason,
        "session":    get_session(now),
        "tight_mode": stats.tight_mode,
        "prices":     prices,
        "levels":     store.all_status(),
        **stats.status(),
    }

@app.post("/price-update")
async def price_update(req: Request):
    body  = await req.json()
    inst  = body.get("ticker", "").upper().replace("1!", "").replace("!", "")
    price = float(body.get("price", 0))
    if inst not in INSTRUMENTS or price <= 0:
        return {"ok": False}
    prices[inst] = price
    now = datetime.now(EST)
    store.check_midnight_reset()
    await check_signals(inst, price, now)
    await broadcast({"type": "price", "inst": inst, "price": price,
                     "levels": store.get(inst), "stats": stats.status()})
    return {"ok": True, "inst": inst, "price": price}

# ── HTF LEVEL PUSH FROM PINE SCRIPT ──────────────────────────────────────────
# TradingView Pine Script alerts call this endpoint directly.
# Payload format (JSON):
#   { "inst": "MES", "PDH": 5580.25, "PDL": 5510.00 }          ← 1H alert
#   { "inst": "MES", "AsiaH": 5545.00, "AsiaL": 5520.75 }      ← 15M Asia close
#   { "inst": "MES", "LonH": 5558.50, "LonL": 5531.25 }        ← 15M London close
#   { "inst": "MNQ", "PDH": 19800.0, "PDL": 19350.0 }          ← 1H alert
#   etc.
# source field is auto-tagged based on which keys are present.

class HTFPayload(BaseModel):
    inst:  str
    PDH:   Optional[float] = None
    PDL:   Optional[float] = None
    AsiaH: Optional[float] = None
    AsiaL: Optional[float] = None
    LonH:  Optional[float] = None
    LonL:  Optional[float] = None

@app.post("/levels-auto")
async def levels_auto(p: HTFPayload):
    """Receives confirmed HTF levels from TradingView Pine Script alerts."""
    inst = p.inst.upper()
    if inst not in INSTRUMENTS:
        return {"ok": False, "reason": "unknown instrument"}

    data = {k: getattr(p, k) for k in ["PDH","PDL","AsiaH","AsiaL","LonH","LonL"]
            if getattr(p, k) is not None}

    # Tag source by which keys arrived
    if "PDH" in data or "PDL" in data:
        source = "1H_pine"
    elif "AsiaH" in data or "AsiaL" in data:
        source = "15M_pine_asia"
    elif "LonH" in data or "LonL" in data:
        source = "15M_pine_london"
    else:
        source = "pine"

    store.set_many(inst, data, source)
    await broadcast({"type": "levels", "inst": inst, "levels": store.get(inst)})
    logger.info(f"📐 HTF levels received [{inst}] [{source}]: {data}")
    return {"ok": True, "inst": inst, "levels": store.get(inst), "source": source}

# Manual override — dashboard or manual paste still works
@app.post("/levels")
async def set_levels_manual(p: HTFPayload):
    inst = p.inst.upper()
    if inst not in INSTRUMENTS:
        return {"ok": False, "reason": "unknown instrument"}
    data = {k: getattr(p, k) for k in ["PDH","PDL","AsiaH","AsiaL","LonH","LonL"]
            if getattr(p, k) is not None}
    store.set_many(inst, data, "manual")
    await broadcast({"type": "levels", "inst": inst, "levels": store.get(inst)})
    return {"ok": True, "levels": store.get(inst)}

class ResultPayload(BaseModel):
    pnl:       float
    won:       bool
    inst:      Optional[str] = None
    direction: Optional[str] = None
    session:   Optional[str] = None

@app.post("/result")
async def record_result(p: ResultPayload):
    meta   = {"inst": p.inst, "direction": p.direction, "session": p.session,
               "ts": datetime.now(EST).strftime("%H:%M ET")}
    locked = stats.record(p.pnl, meta)
    s      = stats.status()
    await broadcast({"type": "result", "pnl": p.pnl, "stats": s})
    if locked:
        await send_telegram(
            f"⛔ *ACCOUNT LOCKED*\n"
            f"Total drawdown: `${stats.total_pnl:.0f}`\nBot paused — manual reset required."
        )
    elif stats.tight_mode:
        await send_telegram(
            f"⚠️ *Tight Mode Active*\n"
            f"Win rate: `{stats.win_rate:.0%}` ({stats.wins}W/{stats.losses}L)\n"
            f"Filters tightened until WR > 60%."
        )
    return s

@app.post("/reset_day")
async def reset_day():
    stats.day_pnl  = 0.0
    stats.day_date = date.today()
    return {"ok": True}

@app.post("/reset_stats")
async def reset_all_stats():
    stats.__init__()
    return {"ok": True}

@app.get("/stats")
async def get_stats():
    allowed, reason = stats.can_trade()
    return {**stats.status(), "trading_allowed": allowed, "reason": reason,
            "levels": store.all_status()}

@app.post("/report/now")
async def send_report_now():
    await report_6am()
    await asyncio.sleep(1)
    await report_755am()
    return {"ok": True}

@app.websocket("/ws")
async def ws_ep(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)
    await ws.send_text(json.dumps({
        "type":   "init",
        "prices": prices,
        "trades": trades[:20],
        "stats":  stats.status(),
        "levels": store.all_status(),
        "config": {
            "leg1_contracts": LEG1_CONTRACTS, "leg1_tp": LEG1_TP, "leg1_sl": LEG1_SL,
            "leg2_contracts": LEG2_CONTRACTS, "leg2_tp": LEG2_TP, "leg2_sl": LEG2_SL,
            "max_day_loss":   MAX_DAY_LOSS,   "win_rate_threshold": WIN_RATE_THRESHOLD,
        }
    }))
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        try: ws_clients.remove(ws)
        except: pass

@app.post("/test-trade")
async def test_trade():
    """Bypasses session/kill checks — fires real PMT webhook to verify pipeline."""
    sig = {
        "id": "TEST001", "inst": "MES", "direction": "BUY",
        "entry": prices.get("MES", 5416.0), "stop": 5406.0, "tp1": 5431.0,
        "session": "TEST", "swept": "AsiaL", "tight_mode": False,
        "ts": datetime.now(EST).strftime("%H:%M ET"),
    }
    logger.info("🧪 TEST TRADE firing — bypassing session/kill checks")
    l1_ok, l2_ok, l1_body, l2_body = await fire_trade_legs(sig)
    await send_telegram(
        f"🧪 *TEST TRADE — Pipeline Verification*\n"
        f"MES BUY @ `{sig['entry']:.2f}`\n"
        f"{'✅' if l1_ok else '❌'} L1 `{LEG1_CONTRACTS}ct` TP`${LEG1_TP:.0f}` SL`${LEG1_SL:.0f}`: `{l1_body[:80]}`\n"
        f"{'✅' if l2_ok else '❌'} L2 `{LEG2_CONTRACTS}ct` TP`${LEG2_TP:.0f}` SL`${LEG2_SL:.0f}`: `{l2_body[:80]}`"
    )
    return {"ok": l1_ok or l2_ok,
            "l1_ok": l1_ok, "l1_body": l1_body[:200],
            "l2_ok": l2_ok, "l2_body": l2_body[:200]}

@app.get("/state")
async def get_state():
    """Legacy alias — dashboard compatibility."""
    allowed, reason = stats.can_trade()
    return {**stats.status(), "trading_allowed": allowed,
            "levels": store.all_status(), "prices": prices}
