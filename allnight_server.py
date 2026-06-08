import asyncio, os, json, logging, urllib.request
from contextlib import asynccontextmanager
from datetime import datetime, date, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import aiohttp
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("allnight_bot")

EST = ZoneInfo("America/New_York")

# ── PROP FIRM CONFIG ──────────────────────────────────────────────────────────
ACCOUNT_SIZE  = 50000
MAX_RISK      = 500.0
MAX_DRAWDOWN  = -1800
PROFIT_TARGET = 3000
MAX_DAY_PROFIT = 1500
CONTRACTS     = 5

INSTRUMENTS = {
    "MES": {"yf": "ES=F",  "pmt": "MES", "point_value": 5.0,  "stop_pts": 10, "name": "Micro E-mini S&P"},
    "MNQ": {"yf": "NQ=F",  "pmt": "MNQ", "point_value": 2.0,  "stop_pts": 20, "name": "Micro E-mini Nasdaq"},
}

# ── SESSIONS ──────────────────────────────────────────────────────────────────
SESSIONS = {
    "asia":   {"name": "Asia",        "start": time(20, 0),  "end": time(23, 59), "color": "🟡"},
    "london": {"name": "London",      "start": time(2, 0),   "end": time(5, 0),   "color": "🔵"},
    "ny":     {"name": "NY Kill Zone","start": time(8, 0),   "end": time(10, 30), "color": "🟢"},
}

# ── ENV ───────────────────────────────────────────────────────────────────────
PMT_URL     = os.getenv("PMT_WEBHOOK_URL", "https://api.pickmytrade.trade/v2/add-trade-data")
PMT_TOKEN   = os.getenv("PMT_TOKEN", "")
PMT_ACCOUNT = os.getenv("PMT_ACCOUNT_ID", "")
TG_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT     = os.getenv("TELEGRAM_CHAT_ID", "")

# ── STATE ─────────────────────────────────────────────────────────────────────
prices     = {"MES": 0.0, "MNQ": 0.0}
signals    = []
trades     = []
ws_clients = []
last_signal = {"MES": 0, "MNQ": 0}

session_levels = {
    "MES": {"PDH": None, "PDL": None, "AsiaH": None, "AsiaL": None,
            "LonH": None, "LonL": None, "PMH": None, "PML": None,
            "NYOpenH": None, "NYOpenL": None},
    "MNQ": {"PDH": None, "PDL": None, "AsiaH": None, "AsiaL": None,
            "LonH": None, "LonL": None, "PMH": None, "PML": None,
            "NYOpenH": None, "NYOpenL": None},
}


# ── PROP STATS ────────────────────────────────────────────────────────────────
class PropStats:
    def __init__(self):
        self.total_pnl    = 0.0
        self.day_pnl      = 0.0
        self.day_date     = date.today()
        self.trades       = 0
        self.wins         = 0
        self.losses       = 0
        self.locked       = False
        self.revenge_until = None

    def new_day(self):
        if date.today() != self.day_date:
            self.day_pnl  = 0.0
            self.day_date = date.today()

    def record(self, pnl):
        self.new_day()
        self.total_pnl += pnl
        self.day_pnl   += pnl
        self.trades    += 1
        if pnl > 0:
            self.wins += 1
        else:
            self.losses += 1
            self.revenge_until = datetime.utcnow() + timedelta(minutes=30)
        if self.total_pnl <= MAX_DRAWDOWN:
            self.locked = True
        return self.locked

    def can_trade(self):
        self.new_day()
        if self.locked:
            return False, "Max drawdown hit"
        if self.total_pnl >= PROFIT_TARGET:
            return False, "Profit target reached"
        if self.day_pnl >= MAX_DAY_PROFIT:
            return False, "Daily consistency limit"
        if self.revenge_until and datetime.utcnow() < self.revenge_until:
            return False, "Revenge trade cooldown"
        return True, "OK"

    def status(self):
        ok, reason = self.can_trade()
        mins_left = 0
        if self.revenge_until and datetime.utcnow() < self.revenge_until:
            mins_left = int((self.revenge_until - datetime.utcnow()).total_seconds() / 60)
        return {
            "total_pnl":    round(self.total_pnl, 2),
            "day_pnl":      round(self.day_pnl, 2),
            "trades":       self.trades,
            "wins":         self.wins,
            "losses":       self.losses,
            "win_rate":     round(self.wins / self.trades * 100, 1) if self.trades else 0,
            "locked":       self.locked,
            "can_trade":    ok,
            "reason":       reason,
            "revenge_mins": mins_left,
            "to_target":    round(PROFIT_TARGET - self.total_pnl, 2),
            "drawdown_used": round(abs(min(0, self.total_pnl)) / abs(MAX_DRAWDOWN) * 100, 1),
        }


stats = PropStats()


# ── SESSION DETECTION ─────────────────────────────────────────────────────────
def get_active_session():
    now = datetime.now(EST)
    t = now.time()
    for key, sess in SESSIONS.items():
        start = sess["start"]
        end   = sess["end"]
        # Handle overnight sessions (e.g. Asia: 20:00 - 23:59)
        if start <= end:
            if start <= t <= end:
                return key, sess
        else:
            if t >= start or t <= end:
                return key, sess
    return None, None


def in_any_session():
    key, _ = get_active_session()
    return key is not None


def get_phase():
    key, sess = get_active_session()
    if key:
        return f"{sess['name']} Session"
    now = datetime.now(EST)
    t = now.time()
    if time(5, 0) <= t < time(8, 0):
        return "NY Pre-Market"
    if time(0, 0) <= t < time(2, 0):
        return "Between Sessions"
    return "Closed"


# ── SWEEP DETECTION ───────────────────────────────────────────────────────────
def check_sweep(price, inst):
    lvls = session_levels[inst]
    buf  = 1.5 if inst == "MES" else 4.0
    for name, val in lvls.items():
        if val is None:
            continue
        if price > val + buf:
            return name, val, "SHORT"
        if price < val - buf:
            return name, val, "LONG"
    return None


# ── SIGNAL ENGINE ─────────────────────────────────────────────────────────────
def process_signal(inst, price):
    if not in_any_session():
        return None
    ok, _ = stats.can_trade()
    if not ok:
        return None
    if datetime.utcnow().timestamp() - last_signal[inst] < 600:
        return None
    lvls = session_levels[inst]
    has_levels = sum(1 for v in lvls.values() if v is not None) >= 4
    if not has_levels:
        return None

    sweep = check_sweep(price, inst)
    if not sweep:
        return None

    sweep_name, sweep_val, direction = sweep
    cfg      = INSTRUMENTS[inst]
    stop_pts = cfg["stop_pts"]
    pv       = cfg["point_value"]

    if direction == "LONG":
        stop   = round(price - stop_pts, 2)
        tp1    = round(price + stop_pts * 2.5, 2)
        tp2    = lvls.get("PDH") or round(price + stop_pts * 4, 2)
        runner = round(price + stop_pts * 6, 2)
    else:
        stop   = round(price + stop_pts, 2)
        tp1    = round(price - stop_pts * 2.5, 2)
        tp2    = lvls.get("PDL") or round(price - stop_pts * 4, 2)
        runner = round(price - stop_pts * 6, 2)

    risk = abs(price - stop) * pv * CONTRACTS
    if risk > MAX_RISK:
        return None

    last_signal[inst] = datetime.utcnow().timestamp()
    reward = round(abs(tp1 - price) * pv * CONTRACTS, 2)
    rr     = round(abs(tp1 - price) / abs(price - stop), 1) if abs(price - stop) > 0 else 0
    sess_key, sess = get_active_session()

    return {
        "id":          f"{inst}_{int(datetime.utcnow().timestamp())}",
        "inst":        inst,
        "direction":   direction,
        "sweep_level": sweep_name,
        "entry":       round(price, 2),
        "stop":        stop,
        "tp1":         tp1,
        "tp2":         round(tp2 if isinstance(tp2, float) else tp2, 2),
        "runner":      runner,
        "risk":        round(risk, 2),
        "reward_tp1":  reward,
        "rr1":         rr,
        "dollar_sl":   round(risk, 2),
        "dollar_tp":   reward,
        "session":     sess["name"] if sess else "Unknown",
        "session_emoji": sess["color"] if sess else "⚪",
        "contracts":   CONTRACTS,
        "time":        datetime.now(EST).strftime("%H:%M:%S EST"),
        "confidence":  78,
    }


# ── TELEGRAM ──────────────────────────────────────────────────────────────────
async def send_telegram(msg, keyboard=None):
    if not TG_TOKEN or not TG_CHAT:
        return
    url     = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT, "text": msg, "parse_mode": "Markdown"}
    if keyboard:
        payload["reply_markup"] = keyboard
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logger.warning(f"Telegram error: {e}")


# ── WEBHOOK ───────────────────────────────────────────────────────────────────
async def fire_webhook(sig):
    payload = {
        "symbol":              sig["inst"],
        "date":                datetime.utcnow().isoformat(),
        "data":                "buy" if sig["direction"] == "LONG" else "sell",
        "quantity":            sig["contracts"],
        "risk_percentage":     0,
        "price":               sig["entry"],
        "dollar_tp":           sig["dollar_tp"],
        "dollar_sl":           sig["dollar_sl"],
        "tp": 0, "percentage_tp": 0,
        "sl": 0, "percentage_sl": 0,
        "trail": 0, "trail_stop": 0, "trail_trigger": 0, "trail_freq": 0,
        "update_tp":           False,
        "update_sl":           False,
        "breakeven":           0,
        "token":               PMT_TOKEN,
        "pyramid":             False,
        "reverse_order_close": True,
        "order_type":          "MKT",
        "account_id":          PMT_ACCOUNT,
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(PMT_URL, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as r:
                body = await r.text()
                return r.status == 200, body
    except Exception as e:
        return False, str(e)


# ── PRICE FEED ────────────────────────────────────────────────────────────────
def fetch_price_sync(symbol):
    req = urllib.request.Request(
        f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1m&range=1d",
        headers={"User-Agent": "Mozilla/5.0"}
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        d = json.loads(r.read())
        return float(d["chart"]["result"][0]["meta"]["regularMarketPrice"])


async def fetch_price(symbol):
    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(None, fetch_price_sync, symbol)
    except Exception as e:
        logger.warning(f"Price failed {symbol}: {e}")
        return None


# ── BROADCAST ─────────────────────────────────────────────────────────────────
async def broadcast(data):
    dead = []
    for ws in ws_clients:
        try:
            await ws.send_text(json.dumps(data))
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            ws_clients.remove(ws)
        except Exception:
            pass


# ── PRICE LOOP ────────────────────────────────────────────────────────────────
async def price_loop():
    logger.info("All Night price feed started — watching MES + MNQ")
    while True:
        try:
            for inst, cfg in INSTRUMENTS.items():
                try:
                    price = await fetch_price(cfg["yf"])
                    if price:
                        prices[inst] = price
                        logger.info(f"{inst}: {price}")
                        sig = process_signal(inst, price)
                        if sig:
                            signals.append(sig)
                            logger.info(f"SIGNAL: {inst} {sig['direction']} @ {sig['sweep_level']} [{sig['session']}]")
                            emoji = "🟢" if sig["direction"] == "LONG" else "🔴"
                            msg = (
                                f"{sig['session_emoji']} *ALL NIGHT BOT — {sig['session']}*\n"
                                f"{emoji} *{sig['direction']}* {inst} @ `{sig['sweep_level']}` sweep\n"
                                f"━━━━━━━━━━━━━\n"
                                f"🎯 Entry:  `{sig['entry']:,.2f}`\n"
                                f"🛑 Stop:   `{sig['stop']:,.2f}`\n"
                                f"✅ TP1:    `{sig['tp1']:,.2f}` (R:R {sig['rr1']}:1)\n"
                                f"✅ TP2:    `{sig['tp2']:,.2f}`\n"
                                f"🏃 Runner: `{sig['runner']:,.2f}`\n\n"
                                f"💰 Risk: `${sig['risk']:.0f}` | {sig['contracts']} contracts\n"
                                f"⏱ {sig['time']}"
                            )
                            kb = {"inline_keyboard": [[
                                {"text": f"{'🟢 BUY' if sig['direction'] == 'LONG' else '🔴 SELL'} — EXECUTE",
                                 "url": f"https://allnight-production.up.railway.app/execute/{sig['id']}"},
                                {"text": "⏭ Skip", "callback_data": f"skip_{sig['id']}"}
                            ]]}
                            asyncio.create_task(send_telegram(msg, kb))
                        await broadcast({
                            "type":    "price",
                            "inst":    inst,
                            "price":   price,
                            "signals": signals,
                            "stats":   stats.status(),
                            "phase":   get_phase(),
                            "session": get_active_session()[0],
                        })
                except Exception as e:
                    logger.error(f"Error processing {inst}: {e}")
        except Exception as e:
            logger.error(f"Price loop error: {e}")
        await asyncio.sleep(3)


# ── APP ───────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app):
    logger.info("All Night ICT Bot starting...")
    task = asyncio.create_task(price_loop())
    await send_telegram(
        "🌙 *All Night ICT Bot online*\n"
        "Watching MES + MNQ across all sessions:\n"
        "🟡 Asia (8PM–Midnight EST)\n"
        "🔵 London (2AM–5AM EST)\n"
        "🟢 NY Kill Zone (8AM–10:30AM EST)\n"
        f"5 contracts each · $50K prop firm rules active"
    )
    yield
    task.cancel()
    await send_telegram("🔴 *All Night Bot offline*")


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    sess_key, sess = get_active_session()
    return {
        "status":  "ok",
        "phase":   get_phase(),
        "session": sess_key,
        **stats.status()
    }


@app.get("/state")
async def get_state():
    sess_key, _ = get_active_session()
    return {
        "prices":         prices,
        "signals":        signals,
        "session_levels": session_levels,
        "phase":          get_phase(),
        "session":        sess_key,
        **stats.status()
    }


class LevelsPayload(BaseModel):
    instrument: str
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
async def set_levels(p: LevelsPayload):
    inst = p.instrument
    for k in ["PDH", "PDL", "AsiaH", "AsiaL", "LonH", "LonL", "PMH", "PML", "NYOpenH", "NYOpenL"]:
        v = getattr(p, k, None)
        if v is not None:
            session_levels[inst][k] = v
    logger.info(f"Levels set for {inst}: {session_levels[inst]}")
    await broadcast({"type": "levels", "inst": inst, "levels": session_levels[inst]})
    return {"ok": True, "levels": session_levels[inst]}


@app.post("/execute/{sig_id}")
async def execute(sig_id: str):
    ok, reason = stats.can_trade()
    if not ok:
        return {"success": False, "reason": reason}
    sig = next((s for s in signals if s["id"] == sig_id), None)
    if not sig:
        return {"success": False, "reason": "Signal not found"}
    success, body = await fire_webhook(sig)
    if success:
        signals[:] = [s for s in signals if s["id"] != sig_id]
        trades.insert(0, {**sig, "status": "EXECUTED",
                          "executed_at": datetime.now(EST).strftime("%H:%M:%S")})
        await broadcast({"type": "executed", "sig_id": sig_id, "stats": stats.status()})
        await send_telegram(
            f"✅ *Order fired* — {sig['inst']} {sig['direction']}\n"
            f"Entry: `{sig['entry']:,.2f}` | Stop: `{sig['stop']:,.2f}` | TP1: `{sig['tp1']:,.2f}`\n"
            f"Session: {sig['session']} | Contracts: {sig['contracts']}"
        )
        return {"success": True, "sig": sig}
    else:
        return {"success": False, "reason": f"Webhook failed: {body}"}


@app.post("/dismiss/{sig_id}")
async def dismiss(sig_id: str):
    signals[:] = [s for s in signals if s["id"] != sig_id]
    await broadcast({"type": "dismissed", "sig_id": sig_id})
    return {"ok": True}


class ResultPayload(BaseModel):
    sig_id: str
    pnl:    float
    won:    bool


@app.post("/result")
async def record_result(p: ResultPayload):
    locked = stats.record(p.pnl)
    s = stats.status()
    await broadcast({"type": "result", "pnl": p.pnl, "stats": s})
    if locked:
        await send_telegram(
            f"⛔ *ACCOUNT LOCKED*\n"
            f"Total drawdown: `${abs(stats.total_pnl):.0f}`\n"
            f"Prop firm limit approaching."
        )
    return s


@app.post("/reset_day")
async def reset_day():
    stats.day_pnl  = 0.0
    stats.day_date = date.today()
    signals.clear()
    return {"ok": True}


@app.websocket("/ws")
async def ws_ep(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)
    await ws.send_text(json.dumps({"type": "init", **await get_state()}))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        try:
            ws_clients.remove(ws)
        except Exception:
            pass
