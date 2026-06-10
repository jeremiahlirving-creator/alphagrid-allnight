import asyncio, os, json, logging
from contextlib import asynccontextmanager
from datetime import datetime, date, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import aiohttp
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("allnight_bot")

EST = ZoneInfo("America/New_York")

# ── PROP FIRM CONFIG ──────────────────────────────────────────────────────────
ACCOUNT_SIZE   = 50000
MAX_RISK       = 500.0
MAX_DRAWDOWN   = -1800
PROFIT_TARGET  = 3000
MAX_DAY_PROFIT = 1500

# ── KILL SWITCH CONFIG ────────────────────────────────────────────────────────
MODE               = os.getenv("MODE", "EVAL")
DAILY_CAP_LIMIT    = float(os.getenv("DAILY_CAP_LIMIT", "2500"))
TRAILING_DD_LIMIT  = float(os.getenv("TRAILING_DD_LIMIT", "500"))
LOSS_STREAK_LIMIT  = int(os.getenv("LOSS_STREAK_LIMIT", "3"))

# Bot 2 instruments — MICRO contracts, 5 each
INSTRUMENTS = {
    "MES": {"pmt": "MES", "point_value": 5.0,  "stop_pts": 10, "contracts": 5, "name": "Micro E-mini S&P"},
    "MNQ": {"pmt": "MNQ", "point_value": 2.0,  "stop_pts": 20, "contracts": 5, "name": "Micro E-mini Nasdaq"},
}

# All three sessions
SESSIONS = {
    "asia":   {"name": "Asia",         "start": time(20, 0),  "end": time(23, 59), "emoji": "🟡"},
    "london": {"name": "London",       "start": time(2, 0),   "end": time(5, 0),   "emoji": "🔵"},
    "ny":     {"name": "NY Kill Zone", "start": time(8, 0),   "end": time(10, 30), "emoji": "🟢"},
}

PMT_URL     = os.getenv("PMT_WEBHOOK_URL", "https://api.pickmytrade.trade/v2/add-trade-data-latest?t=18504")
PMT_TOKEN   = os.getenv("PMT_TOKEN", "")
PMT_ACCOUNT = os.getenv("PMT_ACCOUNT_ID", "")
TG_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT     = os.getenv("TELEGRAM_CHAT_ID", "")

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


class PropStats:
    def __init__(self):
        self.total_pnl      = 0.0
        self.day_pnl        = 0.0
        self.peak_day_pnl   = 0.0
        self.day_date       = date.today()
        self.trades         = 0
        self.wins           = 0
        self.losses         = 0
        self.loss_streak    = 0
        self.locked         = False
        self.revenge_until  = None
        self.ks_daily_cap   = False
        self.ks_trailing_dd = False
        self.ks_loss_streak = False

    def new_day(self):
        if date.today() != self.day_date:
            self.day_pnl        = 0.0
            self.peak_day_pnl   = 0.0
            self.day_date       = date.today()
            self.loss_streak    = 0
            self.ks_daily_cap   = False
            self.ks_trailing_dd = False
            self.ks_loss_streak = False

    def record(self, pnl):
        self.new_day()
        self.total_pnl += pnl
        self.day_pnl   += pnl
        self.trades    += 1

        if pnl > 0:
            self.wins        += 1
            self.loss_streak  = 0
        else:
            self.losses      += 1
            self.loss_streak += 1
            self.revenge_until = datetime.utcnow() + timedelta(minutes=30)

        if self.day_pnl > self.peak_day_pnl:
            self.peak_day_pnl = self.day_pnl

        self._eval_kill_switches()

        if self.total_pnl <= MAX_DRAWDOWN:
            self.locked = True

        return self.locked

    def _eval_kill_switches(self):
        if self.day_pnl >= DAILY_CAP_LIMIT:
            if not self.ks_daily_cap:
                self.ks_daily_cap = True
                asyncio.create_task(send_telegram(
                    f"🛑 *KILL SWITCH — Daily Cap*\n"
                    f"Day P&L `${self.day_pnl:.0f}` hit cap of `${DAILY_CAP_LIMIT:.0f}`\n"
                    f"Bot paused for rest of day to protect consistency rule."
                ))

        drawdown_from_peak = self.peak_day_pnl - self.day_pnl
        if self.peak_day_pnl > 0 and drawdown_from_peak >= TRAILING_DD_LIMIT:
            if not self.ks_trailing_dd:
                self.ks_trailing_dd = True
                asyncio.create_task(send_telegram(
                    f"🛑 *KILL SWITCH — Trailing Drawdown*\n"
                    f"Pulled back `${drawdown_from_peak:.0f}` from peak of `${self.peak_day_pnl:.0f}`\n"
                    f"Bot paused to protect gains."
                ))

        if self.loss_streak >= LOSS_STREAK_LIMIT:
            if not self.ks_loss_streak:
                self.ks_loss_streak = True
                asyncio.create_task(send_telegram(
                    f"🛑 *KILL SWITCH — Loss Streak*\n"
                    f"`{self.loss_streak}` consecutive losses\n"
                    f"Bot paused — market conditions unfavorable."
                ))

    def reset_kill_switches(self):
        self.ks_daily_cap   = False
        self.ks_trailing_dd = False
        self.ks_loss_streak = False

    def any_kill_switch(self):
        return self.ks_daily_cap or self.ks_trailing_dd or self.ks_loss_streak

    def can_trade(self):
        self.new_day()
        if self.locked:                      return False, "Max drawdown hit"
        if self.total_pnl >= PROFIT_TARGET:  return False, "Profit target reached"
        if self.day_pnl   >= MAX_DAY_PROFIT: return False, "Daily consistency limit"
        if self.revenge_until and datetime.utcnow() < self.revenge_until:
            return False, "Revenge trade cooldown"
        if self.ks_daily_cap:   return False, "Kill switch: daily cap"
        if self.ks_trailing_dd: return False, "Kill switch: trailing drawdown"
        if self.ks_loss_streak: return False, "Kill switch: loss streak"
        return True, "OK"

    def status(self):
        ok, reason = self.can_trade()
        mins_left = 0
        if self.revenge_until and datetime.utcnow() < self.revenge_until:
            mins_left = int((self.revenge_until - datetime.utcnow()).total_seconds() / 60)
        drawdown_from_peak = round(self.peak_day_pnl - self.day_pnl, 2)
        return {
            "total_pnl":         round(self.total_pnl, 2),
            "day_pnl":           round(self.day_pnl, 2),
            "peak_day_pnl":      round(self.peak_day_pnl, 2),
            "drawdown_from_peak": drawdown_from_peak,
            "loss_streak":       self.loss_streak,
            "trades":            self.trades,
            "wins":              self.wins,
            "losses":            self.losses,
            "win_rate":          round(self.wins / self.trades * 100, 1) if self.trades else 0,
            "locked":            self.locked,
            "can_trade":         ok,
            "reason":            reason,
            "revenge_mins":      mins_left,
            "to_target":         round(PROFIT_TARGET - self.total_pnl, 2),
            "drawdown_used":     round(abs(min(0, self.total_pnl)) / abs(MAX_DRAWDOWN) * 100, 1),
            "kill_switches": {
                "daily_cap":   self.ks_daily_cap,
                "trailing_dd": self.ks_trailing_dd,
                "loss_streak": self.ks_loss_streak,
            },
            "mode":              MODE,
            "daily_cap_limit":   int(DAILY_CAP_LIMIT),
            "trailing_dd_limit": int(TRAILING_DD_LIMIT),
        }


stats = PropStats()


def get_active_session():
    now = datetime.now(EST)
    t   = now.time()
    for key, sess in SESSIONS.items():
        start, end = sess["start"], sess["end"]
        if start <= end:
            if start <= t <= end: return key, sess
        else:
            if t >= start or t <= end: return key, sess
    return None, None


def in_any_session():
    key, _ = get_active_session()
    return key is not None


def get_phase():
    key, sess = get_active_session()
    if key: return f"{sess['name']} Session"
    now = datetime.now(EST)
    t   = now.time()
    if time(5, 0) <= t < time(8, 0): return "NY Pre-Market"
    return "Between Sessions"


def check_sweep(price, inst):
    lvls = session_levels[inst]
    buf  = 1.5 if inst == "MES" else 4.0
    for name, val in lvls.items():
        if val is None: continue
        if price > val + buf: return name, val, "SHORT"
        if price < val - buf: return name, val, "LONG"
    return None


def process_signal(inst, price):
    if not in_any_session(): return None
    ok, _ = stats.can_trade()
    if not ok: return None
    if datetime.utcnow().timestamp() - last_signal[inst] < 600: return None
    lvls = session_levels[inst]
    if sum(1 for v in lvls.values() if v is not None) < 4: return None
    sweep = check_sweep(price, inst)
    if not sweep: return None
    sweep_name, sweep_val, direction = sweep
    cfg       = INSTRUMENTS[inst]
    stop_pts  = cfg["stop_pts"]
    pv        = cfg["point_value"]
    contracts = cfg["contracts"]
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
    risk = abs(price - stop) * pv * contracts
    if risk > MAX_RISK: return None
    last_signal[inst] = datetime.utcnow().timestamp()
    reward = round(abs(tp1 - price) * pv * contracts, 2)
    sess_key, sess = get_active_session()
    return {
        "id":          f"{inst}_{int(datetime.utcnow().timestamp())}",
        "inst":        inst,
        "direction":   direction,
        "sweep_level": sweep_name,
        "entry":       round(price, 2),
        "stop":        stop,
        "tp1":         tp1,
        "tp2":         round(tp2 if isinstance(tp2, float) else float(tp2), 2),
        "runner":      runner,
        "risk":        round(risk, 2),
        "reward_tp1":  reward,
        "rr1":         round(abs(tp1 - price) / abs(price - stop), 1) if abs(price - stop) > 0 else 0,
        "dollar_sl":   round(risk, 2),
        "dollar_tp":   reward,
        "session":     f"{sess['name']} {sess['emoji']}" if sess else "Active",
        "contracts":   contracts,
        "time":        datetime.now(EST).strftime("%H:%M:%S EST"),
        "confidence":  78,
    }


async def send_telegram(msg, keyboard=None):
    if not TG_TOKEN or not TG_CHAT: return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT, "text": msg, "parse_mode": "Markdown"}
    if keyboard: payload["reply_markup"] = keyboard
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logger.warning(f"Telegram error: {e}")

async def fire_webhook(sig):
    payload = {
        "symbol":                sig["inst"],
        "strategy_name":         "All night",
        "date":                  datetime.utcnow().isoformat(),
        "data":                  "buy" if sig["direction"] == "LONG" else "sell",
        "quantity":              sig["contracts"],
        "risk_percentage":       0,
        "price":                 sig["entry"],
        "tp": 0, "percentage_tp": 0, "dollar_tp": sig["dollar_tp"],
        "sl": 0, "dollar_sl": sig["dollar_sl"], "percentage_sl": 0,
        "trail": 0, "trail_stop": 0, "trail_trigger": 0, "trail_freq": 0,
        "update_tp": False, "update_sl": False,
        "breakeven": 0, "breakeven_offset": 0,
        "token":                 PMT_TOKEN,
        "pyramid":               True,
        "same_direction_ignore": False,
        "reverse_order_close":   False,
        "multiple_accounts": [
            {
                "token":               PMT_TOKEN,
                "account_id":          "MFFUEVRPD505461064",
                "risk_percentage":     0,
                "quantity_multiplier": 1,
            }
        ],
    }
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
        "Origin": "https://www.pickmytrade.trade",
        "Referer": "https://www.pickmytrade.trade/",
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(PMT_URL, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                body = await r.text()
                logger.info(f"PMT response: {r.status} {body}")
                return r.status == 200, body
    except Exception as e:
        logger.error(f"fire_webhook error: {e}")
        return False, str(e)

async def broadcast(data):
    dead = []
    for ws in ws_clients:
        try: await ws.send_text(json.dumps(data))
        except: dead.append(ws)
    for ws in dead:
        try: ws_clients.remove(ws)
        except: pass


async def price_loop():
    logger.info("All Night Bot started — watching MES + MNQ (5 contracts each)")
    while True:
        try:
            for inst, cfg in INSTRUMENTS.items():
                try:
                    price = prices.get(inst)
                    if price:
                        prices[inst] = price
                        logger.info(f"{inst}: {price}")
                        sig = process_signal(inst, price)
                        if sig:
                            signals.append(sig)
                            emoji = "🟢" if sig["direction"] == "LONG" else "🔴"
                            msg = (
                                f"🌙 *ALL NIGHT BOT — {sig['session']}*\n"
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
                                {"text": f"{'🟢 BUY' if sig['direction']=='LONG' else '🔴 SELL'} — EXECUTE",
                                 "url": f"https://alphagrid-allnight-production.up.railway.app/execute/{sig['id']}"},
                                {"text": "⏭ Skip", "callback_data": f"skip_{sig['id']}"}
                            ]]}
                            asyncio.create_task(send_telegram(msg, kb))
                        await broadcast({
                            "type": "price", "inst": inst, "price": price,
                            "signals": signals, "trades": trades,
                            "stats": stats.status(),
                            "phase": get_phase(), "session": get_active_session()[0],
                        })
                except Exception as e:
                    logger.error(f"Error processing {inst}: {e}")
        except Exception as e:
            logger.error(f"Price loop error: {e}")
        await asyncio.sleep(3)


@asynccontextmanager
async def lifespan(app):
    logger.info("All Night Bot starting (MES + MNQ, 5 contracts)...")
    task = asyncio.create_task(price_loop())
    await send_telegram(
        "🌙 *All Night Bot online*\n"
        "Watching *MES + MNQ* — 5 contracts each\n"
        "🟡 Asia · 🔵 London · 🟢 NY Kill Zone\n"
        "Prop firm rules active · Kill switches armed · $50K account"
    )
    yield
    task.cancel()
    await send_telegram("🔴 *All Night Bot offline*")


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    sess_key, sess = get_active_session()
    return {"status": "ok", "phase": get_phase(), "session": sess_key,
            "instruments": "MES + MNQ", "contracts": 5, **stats.status()}


@app.get("/state")
async def get_state():
    sess_key, _ = get_active_session()
    return {"prices": prices, "signals": signals, "trades": trades,
            "session_levels": session_levels,
            "phase": get_phase(), "session": sess_key, **stats.status()}


class LevelsPayload(BaseModel):
    instrument: str
    PDH: Optional[float] = None
    PDL: Optional[float] = None
    AsiaH: Optional[float] = None
    AsiaL: Optional[float] = None
    LonH: Optional[float] = None
    LonL: Optional[float] = None
    PMH: Optional[float] = None
    PML: Optional[float] = None
    NYOpenH: Optional[float] = None
    NYOpenL: Optional[float] = None


@app.post("/levels")
async def set_levels(p: LevelsPayload):
    inst = p.instrument
    for k in ["PDH","PDL","AsiaH","AsiaL","LonH","LonL","PMH","PML","NYOpenH","NYOpenL"]:
        v = getattr(p, k, None)
        if v is not None:
            session_levels[inst][k] = v
    await broadcast({"type": "levels", "inst": inst, "levels": session_levels[inst]})
    return {"ok": True}


@app.get("/execute/{sig_id}")
async def execute_get(sig_id: str):
    ok, reason = stats.can_trade()
    if not ok:
        return {"success": False, "reason": reason}
    sig = next((s for s in signals if s["id"] == sig_id), None)
    if not sig:
        return {"success": False, "reason": "Signal not found or expired"}
    success, body = await fire_webhook(sig)
    if success:
        signals[:] = [s for s in signals if s["id"] != sig_id]
        trades.insert(0, {**sig, "status": "EXECUTED", "executed_at": datetime.now(EST).strftime("%H:%M:%S")})
        await broadcast({"type": "trade_executed", "trades": trades, "signals": signals, "stats": stats.status()})
        await send_telegram(f"✅ *Order fired* — {sig['inst']} {sig['direction']} @ `{sig['entry']:,.2f}`")
        return {"success": True, "message": "Order executed successfully"}
    else:
        return {"success": False, "reason": "Webhook failed"}


@app.post("/dismiss/{sig_id}")
async def dismiss(sig_id: str):
    signals[:] = [s for s in signals if s["id"] != sig_id]
    await broadcast({"type": "dismissed", "sig_id": sig_id})
    return {"ok": True}


class ResultPayload(BaseModel):
    sig_id: str
    pnl: float
    won: bool


@app.post("/result")
async def record_result(p: ResultPayload):
    locked = stats.record(p.pnl)
    s = stats.status()
    await broadcast({"type": "result", "pnl": p.pnl, "stats": s})
    if locked:
        await send_telegram(f"⛔ *ACCOUNT LOCKED*\nDrawdown: `${abs(stats.total_pnl):.0f}`")
    return s


@app.post("/reset_day")
async def reset_day():
    stats.day_pnl       = 0.0
    stats.peak_day_pnl  = 0.0
    stats.day_date      = date.today()
    stats.loss_streak   = 0
    stats.reset_kill_switches()
    signals.clear()
    return {"ok": True}


@app.post("/reset_kill_switches")
async def reset_kill_switches():
    stats.reset_kill_switches()
    return {"ok": True, "kill_switches": stats.status()["kill_switches"]}


@app.post("/price-update")
async def price_update(request: Request):
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        price = float(data.get("price", 0))
        if symbol and price:
            prices[symbol] = price
            logger.info(f"TradingView price: {symbol} = {price}")
        return {"ok": True}
    except Exception as e:
        logger.error(f"Price update error: {e}")
        return {"ok": False}


@app.websocket("/ws")
async def ws_ep(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)
    await ws.send_text(json.dumps({"type": "init", **await get_state()}))
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        try: ws_clients.remove(ws)
        except: pass
