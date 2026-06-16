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

# ── MFFU BUILDER ACCOUNT RULES ───────────────────────────────────────────────
# Builder sim-funded account — MFFUEVBLDR505461067
# Daily Loss Limit:  -$1,000 soft pause (we treat as hard stop)
# Max EOD Trailing Drawdown: -$2,000 from peak balance
# No profit target, no consistency rule, no daily profit cap
# Max contracts: 40 micros (we use 5 MES = well within limit)
# Payout path: $500 above $2,100 buffer → request payout (max $2,000/cycle, 5 payouts → live)

MAX_DAY_LOSS      = -1_000.0   # daily loss hard stop
MAX_DAY_PROFIT    =  99_999.0  # no daily profit cap — set high to disable
PROFIT_TARGET     =  99_999.0  # no eval target
EOD_TRAIL_MAX     =  2_000.0   # max EOD trailing drawdown from peak
PAYOUT_BUFFER     =  2_100.0   # must clear this buffer before requesting payout
PAYOUT_THRESHOLD  =    500.0   # need $500 above buffer to request payout

# ── TRADE CONFIG ──────────────────────────────────────────────────────────────
LEG1_CONTRACTS = 3;  LEG1_TP = 200.0;  LEG1_SL = 100.0
LEG2_CONTRACTS = 2;  LEG2_TP = 300.0;  LEG2_SL = 100.0

# ── WIN RATE ADAPTIVE FILTER ──────────────────────────────────────────────────
WIN_RATE_THRESHOLD  = 0.60
WIN_RATE_MIN_TRADES = 10

# ── ACTIVE INSTRUMENTS (add "MNQ" when ready to expand) ──────────────────────
ACTIVE_INSTRUMENTS = ["MES"]

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
    "NY_KZ":  (time(8,  0),  time(8,  30)),   # Narrowed to 8:00-8:30 AM — backtest shows post-8:30 signals lose
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

# ── FVG ENGINE ───────────────────────────────────────────────────────────────
# Two-stage signal: 15M sweep detected → watch 1M bars for FVG → fire on retest
# FVG (Fair Value Gap): gap between candle[i-2].low and candle[i].high (bullish)
#                       gap between candle[i-2].high and candle[i].low (bearish)
# Entry fires when price retraces into the gap zone.

FVG_TIMEOUT_MINS  = 15    # cancel sweep watch if no FVG forms within 15 mins
FVG_MIN_GAP_PTS   = 1.0   # minimum gap size to qualify as FVG (MES points)

class Bar1M:
    """Single 1-minute OHLC bar built from tick stream."""
    def __init__(self, open_p: float, ts: datetime):
        self.open  = open_p
        self.high  = open_p
        self.low   = open_p
        self.close = open_p
        self.ts    = ts

    def update(self, price: float):
        self.high  = max(self.high, price)
        self.low   = min(self.low,  price)
        self.close = price

class FVGState:
    """Tracks sweep detection and FVG watch state per instrument."""
    def __init__(self):
        self.reset()

    def reset(self):
        self.sweep_active    = False
        self.sweep_direction = None   # "BUY" or "SELL"
        self.sweep_level_key = None   # e.g. "AsiaL"
        self.sweep_level_val = None   # e.g. 5420.0
        self.sweep_ts        = None
        self.sweep_price     = None
        self.fvg_high        = None   # FVG zone top
        self.fvg_low         = None   # FVG zone bottom
        self.fvg_detected    = False

    def activate_sweep(self, direction: str, level_key: str,
                       level_val: float, price: float, now: datetime):
        self.reset()
        self.sweep_active    = True
        self.sweep_direction = direction
        self.sweep_level_key = level_key
        self.sweep_level_val = level_val
        self.sweep_ts        = now
        self.sweep_price     = price

    def is_timed_out(self, now: datetime) -> bool:
        if not self.sweep_ts:
            return False
        return (now - self.sweep_ts).total_seconds() > FVG_TIMEOUT_MINS * 60

    def check_fvg(self, bars: list) -> bool:
        """
        Check last 3 completed 1M bars for FVG pattern.
        Bullish FVG (after BUY sweep): bar[-3].low > bar[-1].high
        Bearish FVG (after SELL sweep): bar[-3].high < bar[-1].low
        """
        if len(bars) < 3:
            return False
        b0, b1, b2 = bars[-3], bars[-2], bars[-1]
        if self.sweep_direction == "BUY":
            gap_low  = b2.high   # top of most recent bar
            gap_high = b0.low    # bottom of oldest bar
            if gap_high > gap_low + FVG_MIN_GAP_PTS:
                self.fvg_low  = gap_low
                self.fvg_high = gap_high
                self.fvg_detected = True
                return True
        else:
            gap_high = b2.low
            gap_low  = b0.high
            if gap_low < gap_high - FVG_MIN_GAP_PTS:
                self.fvg_low  = gap_low
                self.fvg_high = gap_high
                self.fvg_detected = True
                return True
        return False

    def price_in_fvg(self, price: float) -> bool:
        """Returns True when price retraces into the FVG zone."""
        if not self.fvg_detected:
            return False
        return self.fvg_low <= price <= self.fvg_high

class BarTracker:
    """Builds 1M OHLC bars from tick stream per instrument."""
    def __init__(self):
        self.current: dict[str, Bar1M] = {}
        self.completed: dict[str, list] = {"MES": [], "MNQ": []}
        self.last_minute: dict[str, int] = {}

    def update(self, inst: str, price: float, now: datetime) -> bool:
        """Update tracker. Returns True if a new bar just completed."""
        minute_key = now.hour * 60 + now.minute
        new_bar = False

        if inst not in self.current:
            self.current[inst] = Bar1M(price, now)
            self.last_minute[inst] = minute_key
        elif minute_key != self.last_minute.get(inst):
            # New minute — close current bar, start new one
            completed = self.current[inst]
            self.completed[inst].append(completed)
            if len(self.completed[inst]) > 50:   # keep last 50 bars
                self.completed[inst] = self.completed[inst][-50:]
            self.current[inst] = Bar1M(price, now)
            self.last_minute[inst] = minute_key
            new_bar = True
        else:
            self.current[inst].update(price)

        return new_bar

    def get_bars(self, inst: str) -> list:
        return self.completed[inst]

bar_tracker = BarTracker()
fvg_states  = {"MES": FVGState(), "MNQ": FVGState()}

# ── SELF-TUNING ENGINE ────────────────────────────────────────────────────────
TUNE_EVERY    = 10          # retune after every N trades
BUF_MIN       = 1.0         # minimum sweep buffer (MES)
BUF_MAX       = 6.0         # maximum sweep buffer (MES)
BUF_MIN_MNQ   = 2.0
BUF_MAX_MNQ   = 16.0
WIN_RATE_GOOD = 0.60        # above this = loosen buffer slightly
WIN_RATE_BAD  = 0.40        # below this = tighten buffer aggressively

class TradeRecord:
    def __init__(self, sig: dict, pnl: float, won: bool):
        self.id        = sig.get("id", "?")
        self.inst      = sig.get("inst", "?")
        self.direction = sig.get("direction", "?")
        self.session   = sig.get("session", "?")
        self.swept     = sig.get("swept", "?")
        self.entry     = sig.get("entry", 0)
        self.pnl       = pnl
        self.won       = won
        self.ts        = datetime.now(EST).strftime("%Y-%m-%d %H:%M ET")

class TuningEngine:
    """
    Tracks every trade with full context and automatically adjusts:
    - Sweep buffers per instrument (tighter = fewer signals, higher quality)
    - Session contract multipliers (reduce size on losing sessions)
    - Direction bias per session (suppress BUY or SELL if one consistently loses)
    - Cooldown per session (extend if signals cluster and lose)
    After every TUNE_EVERY trades, recomputes all parameters and
    sends a Telegram report with what changed and why.
    """
    def __init__(self):
        self.all_trades: list[TradeRecord] = []
        self.trades_since_tune = 0

        # Tunable parameters — start at defaults
        self.sweep_buf = {
            "MES": {"normal": 1.5, "tight": 2.5},
            "MNQ": {"normal": 4.0, "tight": 6.0},
        }
        self.session_multiplier = {
            "Asia": 1.0, "London": 1.0, "NY_KZ": 1.0
        }
        # Direction suppression per session: None = both, "BUY" = BUY only, "SELL" = SELL only
        self.session_direction_bias = {
            "Asia": None, "London": None, "NY_KZ": None
        }
        self.session_cooldown = {
            "Asia": 600, "London": 600, "NY_KZ": 600
        }
        # Level quality scores (win rate per swept level)
        self.level_stats: dict[str, dict] = {}  # key: "INST_LEVEL" e.g. "MES_PDL"

        self.tune_count = 0
        self.tune_log   = []   # history of tuning decisions

    def record(self, rec: TradeRecord):
        self.all_trades.append(rec)
        self.trades_since_tune += 1

        # Update level stats
        key = f"{rec.inst}_{rec.swept}"
        if key not in self.level_stats:
            self.level_stats[key] = {"wins": 0, "losses": 0}
        if rec.won:
            self.level_stats[key]["wins"] += 1
        else:
            self.level_stats[key]["losses"] += 1

    def _wr(self, wins, losses):
        t = wins + losses
        return wins / t if t > 0 else None

    def _session_trades(self, session, last_n=None):
        t = [r for r in self.all_trades if r.session == session]
        return t[-last_n:] if last_n else t

    def _session_wr(self, session, last_n=20):
        t = self._session_trades(session, last_n)
        if len(t) < 3:
            return None
        wins = sum(1 for r in t if r.won)
        return wins / len(t)

    def _direction_wr(self, session, direction, last_n=20):
        t = [r for r in self._session_trades(session, last_n) if r.direction == direction]
        if len(t) < 3:
            return None
        wins = sum(1 for r in t if r.won)
        return wins / len(t)

    def _level_wr(self, inst, level):
        key = f"{inst}_{level}"
        s = self.level_stats.get(key, {})
        return self._wr(s.get("wins", 0), s.get("losses", 0))

    def tune(self) -> list[str]:
        """
        Recompute all parameters. Returns list of change descriptions.
        Called after every TUNE_EVERY trades.
        """
        self.tune_count += 1
        self.trades_since_tune = 0
        changes = []

        # ── 1. SWEEP BUFFER TUNING (per instrument, last 20 trades) ──────────
        for inst in INSTRUMENTS:
            recent = [r for r in self.all_trades[-40:] if r.inst == inst]
            if len(recent) < 5:
                continue
            wins = sum(1 for r in recent if r.won)
            wr   = wins / len(recent)
            old_buf = self.sweep_buf[inst]["normal"]
            buf_min = BUF_MIN if inst == "MES" else BUF_MIN_MNQ
            buf_max = BUF_MAX if inst == "MES" else BUF_MAX_MNQ

            if wr < WIN_RATE_BAD:
                # Tighten aggressively — widen buffer so fewer signals fire
                new_buf = min(old_buf * 1.30, buf_max)
                reason  = f"WR {wr:.0%} < 40% — tightening"
            elif wr < WIN_RATE_GOOD:
                # Tighten slightly
                new_buf = min(old_buf * 1.10, buf_max)
                reason  = f"WR {wr:.0%} < 60% — slight tighten"
            else:
                # Winning — loosen slightly to catch more setups
                new_buf = max(old_buf * 0.95, buf_min)
                reason  = f"WR {wr:.0%} ≥ 60% — slight loosen"

            if abs(new_buf - old_buf) > 0.1:
                self.sweep_buf[inst]["normal"] = round(new_buf, 2)
                self.sweep_buf[inst]["tight"]  = round(new_buf * 1.5, 2)
                changes.append(f"📐 {inst} buffer {old_buf:.1f}→{new_buf:.1f}pts ({reason})")

        # ── 2. SESSION CONTRACT MULTIPLIER ────────────────────────────────────
        for session in SESSIONS:
            wr = self._session_wr(session, last_n=20)
            if wr is None:
                continue
            old_mult = self.session_multiplier[session]
            if wr < 0.35:
                new_mult = max(old_mult - 0.25, 0.25)   # reduce to 25% contracts min
                reason   = f"WR {wr:.0%} < 35%"
            elif wr < 0.50:
                new_mult = max(old_mult - 0.10, 0.50)
                reason   = f"WR {wr:.0%} < 50%"
            elif wr > 0.65:
                new_mult = min(old_mult + 0.10, 1.0)
                reason   = f"WR {wr:.0%} > 65%"
            else:
                new_mult = old_mult
                reason   = None
            if reason and abs(new_mult - old_mult) > 0.01:
                self.session_multiplier[session] = round(new_mult, 2)
                changes.append(f"📊 {session} size {old_mult:.0%}→{new_mult:.0%} ({reason})")

        # ── 3. DIRECTION BIAS PER SESSION ─────────────────────────────────────
        for session in SESSIONS:
            buy_wr  = self._direction_wr(session, "BUY",  last_n=20)
            sell_wr = self._direction_wr(session, "SELL", last_n=20)
            old_bias = self.session_direction_bias[session]
            new_bias = None

            if buy_wr is not None and sell_wr is not None:
                if buy_wr > 0.60 and sell_wr < 0.40:
                    new_bias = "BUY"
                elif sell_wr > 0.60 and buy_wr < 0.40:
                    new_bias = "SELL"
                elif buy_wr > 0.45 and sell_wr > 0.45:
                    new_bias = None   # both working, allow both

            if new_bias != old_bias:
                self.session_direction_bias[session] = new_bias
                bias_str = new_bias if new_bias else "BOTH"
                changes.append(f"🧭 {session} direction bias → {bias_str} (BUY {buy_wr:.0%} / SELL {sell_wr:.0%})" if buy_wr and sell_wr else f"🧭 {session} bias → {bias_str}")

        # ── 4. COOLDOWN TUNING ────────────────────────────────────────────────
        for session in SESSIONS:
            t = self._session_trades(session, last_n=20)
            if len(t) < 5:
                continue
            # Count consecutive losses
            max_consec = 0
            curr = 0
            for r in t:
                if not r.won:
                    curr += 1
                    max_consec = max(max_consec, curr)
                else:
                    curr = 0
            old_cd = self.session_cooldown[session]
            if max_consec >= 3:
                new_cd = min(old_cd + 300, 1800)   # add 5 min, max 30 min
            elif max_consec <= 1:
                new_cd = max(old_cd - 60, 300)     # shorten slightly, min 5 min
            else:
                new_cd = old_cd
            if new_cd != old_cd:
                self.session_cooldown[session] = new_cd
                changes.append(f"⏱️ {session} cooldown {old_cd//60}min→{new_cd//60}min (max consec losses: {max_consec})")

        self.tune_log.append({
            "tune_num": self.tune_count,
            "ts":       datetime.now(EST).strftime("%Y-%m-%d %H:%M ET"),
            "changes":  changes,
            "total_trades": len(self.all_trades),
        })
        return changes

    def get_buf(self, inst: str, tight: bool) -> float:
        mode = "tight" if tight else "normal"
        return self.sweep_buf[inst][mode]

    def get_cooldown(self, session: str, tight: bool) -> int:
        base = self.session_cooldown.get(session, 600)
        return base * 2 if tight else base

    def get_qty(self, base_qty: int, session: str) -> int:
        mult = self.session_multiplier.get(session, 1.0)
        return max(1, round(base_qty * mult))

    def direction_allowed(self, session: str, direction: str) -> bool:
        bias = self.session_direction_bias.get(session)
        if bias is None:
            return True
        return direction == bias

    def weekly_report(self) -> str:
        if not self.all_trades:
            return "📊 *Weekly Report*\nNo trades recorded yet."

        total   = len(self.all_trades)
        wins    = sum(1 for r in self.all_trades if r.won)
        total_pnl = sum(r.pnl for r in self.all_trades)
        wr      = wins / total if total else 0

        lines = [
            f"📊 *AlphaGrid Weekly Performance Report*",
            f"━━━━━━━━━━━━━━━━━━━━━",
            f"Total Trades: `{total}` | Wins: `{wins}` | WR: `{wr:.0%}`",
            f"Total P&L: `${total_pnl:+.2f}`",
            f"Tune Cycles: `{self.tune_count}`",
            f"",
            f"*By Session:*",
        ]

        for session in SESSIONS:
            t = self._session_trades(session)
            if not t:
                continue
            sw = sum(1 for r in t if r.won)
            swr = sw / len(t) if t else 0
            spnl = sum(r.pnl for r in t)
            mult = self.session_multiplier[session]
            bias = self.session_direction_bias[session] or "BOTH"
            lines.append(
                f"  {session}: `{len(t)}` trades `{swr:.0%}` WR `${spnl:+.0f}` "
                f"| Size `{mult:.0%}` Dir `{bias}`"
            )

        lines += ["", "*By Level:*"]
        for key, s in sorted(self.level_stats.items(),
                              key=lambda x: -(x[1]["wins"] + x[1]["losses"])):
            t = s["wins"] + s["losses"]
            if t < 2:
                continue
            lwr = s["wins"] / t
            lines.append(f"  {key}: `{t}` trades `{lwr:.0%}` WR")

        lines += ["", "*Current Buffers:*"]
        for inst in INSTRUMENTS:
            b = self.sweep_buf[inst]
            lines.append(f"  {inst}: normal `{b['normal']}pts` tight `{b['tight']}pts`")

        if self.tune_log:
            last = self.tune_log[-1]
            lines += ["", f"*Last Tune* (#{last['tune_num']}): {last['ts']}"]
            for c in last["changes"]:
                lines.append(f"  {c}")
            if not last["changes"]:
                lines.append("  No changes needed")

        return "\n".join(lines)

    def status(self) -> dict:
        return {
            "tune_count":        self.tune_count,
            "trades_since_tune": self.trades_since_tune,
            "next_tune_in":      TUNE_EVERY - self.trades_since_tune,
            "sweep_buf":         self.sweep_buf,
            "session_multiplier":self.session_multiplier,
            "session_direction_bias": self.session_direction_bias,
            "session_cooldown":  self.session_cooldown,
            "level_stats":       self.level_stats,
        }

tuner = TuningEngine()

# ── HTF LEVEL STORE ───────────────────────────────────────────────────────────
class HTFLevelStore:
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
        for inst in INSTRUMENTS:
            self.levels[inst]["AsiaH"] = None
            self.levels[inst]["AsiaL"] = None
            self.levels[inst]["LonH"]  = None
            self.levels[inst]["LonL"]  = None
        self._last_reset = date.today()
        logger.info("🔄 Midnight reset — Asia/London levels cleared")

    def set(self, inst: str, key: str, value: float, source: str = "auto"):
        self.levels[inst][key] = value
        self.last_updated[inst][key] = {"value": value, "source": source,
                                         "ts": datetime.now(EST).strftime("%H:%M ET")}
        logger.info(f"📐 {inst} {key} = {value:.2f} [{source}]")

    def set_many(self, inst: str, data: dict, source: str):
        for k, v in data.items():
            if v is not None and k in self.levels[inst]:
                self.set(inst, k, v, source)

    def get(self, inst: str) -> dict:
        return self.levels[inst]

    def all_status(self) -> dict:
        return {inst: {"levels": self.levels[inst],
                       "last_updated": self.last_updated[inst]}
                for inst in INSTRUMENTS}

    def check_midnight_reset(self):
        if date.today() != self._last_reset:
            self.midnight_reset()

store = HTFLevelStore()

# ── PROP STATS ────────────────────────────────────────────────────────────────
class PropStats:
    def __init__(self):
        self.total_pnl        = 0.0
        self.day_pnl          = 0.0
        self.day_date         = date.today()
        self.eod_peak_pnl     = 0.0   # peak P&L at EOD — floor trails this
        self.intraday_peak    = 0.0   # highest P&L seen today (for EOD trail calc)
        self.wins             = 0
        self.losses           = 0
        self.tight_mode       = False
        self.yesterday_pnl    = 0.0
        self.yesterday_wins   = 0
        self.yesterday_losses = 0
        self.day_trades_log   = []
        self.payout_count     = 0
        self.total_withdrawn  = 0.0

    def _check_day_reset(self):
        today = date.today()
        if self.day_date != today:
            self.yesterday_pnl    = self.day_pnl
            self.yesterday_wins   = sum(1 for t in self.day_trades_log if t["pnl"] > 0)
            self.yesterday_losses = sum(1 for t in self.day_trades_log if t["pnl"] <= 0)
            # EOD: update peak with today's closing P&L
            if self.total_pnl > self.eod_peak_pnl:
                self.eod_peak_pnl = self.total_pnl
            self.day_pnl        = 0.0
            self.intraday_peak  = 0.0
            self.day_date       = today
            self.day_trades_log = []

    @property
    def total_trades(self): return self.wins + self.losses

    @property
    def win_rate(self):
        return self.wins / self.total_trades if self.total_trades else 1.0

    @property
    def trailing_floor(self) -> float:
        """EOD trailing drawdown floor — rises with EOD peak, never falls."""
        return self.eod_peak_pnl - EOD_TRAIL_MAX

    @property
    def drawdown_remaining(self) -> float:
        return self.total_pnl - self.trailing_floor

    @property
    def payout_eligible(self) -> bool:
        """True when account is $500 above the $2,100 buffer."""
        return self.total_pnl >= (PAYOUT_BUFFER + PAYOUT_THRESHOLD)

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
        # Track intraday peak for reporting
        if self.total_pnl > self.intraday_peak:
            self.intraday_peak = self.total_pnl
        if meta:
            self.day_trades_log.append({**meta, "pnl": pnl})
        # Return True if EOD trailing floor breached
        return self.total_pnl <= self.trailing_floor

    def can_trade(self) -> tuple[bool, str]:
        self._check_day_reset()
        if self.total_pnl <= self.trailing_floor:
            return False, f"EOD trailing drawdown hit (floor: ${self.trailing_floor:.0f})"
        if self.day_pnl <= MAX_DAY_LOSS:
            return False, f"Daily loss limit hit (${self.day_pnl:.0f})"
        return True, "ok"

    def status(self):
        return {
            "total_pnl":          round(self.total_pnl, 2),
            "day_pnl":            round(self.day_pnl, 2),
            "eod_peak_pnl":       round(self.eod_peak_pnl, 2),
            "trailing_floor":     round(self.trailing_floor, 2),
            "drawdown_remaining": round(self.drawdown_remaining, 2),
            "day_loss_remaining": round(MAX_DAY_LOSS - self.day_pnl, 2),
            "wins":               self.wins,
            "losses":             self.losses,
            "win_rate":           round(self.win_rate * 100, 1),
            "total_trades":       self.total_trades,
            "tight_mode":         self.tight_mode,
            "payout_eligible":    self.payout_eligible,
            "payout_count":       self.payout_count,
            "yesterday_pnl":      round(self.yesterday_pnl, 2),
            "to_payout":          round(max(0, PAYOUT_BUFFER + PAYOUT_THRESHOLD - self.total_pnl), 2),
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
    wr_str  = f"{stats.win_rate:.0%} ({stats.wins}W/{stats.losses}L)" if stats.total_trades else "No trades yet"
    allowed, reason = stats.can_trade()
    ts = tuner.status()
    s = stats.status()
    text = (
        f"☀️ *AlphaGrid Morning Report* — {datetime.now(EST).strftime('%b %d, %Y')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📐 *HTF Levels* _(1H: PDH/PDL · 15M: Asia/London)_\n"
        f"*MES:* PDH {fmt_level(lvl_mes['PDH'])} PDL {fmt_level(lvl_mes['PDL'])}\n"
        f"  AsiaH {fmt_level(lvl_mes['AsiaH'])} AsiaL {fmt_level(lvl_mes['AsiaL'])}\n"
        f"  LonH {fmt_level(lvl_mes['LonH'])} LonL {fmt_level(lvl_mes['LonL'])}\n\n"
        f"📊 *Yesterday* P&L: `${stats.yesterday_pnl:+.2f}` | "
        f"{stats.yesterday_wins + stats.yesterday_losses} trades "
        f"({stats.yesterday_wins}W/{stats.yesterday_losses}L)\n\n"
        f"💼 *Builder Account Status*\n"
        f"  Total P&L: `${stats.total_pnl:+.2f}`\n"
        f"  EOD Floor: `${stats.trailing_floor:.0f}` | Drawdown room: `${s['drawdown_remaining']:.0f}`\n"
        f"  Day loss room: `${s['day_loss_remaining']:.0f}` of `-$1,000`\n"
        f"  To payout: `${s['to_payout']:.0f}` needed (buffer $2,100 + $500)\n"
        f"  Payouts: `{stats.payout_count}/5` → live account\n\n"
        f"📈 *Performance* WR: `{wr_str}` | Mode: `{'🔒 TIGHT' if stats.tight_mode else '✅ NORMAL'}`\n\n"
        f"🧠 *Tuner* Cycle `#{ts['tune_count']}` | Next tune in `{ts['next_tune_in']}` trades\n"
        f"  MES buf: `{ts['sweep_buf']['MES']['normal']}pts` | "
        f"Asia `{ts['session_multiplier']['Asia']:.0%}` "
        f"Lon `{ts['session_multiplier']['London']:.0%}` "
        f"NYKZ `{ts['session_multiplier']['NY_KZ']:.0%}`\n\n"
        f"{'✅ Bot ARMED — NY KZ opens 8:00 AM ET' if allowed else f'🚫 PAUSED — {reason}'}"
    )
    await send_telegram(text)

async def report_755am():
    lvl_mes = store.get("MES")
    allowed, reason = stats.can_trade()
    ts = tuner.status()
    s = stats.status()
    text = (
        f"⚡ *Pre-Session Brief* — NY Kill Zone in 5 mins\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎯 *MES Levels:*\n"
        f"PDH {fmt_level(lvl_mes['PDH'])} PDL {fmt_level(lvl_mes['PDL'])}\n"
        f"AsiaH {fmt_level(lvl_mes['AsiaH'])} AsiaL {fmt_level(lvl_mes['AsiaL'])}\n"
        f"LonH {fmt_level(lvl_mes['LonH'])} LonL {fmt_level(lvl_mes['LonL'])}\n\n"
        f"🛡️ *Kill Conditions*\n"
        f"  Day P&L: `${stats.day_pnl:+.2f}` | Day loss room: `${s['day_loss_remaining']:.0f}`\n"
        f"  EOD floor: `${stats.trailing_floor:.0f}` | Drawdown room: `${s['drawdown_remaining']:.0f}`\n\n"
        f"💰 *Payout Progress*\n"
        f"  `${s['to_payout']:.0f}` to next payout | `{stats.payout_count}/5` complete\n\n"
        f"🧠 *Active Tuning*\n"
        f"  Buf: `{ts['sweep_buf']['MES']['normal']}pts` | "
        f"NYKZ dir: `{ts['session_direction_bias']['NY_KZ'] or 'BOTH'}` | "
        f"Size: `{ts['session_multiplier']['NY_KZ']:.0%}`\n\n"
        f"{'🟢 BOT LIVE — NY KZ window: 8:00–8:30 AM ET' if allowed else f'🔴 PAUSED — {reason}'}"
    )
    await send_telegram(text)

async def report_eod():
    """4:30 PM ET (1:30 PM PT) — end of day summary after NY session closes."""
    s      = stats.status()
    ts     = tuner.status()
    allowed, reason = stats.can_trade()
    today_trades = stats.day_trades_log
    today_wins   = sum(1 for t in today_trades if t["pnl"] > 0)
    today_losses = sum(1 for t in today_trades if t["pnl"] <= 0)
    today_pnl    = stats.day_pnl
    day_emoji    = "🟢" if today_pnl > 0 else "🔴" if today_pnl < 0 else "⚪"

    best_str  = "—"
    worst_str = "—"
    if today_trades:
        best      = max(today_trades, key=lambda t: t["pnl"])
        worst     = min(today_trades, key=lambda t: t["pnl"])
        best_str  = f"${best['pnl']:+.0f} ({best.get('inst','?')} {best.get('session','?')})"
        worst_str = f"${worst['pnl']:+.0f} ({worst.get('inst','?')} {worst.get('session','?')})"

    session_lines = []
    for sess in ["Asia", "London", "NY_KZ"]:
        st = [t for t in today_trades if t.get("session") == sess]
        if st:
            sw   = sum(1 for t in st if t["pnl"] > 0)
            spnl = sum(t["pnl"] for t in st)
            session_lines.append(f"  {sess}: {len(st)} trades {sw}W/${spnl:+.0f}")
    session_str = "\n".join(session_lines) if session_lines else "  No trades today"

    lines = [
        f"{day_emoji} *End of Day Report* — {datetime.now(EST).strftime('%b %d, %Y')}",
        "━━━━━━━━━━━━━━━━━━━━━",
        "",
        "📊 *Today*",
        f"  P&L: `${today_pnl:+.2f}` | {len(today_trades)} trades ({today_wins}W / {today_losses}L)",
        f"  Best: `{best_str}` | Worst: `{worst_str}`",
        "",
        "*By Session:*",
        session_str,
        "",
        "💼 *Account*",
        f"  Total P&L: `${stats.total_pnl:+.2f}`",
        f"  EOD Floor: `${stats.trailing_floor:.0f}` | Drawdown room: `${s['drawdown_remaining']:.0f}`",
        f"  Day loss room: `${s['day_loss_remaining']:.0f}` of -$1,000",
        f"  To payout: `${s['to_payout']:.0f}` | Payouts: `{stats.payout_count}/5` → live",
        "",
        "🧠 *Tuner*",
        f"  Cycle #{ts['tune_count']} | Next tune in {ts['next_tune_in']} trades",
        f"  MES buf: {ts['sweep_buf']['MES']['normal']}pts | WR: {s['win_rate']}% ({s['wins']}W/{s['losses']}L)",
        "",
        "✅ Account healthy — Asia opens 8PM ET" if allowed else f"🚫 PAUSED — {reason}",
    ]
    await send_telegram("\n".join(lines))
    logger.info("📬 EOD report sent")

async def report_weekly():
    text = tuner.weekly_report()
    await send_telegram(text)
    logger.info("📬 Weekly report sent")

# ── SCHEDULER ─────────────────────────────────────────────────────────────────
async def scheduler():
    sent_6am    = False
    sent_755    = False
    sent_eod    = False
    sent_weekly = False
    last_date   = date.today()
    while True:
        await asyncio.sleep(30)
        now   = datetime.now(EST)
        today = now.date()
        if today != last_date:
            sent_6am  = False
            sent_755  = False
            sent_eod  = False
            last_date = today
            store.check_midnight_reset()
        h, m = now.hour, now.minute
        dow  = now.weekday()   # 6 = Sunday
        if h == 6  and m == 0  and not sent_6am:    sent_6am    = True; await report_6am()
        if h == 7  and m == 55 and not sent_755:     sent_755    = True; await report_755am()
        if h == 16 and m == 30 and not sent_eod:     sent_eod    = True; await report_eod()
        if h == 8  and m == 0  and dow == 6 and not sent_weekly:
            sent_weekly = True; await report_weekly()

# ── PMT WEBHOOK ───────────────────────────────────────────────────────────────
async def _send_pmt(inst: str, direction: str, qty: int,
                    dollar_tp: float, dollar_sl: float,
                    suffix: str) -> tuple[bool, str]:
    cfg = INSTRUMENTS[inst]
    pv       = cfg["point_value"]
    sl_pts   = dollar_sl / qty / pv
    tp_pts   = dollar_tp / qty / pv
    if direction.upper() == "BUY":
        sl_price = round(prices.get(inst, 0) - sl_pts, 2)
        tp_price = round(prices.get(inst, 0) + tp_pts, 2)
    else:
        sl_price = round(prices.get(inst, 0) + sl_pts, 2)
        tp_price = round(prices.get(inst, 0) - tp_pts, 2)
    payload = {
        "symbol":                f"{cfg['pmt']}1!",
        "strategy_name":         f"AlphaGrid_{inst}_{suffix}",
        "date":                  datetime.now(EST).strftime("%Y-%m-%dT%H:%M:%S"),
        "data":                  direction.lower(),
        "quantity":              str(qty),
        "risk_percentage":       0,
        "price":                 str(prices.get(inst, 0)),
        "tp":                    tp_price,
        "percentage_tp":         0,
        "dollar_tp":             0,
        "sl":                    sl_price,
        "dollar_sl":             0,
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

async def fire_trade_legs(sig: dict, session: str) -> tuple[bool, bool, str, str]:
    inst = sig["inst"]
    qty1 = tuner.get_qty(LEG1_CONTRACTS, session)
    qty2 = tuner.get_qty(LEG2_CONTRACTS, session)
    leg1, leg2 = await asyncio.gather(
        _send_pmt(inst, sig["direction"], qty1, LEG1_TP, LEG1_SL, "L1"),
        _send_pmt(inst, sig["direction"], qty2, LEG2_TP, LEG2_SL, "L2"),
    )
    return leg1[0], leg2[0], leg1[1], leg2[1]

# ── SIGNAL ENGINE ─────────────────────────────────────────────────────────────
async def _execute_signal(inst: str, direction: str, swept_level: tuple,
                          price: float, session: str, now: datetime,
                          trigger: str = "SWEEP"):
    """Execute a confirmed trade signal — shared by sweep and FVG retest paths."""
    cfg       = INSTRUMENTS[inst]
    pv        = cfg["point_value"]
    qty1      = tuner.get_qty(LEG1_CONTRACTS, session)
    qty2      = tuner.get_qty(LEG2_CONTRACTS, session)
    l1_tp_pts = LEG1_TP / qty1 / pv
    l2_tp_pts = LEG2_TP / qty2 / pv
    sl_pts    = LEG1_SL / qty1 / pv
    buf       = tuner.get_buf(inst, stats.tight_mode)

    if direction == "BUY":
        stop = price - sl_pts; l1_target = price + l1_tp_pts
        l2_target = price + l2_tp_pts; sl_price = price - sl_pts
    else:
        stop = price + sl_pts; l1_target = price - l1_tp_pts
        l2_target = price - l2_tp_pts; sl_price = price + sl_pts

    sig = {
        "id": str(uuid.uuid4())[:8], "inst": inst, "direction": direction,
        "entry": price, "stop": stop,
        "leg1_tp": LEG1_TP, "leg1_sl": LEG1_SL, "leg1_qty": qty1,
        "leg2_tp": LEG2_TP, "leg2_sl": LEG2_SL, "leg2_qty": qty2,
        "contracts": qty1 + qty2, "session": session,
        "swept": swept_level[0], "tight_mode": stats.tight_mode,
        "trigger": trigger, "ts": now.strftime("%H:%M ET"),
    }
    last_signal[inst] = now.timestamp()

    allowed, reason = stats.can_trade()
    if not allowed:
        logger.info(f"🚫 Blocked [{inst}]: {reason}")
        await send_telegram(f"🚫 *Signal blocked* — {inst} {direction}\n_{reason}_")
        return

    logger.info(f"🚀 {inst} {direction} @ {price:.2f} | {trigger} | {session} | {swept_level[0]}")
    l1_ok, l2_ok, l1_body, l2_body = await fire_trade_legs(sig, session)

    if l1_ok or l2_ok:
        trades.insert(0, {**sig, "status": "EXECUTED",
                          "l1_ok": l1_ok, "l2_ok": l2_ok,
                          "executed_at": now.strftime("%H:%M ET")})
        await broadcast({"type": "executed", "sig": sig, "stats": stats.status()})
        mode_tag = "🔒 TIGHT" if stats.tight_mode else "✅ NORMAL"
        wr_str   = f"{stats.win_rate:.0%} ({stats.wins}W/{stats.losses}L)" if stats.total_trades else "—"
        mult     = tuner.session_multiplier.get(session, 1.0)
        trigger_tag = "🎯 FVG Retest" if trigger == "FVG_RETEST" else "💧 Sweep"
        await send_telegram(
            f"🤖 *Auto-Trade Fired* [{mode_tag}] {trigger_tag}\n"
            f"`{inst}` {direction} @ `{price:,.2f}`\n\n"
            f"{'✅' if l1_ok else '❌'} *L1* `{qty1}ct` needs `{l1_tp_pts:.1f}pts` → `{l1_target:,.2f}` TP `+${LEG1_TP:.0f}`\n"
            f"{'✅' if l2_ok else '❌'} *L2* `{qty2}ct` needs `{l2_tp_pts:.1f}pts` → `{l2_target:,.2f}` TP `+${LEG2_TP:.0f}` 🏃\n"
            f"🛑 SL `{sl_pts:.1f}pts` → `{sl_price:,.2f}` `-${LEG1_SL:.0f}`\n\n"
            f"Session: {session} `{mult:.0%}` | Swept: {swept_level[0]} | Buf: `{buf}pts`\n"
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

async def check_signals(inst: str, price: float, now: datetime):
    session = get_session(now)
    if not session:
        return

    cfg      = INSTRUMENTS[inst]
    levels   = store.get(inst)
    buf      = tuner.get_buf(inst, stats.tight_mode)
    cooldown = tuner.get_cooldown(session, stats.tight_mode)
    fvg      = fvg_states[inst]
    bars     = bar_tracker.get_bars(inst)

    # ── STAGE 2: FVG watch active ─────────────────────────────────────────────
    if fvg.sweep_active:
        if fvg.is_timed_out(now):
            logger.info(f"⏱️ FVG timeout [{inst}] — resetting")
            fvg.reset()
        else:
            # Check for FVG on latest 1M bars
            if not fvg.fvg_detected and len(bars) >= 3:
                if fvg.check_fvg(bars):
                    logger.info(f"📐 FVG [{inst}] {fvg.sweep_direction} zone {fvg.fvg_low:.2f}–{fvg.fvg_high:.2f}")
                    await send_telegram(
                        f"📐 *FVG Detected* — {inst} {fvg.sweep_direction}\n"
                        f"Zone: `{fvg.fvg_low:.2f}` – `{fvg.fvg_high:.2f}`\n"
                        f"Waiting for price to retest…"
                    )
            # Fire when price enters FVG zone
            if fvg.fvg_detected and fvg.price_in_fvg(price):
                if (now.timestamp() - last_signal[inst]) >= cooldown:
                    direction   = fvg.sweep_direction
                    swept_level = (fvg.sweep_level_key, fvg.sweep_level_val)
                    fvg.reset()
                    await _execute_signal(inst, direction, swept_level,
                                          price, session, now, "FVG_RETEST")
            return   # Sweep active — don't check for new sweeps

    # ── STAGE 1: Sweep detection ──────────────────────────────────────────────
    if (now.timestamp() - last_signal[inst]) < cooldown:
        return
    if not level_proximity_ok(inst, price):
        return

    base_lows  = ["AsiaL", "PDL"]
    base_highs = ["AsiaH"]
    if session == "NY_KZ":
        base_lows.append("LonL")

    direction   = None
    swept_level = None

    for k in base_lows:
        lvl = levels.get(k)
        if lvl and price < lvl - buf:
            direction, swept_level = "BUY", (k, lvl)
            break

    if not direction and session == "NY_KZ":
        for k in base_highs:
            lvl = levels.get(k)
            if lvl and price > lvl + buf:
                direction, swept_level = "SELL", (k, lvl)
                break

    if not direction:
        return

    if not tuner.direction_allowed(session, direction):
        logger.info(f"🧭 {inst} {direction} suppressed by bias in {session}")
        return

    # Activate FVG watch — don't fire yet
    fvg.activate_sweep(direction, swept_level[0], swept_level[1], price, now)
    logger.info(f"👁️ Sweep [{inst}] {direction} @ {price:.2f} — watching for FVG")
    await send_telegram(
        f"👁️ *Sweep Detected* — {inst} {direction}\n"
        f"Price `{price:.2f}` swept {swept_level[0]} @ `{swept_level[1]:.2f}`\n"
        f"Watching 1M bars for FVG… (15 min window)"
    )

# ── LIFESPAN ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(scheduler())
    logger.info("AlphaGrid All Night Bot — self-tuning autonomous mode 🧠🤖")
    logger.info("NY KZ: 8:00–8:30 AM ET only (backtest filter v2)")
    logger.info("FVG engine: 2-stage sweep→FVG→retest (15min timeout)")
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    allowed, reason = stats.can_trade()
    return {
        "status": "ok", "trading": allowed, "reason": reason,
        "session": get_session(datetime.now(EST)),
        "tight_mode": stats.tight_mode,
        "prices": prices,
        "levels": store.all_status(),
        **stats.status(),
        "tuner": tuner.status(),
        "fvg": {
            inst: {
                "sweep_active":    fvg_states[inst].sweep_active,
                "sweep_direction": fvg_states[inst].sweep_direction,
                "sweep_level":     fvg_states[inst].sweep_level_key,
                "fvg_detected":    fvg_states[inst].fvg_detected,
                "fvg_low":         fvg_states[inst].fvg_low,
                "fvg_high":        fvg_states[inst].fvg_high,
                "bars_buffered":   len(bar_tracker.get_bars(inst)),
            }
            for inst in ACTIVE_INSTRUMENTS
        },
    }

@app.post("/price-update")
async def price_update(req: Request):
    body  = await req.json()
    inst  = body.get("ticker", "").upper().replace("1!", "").replace("!", "")
    price = float(body.get("price", 0))
    if inst not in INSTRUMENTS or inst not in ACTIVE_INSTRUMENTS or price <= 0:
        return {"ok": False, "reason": f"{inst} not active"}
    prices[inst] = price
    now = datetime.now(EST)
    store.check_midnight_reset()
    # Update 1M bar tracker — builds OHLC bars from tick stream for FVG detection
    bar_tracker.update(inst, price, now)
    await check_signals(inst, price, now)
    await broadcast({"type": "price", "inst": inst, "price": price,
                     "levels": store.get(inst), "stats": stats.status(),
                     "fvg_active": fvg_states[inst].sweep_active})
    return {"ok": True, "inst": inst, "price": price}

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
    inst = p.inst.upper()
    if inst not in INSTRUMENTS:
        return {"ok": False, "reason": "unknown instrument"}
    data = {k: getattr(p, k) for k in ["PDH","PDL","AsiaH","AsiaL","LonH","LonL"]
            if getattr(p, k) is not None}
    source = "1H_pine" if "PDH" in data or "PDL" in data else \
             "15M_pine_asia" if "AsiaH" in data else "15M_pine_london"
    store.set_many(inst, data, source)
    await broadcast({"type": "levels", "inst": inst, "levels": store.get(inst)})
    return {"ok": True, "inst": inst, "levels": store.get(inst)}

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
    swept:     Optional[str] = None

@app.post("/result")
async def record_result(p: ResultPayload):
    meta = {"inst": p.inst, "direction": p.direction,
            "session": p.session, "ts": datetime.now(EST).strftime("%H:%M ET")}
    locked = stats.record(p.pnl, meta)
    s = stats.status()

    # Feed tuner
    fake_sig = {"id": "ext", "inst": p.inst or "MES", "direction": p.direction or "BUY",
                "session": p.session or "?", "swept": p.swept or "?", "entry": 0}
    rec = TradeRecord(fake_sig, p.pnl, p.won)
    tuner.record(rec)

    # Retune every TUNE_EVERY trades
    if tuner.trades_since_tune >= TUNE_EVERY:
        changes = tuner.tune()
        if changes:
            change_text = "\n".join(f"  {c}" for c in changes)
            await send_telegram(
                f"🧠 *AlphaGrid Auto-Tune* — Cycle #{tuner.tune_count}\n"
                f"After {len(tuner.all_trades)} total trades:\n\n"
                f"{change_text}"
            )
        else:
            await send_telegram(
                f"🧠 *Auto-Tune #{tuner.tune_count}* — No changes needed\n"
                f"All parameters performing well ✅"
            )

    await broadcast({"type": "result", "pnl": p.pnl, "stats": s})
    if locked:
        await send_telegram(
            f"⛔ *ACCOUNT LOCKED — EOD Trailing Drawdown Hit*\n"
            f"Total P&L: `${stats.total_pnl:.0f}` | Floor: `${stats.trailing_floor:.0f}`\n"
            f"Bot paused — manual reset required."
        )
    elif stats.payout_eligible:
        await send_telegram(
            f"💰 *PAYOUT ELIGIBLE!*\n"
            f"Total P&L: `${stats.total_pnl:+.0f}` — above buffer + threshold\n"
            f"You can request a payout of up to `$2,000` from MFFU.\n"
            f"Payout #{stats.payout_count + 1} of 5 toward live account."
        )
    return s

@app.get("/stats")
async def get_stats():
    allowed, reason = stats.can_trade()
    return {**stats.status(), "trading_allowed": allowed, "reason": reason,
            "levels": store.all_status(), "tuner": tuner.status()}

@app.get("/tuner")
async def get_tuner():
    return tuner.status()

@app.post("/report/now")
async def send_report_now():
    await report_6am()
    await asyncio.sleep(1)
    await report_755am()
    return {"ok": True}

@app.post("/report/eod")
async def send_eod_now():
    await report_eod()
    return {"ok": True}

@app.post("/report/weekly")
async def send_weekly_now():
    await report_weekly()
    return {"ok": True}

@app.post("/test-trade")
async def test_trade():
    # Use live price if available, otherwise fetch from a reasonable default
    test_price = prices.get("MES", 0)
    if test_price <= 0:
        test_price = 7616.0   # fallback to approximate current price
    # Temporarily set price so SL/TP calculation works
    prices["MES"] = test_price
    sig = {
        "id": "TEST001", "inst": "MES", "direction": "BUY",
        "entry": test_price, "stop": test_price - 6.75,
        "session": "TEST", "swept": "AsiaL", "tight_mode": False,
        "ts": datetime.now(EST).strftime("%H:%M ET"),
    }
    pv = INSTRUMENTS["MES"]["point_value"]
    qty1 = tuner.get_qty(LEG1_CONTRACTS, "NY_KZ")
    qty2 = tuner.get_qty(LEG2_CONTRACTS, "NY_KZ")
    l1_tp_pts = LEG1_TP / qty1 / pv
    l2_tp_pts = LEG2_TP / qty2 / pv
    sl_pts    = LEG1_SL / qty1 / pv
    sl_price  = round(test_price - sl_pts, 2)
    l1_tp_price = round(test_price + l1_tp_pts, 2)
    l2_tp_price = round(test_price + l2_tp_pts, 2)
    logger.info(f"🧪 TEST TRADE @ {test_price:.2f} | SL={sl_price:.2f} ({sl_pts:.1f}pts) | L1 TP={l1_tp_price:.2f} | L2 TP={l2_tp_price:.2f}")
    l1_ok, l2_ok, l1_body, l2_body = await fire_trade_legs(sig, "NY_KZ")
    status = "✅" if l1_ok else "❌"
    status2 = "✅" if l2_ok else "❌"
    msg = (
        f"🧪 *TEST TRADE — Pipeline Verification*\n"
        f"MES BUY @ `{test_price:,.2f}`\n\n"
        f"{status} L1 `{qty1}ct` → TP `{l1_tp_price:.2f}` (+{l1_tp_pts:.1f}pts) SL `{sl_price:.2f}` (-{sl_pts:.1f}pts)\n"
        f"{status2} L2 `{qty2}ct` → TP `{l2_tp_price:.2f}` (+{l2_tp_pts:.1f}pts) SL `{sl_price:.2f}`\n\n"
        f"L1: `{l1_body[:80]}`\nL2: `{l2_body[:80]}`"
    )
    await send_telegram(msg)
    return {"ok": l1_ok or l2_ok,
            "test_price": test_price,
            "sl_price": sl_price,
            "sl_pts": sl_pts,
            "l1_tp": l1_tp_price,
            "l2_tp": l2_tp_price,
            "l1_ok": l1_ok, "l1_body": l1_body[:200],
            "l2_ok": l2_ok, "l2_body": l2_body[:200]}

@app.post("/reset_day")
async def reset_day():
    stats.day_pnl  = 0.0
    stats.day_date = date.today()
    return {"ok": True}

@app.post("/reset_stats")
async def reset_all_stats():
    stats.__init__()
    return {"ok": True}

@app.get("/state")
async def get_state():
    allowed, reason = stats.can_trade()
    return {**stats.status(), "trading_allowed": allowed,
            "levels": store.all_status(), "prices": prices}

@app.websocket("/ws")
async def ws_ep(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)
    await ws.send_text(json.dumps({
        "type": "init", "prices": prices, "trades": trades[:20],
        "stats": stats.status(), "levels": store.all_status(),
        "tuner": tuner.status(),
        "config": {
            "leg1_contracts": LEG1_CONTRACTS, "leg1_tp": LEG1_TP, "leg1_sl": LEG1_SL,
            "leg2_contracts": LEG2_CONTRACTS, "leg2_tp": LEG2_TP, "leg2_sl": LEG2_SL,
            "max_day_loss": MAX_DAY_LOSS, "win_rate_threshold": WIN_RATE_THRESHOLD,
        }
    }))
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        try: ws_clients.remove(ws)
        except: pass
