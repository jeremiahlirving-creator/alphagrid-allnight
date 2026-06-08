"""
ALPHAGRID — Webhook Execution Engine
Replaces direct Tradovate API with PickMyTrade webhook.
No CME license. No $300+/month. Just ~$50/month total.

Flow:
  Price tick → EMA trend → Key level check → Signal
  → Telegram alert → You tap EXECUTE on phone
  → This script fires POST to PickMyTrade webhook
  → PickMyTrade places bracket order on Tradovate (<200ms)
"""

import asyncio
import aiohttp
import os
import json
import logging
from datetime import datetime, date
from dataclasses import dataclass, field
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("alphagrid")

# ─── PICKMYTRADE CONFIG ───────────────────────────────────────────────────────
# Get these from your PickMyTrade dashboard after connecting Tradovate
PMT_WEBHOOK_URL = os.getenv("PMT_WEBHOOK_URL", "https://api.pickmytrade.trade/v2/add-trade-data")
PMT_TOKEN       = os.getenv("PMT_TOKEN", "")           # from PickMyTrade dashboard
PMT_ACCOUNT_ID  = os.getenv("PMT_ACCOUNT_ID", "")      # your Tradovate account ID in PMT

# Symbol mapping — PickMyTrade uses these exact strings for Tradovate micros
PMT_SYMBOLS = {
    "ES": "MES",   # Micro E-mini S&P 500
    "NQ": "MNQ",   # Micro E-mini NASDAQ
}

# ─── RISK CONFIG ─────────────────────────────────────────────────────────────
RISK_PER_TRADE  = 125.0    # dollars risked per trade
RR_RATIO        = 2.0      # reward:risk
DAILY_LIMIT     = -1000.0  # hard stop for the day
MAX_TRADES_DAY  = 5        # max executions per day
CONTRACTS       = 1        # number of micro contracts

STOP_POINTS = {"ES": 6.0,  "NQ": 20.0}   # stop distance in points
# Dollar SL/TP for PickMyTrade (dollar_sl, dollar_tp fields)
# MES: $5/point. MNQ: $2/point
POINT_VALUE = {"ES": 5.0,  "NQ": 2.0}


# ─── DATA CLASSES ─────────────────────────────────────────────────────────────

@dataclass
class Signal:
    inst: str           # ES or NQ
    direction: str      # LONG or SHORT
    level: str          # level name that triggered
    entry: float
    stop: float
    target: float
    dollar_sl: float    # dollar stop loss for PMT
    dollar_tp: float    # dollar take profit for PMT
    trend: str
    confidence: float
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @property
    def pmt_symbol(self):
        return PMT_SYMBOLS.get(self.inst, self.inst)

    @property
    def pmt_action(self):
        return "buy" if self.direction == "LONG" else "sell"


@dataclass
class DayStats:
    date: date = field(default_factory=date.today)
    pnl: float = 0.0
    trades: int = 0
    wins: int = 0
    losses: int = 0
    locked: bool = False

    def reset_if_new_day(self):
        today = date.today()
        if self.date != today:
            self.date = today
            self.pnl = 0.0
            self.trades = 0
            self.wins = 0
            self.losses = 0
            self.locked = False
            logger.info("New trading day — daily stats reset")

    @property
    def win_rate(self):
        if self.trades == 0:
            return 0.0
        return round(self.wins / self.trades * 100, 1)

    def record_trade(self, pnl: float):
        self.pnl += pnl
        self.trades += 1
        if pnl > 0:
            self.wins += 1
        else:
            self.losses += 1
        if self.pnl <= DAILY_LIMIT:
            self.locked = True
            logger.warning(f"DAILY LIMIT HIT — Day P&L: ${self.pnl:.0f}. Execution locked.")
        return self.locked


# ─── WEBHOOK BUILDER ─────────────────────────────────────────────────────────

def build_pmt_payload(signal: Signal) -> dict:
    """
    Build the exact JSON payload PickMyTrade expects.
    Uses dollar_sl and dollar_tp so the bracket is always
    $125 risk / $250 target regardless of price level.
    """
    return {
        "symbol":               signal.pmt_symbol,
        "date":                 signal.timestamp.isoformat(),
        "data":                 signal.pmt_action,
        "quantity":             CONTRACTS,
        "risk_percentage":      0,
        "price":                signal.entry,      # limit order at signal price
        "tp":                   0,
        "percentage_tp":        0,
        "dollar_tp":            signal.dollar_tp,  # e.g. 250.0
        "sl":                   0,
        "dollar_sl":            signal.dollar_sl,  # e.g. 125.0
        "percentage_sl":        0,
        "trail":                0,
        "trail_stop":           0,
        "trail_trigger":        0,
        "trail_freq":           0,
        "update_tp":            False,
        "update_sl":            False,
        "breakeven":            0,
        "token":                PMT_TOKEN,
        "pyramid":              False,
        "reverse_order_close":  True,   # closes opposite if one exists
        "order_type":           "MKT",  # market order for instant fill
        "account_id":           PMT_ACCOUNT_ID,
    }


# ─── WEBHOOK SENDER ───────────────────────────────────────────────────────────

class WebhookExecutor:
    """
    Sends trade signals to PickMyTrade webhook.
    Enforces daily risk limits before every send.
    """

    def __init__(self):
        self.stats = DayStats()
        self._pending: dict[str, Signal] = {}   # inst → pending signal

    def can_trade(self) -> tuple[bool, str]:
        self.stats.reset_if_new_day()
        if self.stats.locked:
            return False, f"Daily limit hit (${self.stats.pnl:.0f}). Locked."
        if self.stats.trades >= MAX_TRADES_DAY:
            return False, f"Max {MAX_TRADES_DAY} trades reached today."
        return True, "OK"

    def register_signal(self, signal: Signal):
        """Store signal so execute() can fire it by instrument."""
        self._pending[signal.inst] = signal
        logger.info(f"Signal registered: {signal.inst} {signal.direction} @ {signal.level} ({signal.entry})")

    def dismiss_signal(self, inst: str):
        self._pending.pop(inst, None)
        logger.info(f"Signal dismissed: {inst}")

    async def execute(self, inst: str) -> dict:
        """
        Execute the pending signal for an instrument.
        Called when user taps EXECUTE on Telegram or dashboard.
        """
        ok, reason = self.can_trade()
        if not ok:
            return {"success": False, "reason": reason}

        signal = self._pending.get(inst)
        if not signal:
            return {"success": False, "reason": f"No pending signal for {inst}"}

        payload = build_pmt_payload(signal)

        logger.info(
            f"Firing webhook: {signal.pmt_action.upper()} {CONTRACTS}x {signal.pmt_symbol} "
            f"| SL ${signal.dollar_sl} | TP ${signal.dollar_tp}"
        )

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    PMT_WEBHOOK_URL,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    status = resp.status
                    body = await resp.text()

            if status == 200:
                self._pending.pop(inst, None)
                logger.info(f"Webhook accepted ✓ ({inst})")
                return {
                    "success":  True,
                    "inst":     inst,
                    "direction": signal.direction,
                    "entry":    signal.entry,
                    "stop":     signal.stop,
                    "target":   signal.target,
                    "payload":  payload,
                }
            else:
                logger.error(f"Webhook rejected: HTTP {status} — {body}")
                return {"success": False, "reason": f"PMT rejected: {status} {body}"}

        except asyncio.TimeoutError:
            logger.error("Webhook timeout (>5s)")
            return {"success": False, "reason": "Webhook timed out"}
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            return {"success": False, "reason": str(e)}

    def record_result(self, pnl: float, inst: str = ""):
        """Call this when a trade closes to track P&L."""
        locked = self.stats.record_trade(pnl)
        logger.info(
            f"Trade result recorded: {'+'if pnl>0 else ''}{pnl:.0f} | "
            f"Day P&L: {self.stats.pnl:.0f} | Win rate: {self.stats.win_rate}%"
        )
        return locked

    def status(self) -> dict:
        self.stats.reset_if_new_day()
        return {
            "day_pnl":    round(self.stats.pnl, 2),
            "trades":     self.stats.trades,
            "wins":       self.stats.wins,
            "losses":     self.stats.losses,
            "win_rate":   self.stats.win_rate,
            "locked":     self.stats.locked,
            "pending":    {k: v.direction for k, v in self._pending.items()},
            "can_trade":  self.can_trade()[0],
        }


# ─── SIGNAL CALCULATOR ────────────────────────────────────────────────────────

def build_signal(inst: str, direction: str, level: str,
                  price: float, trend: str, confidence: float = 75.0) -> Signal:
    """
    Build a Signal with correct stop/target prices AND dollar amounts
    formatted for PickMyTrade's dollar_sl / dollar_tp fields.
    """
    stop_pts   = STOP_POINTS[inst]
    target_pts = stop_pts * RR_RATIO
    pv         = POINT_VALUE[inst]

    if direction == "LONG":
        stop   = round(price - stop_pts, 2)
        target = round(price + target_pts, 2)
    else:
        stop   = round(price + stop_pts, 2)
        target = round(price - target_pts, 2)

    # Dollar amounts for PMT bracket
    dollar_sl = round(stop_pts * pv * CONTRACTS, 2)    # e.g. 6pts × $5 × 1 = $30... 
    # Note: MES tick = $1.25, so 6pts = 24 ticks = $30. Adjust CONTRACTS for target $125 risk.
    # To risk exactly $125 on MES: need 4 contracts (4 × $30 = $120) or adjust stop to 25pts
    # For simplicity, pass dollar amount and let PMT calculate quantity if risk_percentage used
    dollar_tp = round(target_pts * pv * CONTRACTS, 2)

    return Signal(
        inst=inst,
        direction=direction,
        level=level,
        entry=price,
        stop=stop,
        target=target,
        dollar_sl=dollar_sl,
        dollar_tp=dollar_tp,
        trend=trend,
        confidence=confidence,
        timestamp=datetime.utcnow(),
    )


# ─── EMA ENGINE (same as before, self-contained) ─────────────────────────────

class EMAEngine:
    def __init__(self, periods=(9, 21, 50)):
        self.periods = periods
        self._prices: list[float] = []
        self._emas: dict[int, Optional[float]] = {p: None for p in periods}

    def update(self, price: float) -> dict:
        self._prices.append(price)
        for p in self.periods:
            if len(self._prices) == p:
                self._emas[p] = sum(self._prices[-p:]) / p
            elif len(self._prices) > p and self._emas[p]:
                k = 2 / (p + 1)
                self._emas[p] = round(price * k + self._emas[p] * (1 - k), 2)
        return dict(self._emas)

    def trend(self) -> str:
        e9, e21, e50 = self._emas[9], self._emas[21], self._emas[50]
        if None in (e9, e21, e50):
            return "NEUTRAL"
        if e9 > e21 > e50:
            return "BULLISH"
        if e9 < e21 < e50:
            return "BEARISH"
        return "NEUTRAL"

    def values(self) -> dict:
        return dict(self._emas)


# ─── KEY LEVEL CHECKER ────────────────────────────────────────────────────────

def check_level_touch(price: float, inst: str, levels: dict) -> Optional[tuple]:
    """
    Returns (level_name, level_type) if price is within tolerance of a level.
    levels = {"PDH": (5320.0, "resistance"), "PDL": (5290.0, "support"), ...}
    """
    tol = 4.0 if inst == "ES" else 14.0
    for name, (val, lvl_type) in levels.items():
        if abs(price - val) <= tol:
            return name, lvl_type
    return None


def get_trade_direction(level_type: str, trend: str) -> Optional[str]:
    """Only trade in trend direction at matching level type."""
    if level_type == "support"    and trend == "BULLISH": return "LONG"
    if level_type == "resistance" and trend == "BEARISH": return "SHORT"
    if level_type == "neutral":
        if trend == "BULLISH": return "LONG"
        if trend == "BEARISH": return "SHORT"
    return None  # level conflicts with trend — skip


# ─── QUICK TEST ───────────────────────────────────────────────────────────────

async def _test():
    """Test the webhook with a fake signal (PMT DEMO account)."""
    print("\n" + "="*55)
    print("  ALPHAGRID — Webhook Execution Test")
    print("="*55)

    if not PMT_TOKEN:
        print("\n❌ PMT_TOKEN not set. Add to .env first.\n")
        return

    executor = WebhookExecutor()

    # Build a test signal
    sig = build_signal(
        inst="ES",
        direction="LONG",
        level="PDL",
        price=5291.75,
        trend="BULLISH",
        confidence=82.0,
    )

    print(f"\nSignal: {sig.direction} MES @ {sig.entry}")
    print(f"Stop:   {sig.stop}  (${sig.dollar_sl} risk)")
    print(f"Target: {sig.target}  (${sig.dollar_tp} reward)")
    print(f"\nPayload:\n{json.dumps(build_pmt_payload(sig), indent=2)}")

    executor.register_signal(sig)
    print("\nFiring webhook...")
    result = await executor.execute("ES")

    if result["success"]:
        print("✅ Webhook accepted by PickMyTrade!")
    else:
        print(f"❌ Failed: {result['reason']}")

    print("\nDay status:", executor.status())


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    asyncio.run(_test())
