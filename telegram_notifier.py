import aiohttp
import logging
from typing import Optional

logger = logging.getLogger("telegram")

class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str, dashboard_url: str = ""):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.dashboard_url = dashboard_url
        self._base = f"https://api.telegram.org/bot{bot_token}"

    async def send_plain(self, message: str) -> Optional[int]:
        return await self._send_message(message)

    async def send_signal(self, signal) -> Optional[int]:
        direction_emoji = "🟢" if signal.direction == "LONG" else "🔴"
        trend_emoji = "📈" if signal.trend == "BULLISH" else "📉"
        msg = (
            f"{direction_emoji} *ALPHAGRID SIGNAL*\n"
            f"━━━━━━━━━━━━━━\n"
            f"*{signal.direction}* {signal.inst} @ `{signal.level}`\n\n"
            f"{trend_emoji} Trend: *{signal.trend}*\n"
            f"🎯 Entry:  `{signal.entry:,.2f}`\n"
            f"🛑 Stop:   `{signal.stop:,.2f}`\n"
            f"✅ Target: `{signal.target:,.2f}`\n\n"
            f"⚖️ R:R = 2:1  |  Confidence: {signal.confidence:.0f}%\n"
            f"⏱ {signal.timestamp.strftime('%H:%M:%S')} UTC"
        )
        keyboard = {"inline_keyboard": [[
            {"text": f"{'🟢 BUY' if signal.direction=='LONG' else '🔴 SELL'} — EXECUTE",
             "url": f"{self.dashboard_url}?exec={signal.inst}"},
            {"text": "⏭ Skip", "callback_data": f"skip_{signal.inst}"}
        ]]}
        return await self._send_message(msg, reply_markup=keyboard)

    async def _send_message(self, text: str, reply_markup=None, parse_mode: str = "Markdown") -> Optional[int]:
        url = f"{self._base}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": parse_mode}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    data = await resp.json()
                    if data.get("ok"):
                        return data["result"]["message_id"]
                    else:
                        logger.error(f"Telegram error: {data.get('description')}")
                        return None
        except Exception as e:
            logger.error(f"Telegram failed: {e}")
            return None
