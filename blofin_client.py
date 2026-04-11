"""
alert_manager.py — Telegram alerts and structured logging.
Every significant event (trade, halt, error, daily summary) is sent here.
Designed to be non-blocking — alert failures never crash a bot.
"""

import asyncio
import logging
import logging.handlers
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, SYSTEM, LOG_FORMAT, LOG_DATE_FORMAT

log = logging.getLogger("alerts")


def setup_logging() -> None:
    """Configure root logger with file rotation + console output."""
    root = logging.getLogger()
    root.setLevel(getattr(logging, SYSTEM["log_level"], logging.INFO))

    fmt = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)

    # Console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # Rotating file (10 MB × 5 files)
    fh = logging.handlers.RotatingFileHandler(
        SYSTEM["log_file"], maxBytes=10 * 1024 * 1024, backupCount=5
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)


class AlertManager:
    """
    Send Telegram messages and structured trade alerts.
    Messages are queued and sent asynchronously. If Telegram is down,
    messages are logged locally — the trading system continues.
    """

    TELEGRAM_API = "https://api.telegram.org"

    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            base_url=self.TELEGRAM_API,
        )
        self._running = True
        asyncio.create_task(self._drain_queue())
        log.info("AlertManager started")

    async def stop(self) -> None:
        self._running = False
        if self._session:
            await self._session.close()

    # ── Public API ────────────────────────────────────────────────────────────

    async def send(self, message: str, parse_mode: str = "HTML") -> None:
        """Queue a message for Telegram delivery. Never blocks."""
        try:
            self._queue.put_nowait({"text": message, "parse_mode": parse_mode})
        except asyncio.QueueFull:
            log.warning("Alert queue full — dropping message: %s", message[:80])
        log.info("ALERT: %s", message[:200])

    async def send_trade_opened(self, bot_id: str, inst_id: str, direction: str,
                                  entry_price: float, size: float,
                                  tp: float = None, sl: float = None) -> None:
        mode = "📝 PAPER" if not __import__("config").IS_LIVE else "🟢 LIVE"
        msg = (
            f"{mode} TRADE OPENED\n"
            f"<b>Bot:</b> {bot_id}\n"
            f"<b>Asset:</b> {inst_id}\n"
            f"<b>Direction:</b> {direction}\n"
            f"<b>Entry:</b> {entry_price:.4f}\n"
            f"<b>Size:</b> {size:.4f}\n"
        )
        if tp:
            msg += f"<b>TP:</b> {tp:.4f}\n"
        if sl:
            msg += f"<b>SL:</b> {sl:.4f}\n"
        await self.send(msg)

    async def send_trade_closed(self, bot_id: str, inst_id: str,
                                  pnl: float, reason: str,
                                  entry: float, exit_price: float) -> None:
        emoji = "🟢" if pnl >= 0 else "🔴"
        msg = (
            f"{emoji} TRADE CLOSED\n"
            f"<b>Bot:</b> {bot_id}\n"
            f"<b>Asset:</b> {inst_id}\n"
            f"<b>P&L:</b> {pnl:+.2f} USDT\n"
            f"<b>Entry:</b> {entry:.4f} → <b>Exit:</b> {exit_price:.4f}\n"
            f"<b>Reason:</b> {reason}\n"
        )
        await self.send(msg)

    async def send_halt(self, reason: str) -> None:
        await self.send(
            f"🚨 <b>SYSTEM HALTED</b>\n<b>Reason:</b> {reason}\n"
            f"<b>Time:</b> {datetime.now(timezone.utc).isoformat()}"
        )

    async def send_daily_summary(self, balance: float, daily_pnl: float,
                                   open_positions: int, trades_today: int) -> None:
        pct = (daily_pnl / (balance - daily_pnl) * 100) if balance else 0
        emoji = "📈" if daily_pnl >= 0 else "📉"
        await self.send(
            f"{emoji} <b>DAILY SUMMARY</b>\n"
            f"<b>Balance:</b> {balance:.2f} USDT\n"
            f"<b>Daily P&L:</b> {daily_pnl:+.2f} USDT ({pct:+.2f}%)\n"
            f"<b>Open positions:</b> {open_positions}\n"
            f"<b>Trades today:</b> {trades_today}\n"
        )

    async def send_error(self, bot_id: str, error: str) -> None:
        await self.send(f"⚠️ <b>ERROR</b> [{bot_id}]\n{error[:500]}")

    # ── Queue drain loop ──────────────────────────────────────────────────────

    async def _drain_queue(self) -> None:
        while self._running or not self._queue.empty():
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._send_telegram(item["text"], item.get("parse_mode", "HTML"))
                self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.warning("Alert drain error: %s", e)

    async def _send_telegram(self, text: str, parse_mode: str = "HTML") -> None:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return   # Telegram not configured — logged above
        try:
            async with self._session.post(
                f"/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id":    TELEGRAM_CHAT_ID,
                    "text":       text[:4096],
                    "parse_mode": parse_mode,
                },
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.warning("Telegram error %d: %s", resp.status, body[:200])
        except Exception as e:
            log.warning("Telegram send failed: %s", e)


# ── Singleton ─────────────────────────────────────────────────────────────────

_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager

# ── Singleton accessor ────────────────────────────────────────────────────────

_client_instance = None

async def get_client():
    global _client_instance
    if _client_instance is None:
        _client_instance = BloFinClient()
    return _client_instance
