"""
bot_base.py — Abstract base class for all 7 trading bots.
Handles: lifecycle (start/stop/pause), heartbeat, candle loading,
         state persistence, cooldown tracking, and error circuit breaker.
"""

import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Any

from config import SYSTEM
from database import (
    upsert_bot_state, heartbeat as db_heartbeat,
    get_candles, upsert_candles, log_signal,
)
from blofin_client import get_client
from signal_engine import parse_candles
from alert_manager import get_alert_manager

log = logging.getLogger("bot_base")


class BotBase(ABC):
    """
    Every bot inherits from this class.
    Subclasses implement:
      - strategy_tick(): called on each candle/tick — return trade intent or None
      - on_position_update(): called when position price changes
    """

    # Subclasses set these
    BOT_ID:   str = "bot_base"
    BOT_NAME: str = "Base Bot"
    ASSETS:   List[str] = []
    TIMEFRAME: str = "1H"
    LOOP_INTERVAL_SECONDS: int = 60

    def __init__(self, config: dict):
        self.config        = config
        self.bot_id        = config["id"]
        self.bot_name      = config["name"]
        self.assets        = config.get("assets", [])   # Bot7 has no fixed assets
        self.timeframe     = config.get("timeframe_signal", "1H")
        self._running      = False
        self._paused       = False
        self._error_count  = 0
        self._error_limit  = 10        # pause bot after 10 consecutive errors
        self._cooldowns:   Dict[str, datetime] = {}   # inst_id → resume_time
        self._last_trades: Dict[str, int] = {}        # inst_id → trade count this hour
        self._hour_reset:  Optional[datetime] = None
        self._candle_cache: Dict[str, list] = {}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        log.info("[%s] Starting...", self.bot_id)
        self._running = True
        self._hour_reset = datetime.now(timezone.utc).replace(
            minute=0, second=0, microsecond=0
        ) + timedelta(hours=1)
        await upsert_bot_state(self.bot_id, "RUNNING")
        await self._run_loop()

    async def stop(self) -> None:
        log.info("[%s] Stopping...", self.bot_id)
        self._running = False
        await upsert_bot_state(self.bot_id, "STOPPED")

    async def pause(self, reason: str = "") -> None:
        log.warning("[%s] Paused: %s", self.bot_id, reason)
        self._paused = True
        await upsert_bot_state(self.bot_id, "PAUSED", {"reason": reason})

    async def resume(self) -> None:
        log.info("[%s] Resumed", self.bot_id)
        self._paused = False
        await upsert_bot_state(self.bot_id, "RUNNING")

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _run_loop(self) -> None:
        alert = get_alert_manager()
        while self._running:
            try:
                # Heartbeat
                await db_heartbeat(self.bot_id)

                # Reset hourly trade counts
                now = datetime.now(timezone.utc)
                if now >= self._hour_reset:
                    self._last_trades = {}
                    self._hour_reset = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

                if not self._paused:
                    for asset in self.assets:
                        if not self._is_in_cooldown(asset):
                            await self._process_asset(asset)
                            await asyncio.sleep(0.2)   # small gap between assets

                self._error_count = 0   # reset on successful iteration

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._error_count += 1
                log.error("[%s] Loop error (%d/%d): %s",
                           self.bot_id, self._error_count, self._error_limit, e,
                           exc_info=True)
                if self._error_count >= self._error_limit:
                    await self.pause(f"error_limit_reached: {e}")
                    await alert.send_error(
                        self.bot_id,
                        f"Bot paused after {self._error_limit} consecutive errors: {e}"
                    )

            await asyncio.sleep(self.LOOP_INTERVAL_SECONDS)

    async def _process_asset(self, inst_id: str) -> None:
        """Fetch candles, run strategy, handle output."""
        candles = await self._fetch_candles(inst_id, self.timeframe)
        if not candles or len(candles["closes"]) < 50:
            return
        await self.strategy_tick(inst_id, candles)

    # ── Abstract methods ──────────────────────────────────────────────────────

    @abstractmethod
    async def strategy_tick(self, inst_id: str, candles: dict) -> None:
        """
        Called every loop with fresh candle data.
        Bots implement their signal logic here.
        To place a trade, call self._request_trade(...).
        """
        ...

    # ── Trade request helper ──────────────────────────────────────────────────

    async def _request_trade(
        self,
        inst_id:       str,
        direction:     str,
        entry_price:   float,
        sl_price:      float,
        tp_price:      Optional[float] = None,
        trailing_sl:   bool = False,
        trailing_atr_mult: float = 2.0,
        win_prob:      float = 0.55,
        reward_risk:   float = 2.0,
        metadata:      dict = None,
    ) -> Optional[str]:
        """
        Runs risk check, sizes position, opens trade.
        Returns position_id if approved, None if blocked.
        """
        from risk_engine import get_risk_engine, TradeRequest
        from position_tracker import get_position_manager
        from blofin_client import get_client

        client = await get_client()
        balance = await client.get_usdt_balance()

        req = TradeRequest(
            bot_id         = self.bot_id,
            inst_id        = inst_id,
            direction      = direction,
            entry_price    = entry_price,
            stop_loss_price= sl_price,
            account_balance= balance,
            win_probability= win_prob,
            metadata       = {"reward_risk_ratio": reward_risk, **(metadata or {})},
        )

        engine   = get_risk_engine()
        decision = await engine.approve(req)

        if not decision:
            log.debug("[%s] Trade blocked: %s", self.bot_id, decision.reason)
            await log_signal(
                self.bot_id, inst_id, "BLOCKED", direction,
                entry_price, passed_risk=False,
                metadata={"reason": decision.reason},
            )
            return None

        # Generate human-readable commentary for the dashboard
        from commentary import generate_entry_commentary
        commentary = generate_entry_commentary(
            bot_id      = self.bot_id,
            inst_id     = inst_id,
            direction   = direction,
            entry_price = entry_price,
            sl_price    = sl_price,
            tp_price    = tp_price,
            kelly_frac  = decision.kelly_fraction_used,
            size_usdt   = decision.position_size_usdt,
            metadata    = metadata or {},
        )
        # Merge commentary into metadata so it's stored on the position
        full_metadata = {**(metadata or {}), "commentary": commentary}

        await log_signal(
            self.bot_id, inst_id, "APPROVED", direction,
            entry_price, passed_risk=True,
            metadata={"size_usdt": decision.position_size_usdt,
                       "kelly": decision.kelly_fraction_used,
                       "commentary": commentary},
        )

        pm = get_position_manager()
        pos_id = await pm.open_position(
            bot_id         = self.bot_id,
            inst_id        = inst_id,
            direction      = direction,
            size_usdt      = decision.position_size_usdt,
            entry_price    = entry_price,
            tp_price       = tp_price,
            sl_price       = sl_price,
            trailing_sl    = trailing_sl,
            trailing_atr_mult = trailing_atr_mult,
            metadata       = full_metadata,
        )

        if pos_id:
            self._register_trade(inst_id)

        return pos_id

    # ── Cooldown management ───────────────────────────────────────────────────

    def _set_cooldown(self, inst_id: str, seconds: int) -> None:
        self._cooldowns[inst_id] = datetime.now(timezone.utc) + timedelta(seconds=seconds)

    def _is_in_cooldown(self, inst_id: str) -> bool:
        if inst_id not in self._cooldowns:
            return False
        return datetime.now(timezone.utc) < self._cooldowns[inst_id]

    # ── Trade rate limiting ───────────────────────────────────────────────────

    def _register_trade(self, inst_id: str) -> None:
        self._last_trades[inst_id] = self._last_trades.get(inst_id, 0) + 1

    def _trades_this_hour(self, inst_id: str) -> int:
        return self._last_trades.get(inst_id, 0)

    def _can_trade(self, inst_id: str, max_per_hour: int) -> bool:
        return self._trades_this_hour(inst_id) < max_per_hour

    # ── Candle loading ────────────────────────────────────────────────────────

    async def _fetch_candles(self, inst_id: str, timeframe: str,
                               limit: int = 200) -> Optional[dict]:
        """Fetch candles from exchange and cache in DB."""
        try:
            client = await get_client()
            raw = await client.get_candles(inst_id, timeframe, limit)
            if not raw:
                return None

            # Convert and cache
            candle_rows = []
            for row in raw:
                from datetime import datetime
                ts = datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc)
                candle_rows.append({
                    "inst_id":   inst_id,
                    "timeframe": timeframe,
                    "open_time": ts,
                    "open":      float(row[1]),
                    "high":      float(row[2]),
                    "low":       float(row[3]),
                    "close":     float(row[4]),
                    "volume":    float(row[5]),
                })
            await upsert_candles(candle_rows)
            return parse_candles(raw)

        except Exception as e:
            log.warning("[%s] Failed to fetch candles %s %s: %s",
                         self.bot_id, inst_id, timeframe, e)
            # Fallback to DB cache
            db_candles = await get_candles(inst_id, timeframe, limit)
            if db_candles:
                return {
                    "timestamps": [int(c["open_time"].timestamp() * 1000) for c in db_candles],
                    "opens":      [float(c["open"])   for c in db_candles],
                    "highs":      [float(c["high"])   for c in db_candles],
                    "lows":       [float(c["low"])    for c in db_candles],
                    "closes":     [float(c["close"])  for c in db_candles],
                    "volumes":    [float(c["volume"]) for c in db_candles],
                }
            return None

    async def _fetch_candles_multi(self, inst_id: str, timeframes: List[str],
                                    limit: int = 200) -> Dict[str, Optional[dict]]:
        """Fetch multiple timeframes concurrently."""
        tasks = {tf: self._fetch_candles(inst_id, tf, limit) for tf in timeframes}
        results = {}
        for tf, coro in tasks.items():
            results[tf] = await coro
        return results
