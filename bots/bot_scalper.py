"""
bots/bot_scalper.py — Bot 1: Momentum Scalper
Priority 6 — needs low-latency infrastructure. Deploy after system is stable.

Logic:
  - 5M RSI cross + volume confirmation + above/below 20 EMA
  - 0.5% TP, 0.25% SL, time exit at 8 minutes
  - Max 6 trades/hour per asset, 3-min cooldown
  - Don't trade if ATR is below its 30-bar average (flat market)
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional

from bot_base import BotBase
from config import BOT1
from database import get_open_positions
from position_tracker import get_position_manager
from signal_engine import rsi_value, ema_value, atr_value, volume_ratio

log = logging.getLogger("bot1_scalper")


class ScalperBot(BotBase):
    BOT_ID    = "bot1_scalper"
    BOT_NAME  = "Momentum Scalper"
    TIMEFRAME = "5m"
    LOOP_INTERVAL_SECONDS = 30   # every 30 seconds

    def __init__(self):
        super().__init__(BOT1)
        self._entry_times: Dict[str, datetime] = {}   # position_id → entry time

    async def strategy_tick(self, inst_id: str, candles: dict) -> None:
        try:
            c = candles
            closes  = c["closes"]
            highs   = c["highs"]
            lows    = c["lows"]
            volumes = c["volumes"]

            if len(closes) < 35:
                return

            current_close = closes[-1]
            prev_close    = closes[-2]

            # Indicators
            rsi_val  = rsi_value(closes, self.config["rsi_period"])
            rsi_prev = rsi_value(closes[:-1], self.config["rsi_period"])
            ema_val  = ema_value(closes, self.config["ema_period"])
            atr_val  = atr_value(highs, lows, closes, self.config["atr_period"])
            vol_rat  = volume_ratio(volumes, self.config["volume_lookback"])

            if any(v is None for v in [rsi_val, rsi_prev, ema_val, atr_val]):
                return

            # ATR filter: don't scalp in flat markets
            atr_hist = atr_value(highs[:-1], lows[:-1], closes[:-1], self.config["atr_period"])
            atr_avg  = self._atr_average(highs, lows, closes, self.config["atr_lookback"])
            if atr_val < atr_avg * self.config["atr_min_multiplier"]:
                return

            # Rate limit
            if not self._can_trade(inst_id, self.config["max_trades_per_hour"]):
                return

            # Check spread
            client = await __import__("blofin_client").get_client()
            bid, ask = await client.get_best_bid_ask(inst_id)
            if bid and ask and bid > 0:
                spread_pct = (ask - bid) / bid * 100
                if spread_pct > self.config["spread_max_pct"]:
                    return

            # Check existing position
            my_positions = await get_open_positions(bot_id=self.bot_id)
            existing = next((p for p in my_positions if p["inst_id"] == inst_id), None)
            if existing:
                await self._manage_scalp(existing, current_close)
                return

            # LONG: RSI crossed above 55 + price above 20 EMA + vol spike
            long_signal = (
                rsi_prev < self.config["rsi_long_threshold"] and
                rsi_val >= self.config["rsi_long_threshold"] and
                current_close > ema_val and
                vol_rat >= self.config["volume_multiplier"]
            )
            # SHORT: RSI crossed below 45 + price below 20 EMA + vol spike
            short_signal = (
                rsi_prev > self.config["rsi_short_threshold"] and
                rsi_val <= self.config["rsi_short_threshold"] and
                current_close < ema_val and
                vol_rat >= self.config["volume_multiplier"]
            )

            if long_signal:
                tp_price = current_close * (1 + self.config["tp_pct"] / 100)
                sl_price = current_close * (1 - self.config["sl_pct"] / 100)
                log.info("[%s] LONG scalp: %s RSI=%.1f vol=%.1f×",
                          self.bot_id, inst_id, rsi_val, vol_rat)
                pos_id = await self._request_trade(
                    inst_id     = inst_id,
                    direction   = "LONG",
                    entry_price = current_close,
                    sl_price    = sl_price,
                    tp_price    = tp_price,
                    win_prob    = 0.52,
                    reward_risk = 2.0,
                    metadata    = {"rsi": rsi_val, "vol_ratio": vol_rat},
                )
                if pos_id:
                    self._entry_times[pos_id] = datetime.now(timezone.utc)
                    self._set_cooldown(inst_id, self.config["cooldown_seconds"])

            elif short_signal:
                tp_price = current_close * (1 - self.config["tp_pct"] / 100)
                sl_price = current_close * (1 + self.config["sl_pct"] / 100)
                log.info("[%s] SHORT scalp: %s RSI=%.1f vol=%.1f×",
                          self.bot_id, inst_id, rsi_val, vol_rat)
                pos_id = await self._request_trade(
                    inst_id     = inst_id,
                    direction   = "SHORT",
                    entry_price = current_close,
                    sl_price    = sl_price,
                    tp_price    = tp_price,
                    win_prob    = 0.52,
                    reward_risk = 2.0,
                    metadata    = {"rsi": rsi_val, "vol_ratio": vol_rat},
                )
                if pos_id:
                    self._entry_times[pos_id] = datetime.now(timezone.utc)
                    self._set_cooldown(inst_id, self.config["cooldown_seconds"])

        except Exception as e:
            log.error("[%s] error %s: %s", self.bot_id, inst_id, e)

    async def _manage_scalp(self, pos: dict, current_price: float) -> None:
        position_id = pos["position_id"]
        pm = get_position_manager()

        # Time exit: close if not TP'd within time_exit_minutes
        entry_time = self._entry_times.get(position_id)
        if entry_time:
            age_minutes = (datetime.now(timezone.utc) - entry_time).total_seconds() / 60
            if age_minutes >= self.config["time_exit_minutes"]:
                log.info("[%s] Time exit: %s (age=%.1f min)",
                          self.bot_id, position_id, age_minutes)
                await pm.close_position(position_id, "time_exit", current_price)
                self._entry_times.pop(position_id, None)
                return

        from config import IS_PAPER
        if IS_PAPER:
            hit = await pm.check_sl_tp_paper(position_id, current_price)
            if hit:
                await pm.close_position(position_id, f"paper_{hit}", current_price)
                self._entry_times.pop(position_id, None)

    def _atr_average(self, highs, lows, closes, lookback) -> float:
        from signal_engine import atr
        vals = atr(highs, lows, closes, 14)
        if len(vals) < lookback:
            return 0.0
        return sum(vals[-lookback:]) / lookback
