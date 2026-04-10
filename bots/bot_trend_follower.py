"""
bots/bot_trend_follower.py — Bot 2: Trend Follower
Priority 3 — simplest directional bot, deploy second.

Logic:
  - 4H timeframe signal, 1D trend confirmation
  - Entry: 200 EMA slope positive + fresh 50/200 EMA cross + ADX > 25 + RSI 45–65
  - Partial exit (50%) at +4% profit
  - Trailing stop: 2× ATR from highest close
  - No weekend trading in low-vol environments
"""

import asyncio
import logging
from datetime import datetime, timezone

from bot_base import BotBase
from config import BOT2
from database import get_open_positions
from blofin_client import get_client
from position_tracker import get_position_manager
from signal_engine import (
    ema_value, ema_slope, ema_crossed_above, ema_crossed_below,
    adx, rsi_value, atr_value, volume_avg,
)

log = logging.getLogger("bot2_trend")


class TrendFollowerBot(BotBase):
    BOT_ID    = "bot2_trend"
    BOT_NAME  = "Trend Follower"
    TIMEFRAME = "4H"
    LOOP_INTERVAL_SECONDS = 300    # check every 5 minutes

    def __init__(self):
        super().__init__(BOT2)
        self._partial_exits_done: set = set()

    async def strategy_tick(self, inst_id: str, candles: dict) -> None:
        try:
            # Fetch both 4H (signal) and 1D (trend) candles
            candles_4h = candles   # already fetched in _process_asset
            candles_1d = await self._fetch_candles(inst_id, "1D", limit=250)
            if not candles_4h or not candles_1d:
                return

            c4 = candles_4h
            c1 = candles_1d

            closes_4h = c4["closes"]
            highs_4h  = c4["highs"]
            lows_4h   = c4["lows"]
            closes_1d = c1["closes"]

            # ── Trend filters (1D) ────────────────────────────────────────────
            ema_200_slope = ema_slope(closes_1d, 200, lookback=3)
            is_uptrend    = ema_200_slope > 0
            is_downtrend  = ema_200_slope < 0

            # ── Signal indicators (4H) ────────────────────────────────────────
            adx_val     = adx(highs_4h, lows_4h, closes_4h, self.config["adx_period"])
            rsi_val     = rsi_value(closes_4h, self.config["rsi_period"])
            atr_val     = atr_value(highs_4h, lows_4h, closes_4h, self.config["atr_period"])
            ema_50_val  = ema_value(closes_4h, self.config["ema_fast"])
            ema_200_val = ema_value(closes_4h, self.config["ema_slow"])

            if any(v is None for v in [adx_val, rsi_val, atr_val, ema_50_val, ema_200_val]):
                return

            current_price = closes_4h[-1]

            # ── No weekend trading in low-vol (optional check) ────────────────
            if self._is_low_vol_weekend(closes_1d):
                log.debug("[%s] Weekend low-vol — skipping %s", self.bot_id, inst_id)
                return

            # ── Check for existing position ───────────────────────────────────
            open_positions = await get_open_positions(bot_id=self.bot_id)
            existing = next((p for p in open_positions if p["inst_id"] == inst_id), None)

            if existing:
                await self._manage_position(existing, current_price, highs_4h, lows_4h, closes_4h)
                return

            # ── Entry conditions ──────────────────────────────────────────────
            adx_min     = self.config["adx_min"]
            rsi_min     = self.config["rsi_min"]
            rsi_max     = self.config["rsi_max"]
            cross_bars  = self.config["ema_cross_lookback"]

            if adx_val < adx_min:
                return

            if not (rsi_min <= rsi_val <= rsi_max):
                return

            # Kelly fraction: full at ADX > 40, 60% at ADX 25-40
            kelly_adj = 1.0 if adx_val >= self.config["adx_full_kelly"] else 0.6

            # LONG entry
            if (is_uptrend and
                ema_crossed_above(closes_4h, self.config["ema_fast"], self.config["ema_slow"],
                                   within_bars=cross_bars) and
                current_price > ema_200_val):

                sl_price = current_price - (self.config["trailing_atr_mult"] * atr_val)
                tp_price = current_price + (self.config["first_target_pct"] / 100 * current_price)

                log.info("[%s] LONG signal: %s | ADX=%.1f RSI=%.1f",
                          self.bot_id, inst_id, adx_val, rsi_val)
                await self._request_trade(
                    inst_id     = inst_id,
                    direction   = "LONG",
                    entry_price = current_price,
                    sl_price    = sl_price,
                    tp_price    = tp_price,
                    trailing_sl = True,
                    trailing_atr_mult = self.config["trailing_atr_mult"],
                    win_prob    = 0.55,
                    reward_risk = self.config["first_target_pct"] / (self.config["trailing_atr_mult"] * atr_val / current_price * 100),
                    metadata    = {"adx": adx_val, "rsi": rsi_val, "kelly_adj": kelly_adj},
                )

            # SHORT entry
            elif (is_downtrend and
                  ema_crossed_below(closes_4h, self.config["ema_fast"], self.config["ema_slow"],
                                     within_bars=cross_bars) and
                  current_price < ema_200_val):

                sl_price = current_price + (self.config["trailing_atr_mult"] * atr_val)
                tp_price = current_price - (self.config["first_target_pct"] / 100 * current_price)

                log.info("[%s] SHORT signal: %s | ADX=%.1f RSI=%.1f",
                          self.bot_id, inst_id, adx_val, rsi_val)
                await self._request_trade(
                    inst_id     = inst_id,
                    direction   = "SHORT",
                    entry_price = current_price,
                    sl_price    = sl_price,
                    tp_price    = tp_price,
                    trailing_sl = True,
                    trailing_atr_mult = self.config["trailing_atr_mult"],
                    win_prob    = 0.55,
                    reward_risk = 2.0,
                    metadata    = {"adx": adx_val, "rsi": rsi_val},
                )

        except Exception as e:
            log.error("[%s] strategy_tick error %s: %s", self.bot_id, inst_id, e)

    # ── Position management ───────────────────────────────────────────────────

    async def _manage_position(self, pos: dict, current_price: float,
                                highs: list, lows: list, closes: list) -> None:
        position_id = pos["position_id"]
        entry       = float(pos["entry_price"])
        side        = pos["side"]
        pm          = get_position_manager()

        # Partial exit at first target
        if position_id not in self._partial_exits_done:
            target_pct  = self.config["partial_exit_pct"] / 100
            first_target = self.config["first_target_pct"] / 100
            if side == "LONG"  and current_price >= entry * (1 + first_target):
                await pm.partial_close(position_id, 0.5, "first_target_long")
                self._partial_exits_done.add(position_id)
            elif side == "SHORT" and current_price <= entry * (1 - first_target):
                await pm.partial_close(position_id, 0.5, "first_target_short")
                self._partial_exits_done.add(position_id)

        # Update trailing stop
        atr_val = atr_value(highs, lows, closes, self.config["atr_period"])
        if atr_val:
            await pm.update_trailing_stop(
                position_id   = position_id,
                current_price = current_price,
                candle_highs  = highs,
                candle_lows   = lows,
                candle_closes = closes,
                atr_period    = self.config["atr_period"],
                atr_mult      = self.config["trailing_atr_mult"],
            )

        # Paper mode: check TP/SL manually
        from config import IS_PAPER
        if IS_PAPER:
            hit = await pm.check_sl_tp_paper(position_id, current_price)
            if hit:
                await pm.close_position(position_id, f"paper_{hit}",
                                         exit_price=current_price)
                self._partial_exits_done.discard(position_id)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _is_low_vol_weekend(self, closes_1d: list) -> bool:
        now = datetime.now(timezone.utc)
        is_weekend = now.weekday() >= 5   # Saturday or Sunday
        if not is_weekend:
            return False
        # Calculate 7-day vol vs 30-day median vol
        if len(closes_1d) < 31:
            return False
        import math
        returns_7d  = [abs(math.log(closes_1d[i] / closes_1d[i-1]))
                        for i in range(-7, 0)]
        returns_30d = [abs(math.log(closes_1d[i] / closes_1d[i-1]))
                        for i in range(-30, 0)]
        vol_7d  = sum(returns_7d)  / len(returns_7d)
        vol_30d = sorted(returns_30d)[len(returns_30d) // 2]   # median
        return vol_7d < vol_30d   # below median = low vol weekend
