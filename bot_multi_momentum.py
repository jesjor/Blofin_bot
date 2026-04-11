"""
bots/bot_mean_reversion.py — Bot 3: Mean Reversion
Priority 5 — only runs in ranging (non-trending) markets.
"""

import asyncio
import logging
from bot_base import BotBase
from config import BOT3
from database import get_open_positions
from position_tracker import get_position_manager
from signal_engine import rsi_value, bb_values, adx, sma_value

log = logging.getLogger("bot3_meanrev")


class MeanReversionBot(BotBase):
    BOT_ID    = "bot3_meanrev"
    BOT_NAME  = "Mean Reversion"
    TIMEFRAME = "1H"
    LOOP_INTERVAL_SECONDS = 180

    def __init__(self):
        super().__init__(BOT3)

    async def strategy_tick(self, inst_id: str, candles: dict) -> None:
        try:
            c = candles
            closes = c["closes"]
            highs  = c["highs"]
            lows   = c["lows"]

            if len(closes) < 30:
                return

            # Must be in a range (ADX < 20)
            adx_val = adx(highs, lows, closes, self.config["adx_period"])
            if adx_val is None or adx_val >= self.config["adx_max"]:
                return

            rsi_val = rsi_value(closes, self.config["rsi_period"])
            bb_upper, bb_mid, bb_lower = bb_values(closes, self.config["bb_period"],
                                                     self.config["bb_std"])
            sma_val = sma_value(closes, self.config["mean_period"])

            if any(v is None for v in [rsi_val, bb_upper, bb_lower, sma_val]):
                return

            current_close = closes[-1]
            prev_close    = closes[-2]

            # Check Bot 2 — if Bot 2 has a position in this direction, skip
            from database import get_open_positions as gop
            bot2_positions = await gop(bot_id="bot2_trend")
            bot2_long  = any(p["inst_id"] == inst_id and p["side"] == "LONG"  for p in bot2_positions)
            bot2_short = any(p["inst_id"] == inst_id and p["side"] == "SHORT" for p in bot2_positions)

            # Check existing Bot 3 position
            my_positions = await get_open_positions(bot_id=self.bot_id)
            existing = next((p for p in my_positions if p["inst_id"] == inst_id), None)
            if existing:
                await self._manage_position(existing, current_close, sma_val)
                return

            # LONG: RSI oversold + price closed below lower BB + now closing back inside
            if (rsi_val < self.config["rsi_oversold"] and
                prev_close < bb_lower and
                current_close >= bb_lower and   # ← confirmation: 1 bar back inside
                not bot2_short):

                sl_pct   = self.config["sl_pct"] / 100
                sl_price = current_close * (1 - sl_pct)
                tp_price = sma_val     # TP at 50% reversion handled in manage

                log.info("[%s] LONG signal: %s RSI=%.1f ADX=%.1f", self.bot_id, inst_id, rsi_val, adx_val)
                await self._request_trade(
                    inst_id     = inst_id,
                    direction   = "LONG",
                    entry_price = current_close,
                    sl_price    = sl_price,
                    tp_price    = tp_price,
                    win_prob    = 0.58,
                    reward_risk = abs(sma_val - current_close) / abs(current_close - sl_price),
                    metadata    = {"adx": adx_val, "rsi": rsi_val, "sma": sma_val},
                )

            # SHORT: RSI overbought + price closed above upper BB + now closing back inside
            elif (rsi_val > self.config["rsi_overbought"] and
                  prev_close > bb_upper and
                  current_close <= bb_upper and
                  not bot2_long):

                sl_pct   = self.config["sl_pct"] / 100
                sl_price = current_close * (1 + sl_pct)
                tp_price = sma_val

                log.info("[%s] SHORT signal: %s RSI=%.1f ADX=%.1f", self.bot_id, inst_id, rsi_val, adx_val)
                await self._request_trade(
                    inst_id     = inst_id,
                    direction   = "SHORT",
                    entry_price = current_close,
                    sl_price    = sl_price,
                    tp_price    = tp_price,
                    win_prob    = 0.58,
                    reward_risk = abs(current_close - sma_val) / abs(sl_price - current_close),
                    metadata    = {"adx": adx_val, "rsi": rsi_val, "sma": sma_val},
                )

        except Exception as e:
            log.error("[%s] error %s: %s", self.bot_id, inst_id, e)

    async def _manage_position(self, pos: dict, current_price: float, sma_val: float) -> None:
        position_id = pos["position_id"]
        entry = float(pos["entry_price"])
        side  = pos["side"]
        pm    = get_position_manager()

        # TP at 50% reversion to mean
        reversion_pct = self.config["tp_reversion_pct"]
        if side == "LONG":
            reversion_target = entry + (sma_val - entry) * reversion_pct
            if current_price >= reversion_target:
                await pm.close_position(position_id, "mean_reversion_tp", current_price)
                return
        else:
            reversion_target = entry - (entry - sma_val) * reversion_pct
            if current_price <= reversion_target:
                await pm.close_position(position_id, "mean_reversion_tp", current_price)
                return

        from config import IS_PAPER
        if IS_PAPER:
            hit = await pm.check_sl_tp_paper(position_id, current_price)
            if hit:
                await pm.close_position(position_id, f"paper_{hit}", current_price)
