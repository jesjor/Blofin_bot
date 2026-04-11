"""
bots/bot_breakout.py — Bot 5: Breakout Hunter
Priority 4 — high-conviction setups with volume confirmation.

Logic:
  1. Detect consolidation (range < 2.5% over 20 bars on 1H)
  2. Wait for candle CLOSE outside box with volume > 2× average
  3. Optional: wait for retest (within 3 bars) for better entry + add 50%
  4. TP = 1.5× box height from breakout level
  5. SL = back inside the box (invalidation)
  6. Max 5 active boxes monitored per asset
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from bot_base import BotBase
from config import BOT5
from database import get_open_positions
from blofin_client import get_client
from position_tracker import get_position_manager
from signal_engine import (
    detect_consolidation, volume_ratio, atr_value,
)

log = logging.getLogger("bot5_breakout")


@dataclass
class BreakoutBox:
    """Tracks a detected consolidation box."""
    inst_id:          str
    box_high:         float
    box_low:          float
    box_height:       float
    direction:        Optional[str] = None    # "LONG" | "SHORT" — set after breakout
    breakout_price:   Optional[float] = None
    breakout_bar_idx: Optional[int] = None    # which bar index the breakout occurred on
    bars_since_breakout: int = 0
    retest_confirmed: bool = False
    position_id:      Optional[str] = None
    created_at:       datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class BreakoutBot(BotBase):
    BOT_ID    = "bot5_breakout"
    BOT_NAME  = "Breakout Hunter"
    TIMEFRAME = "1H"
    LOOP_INTERVAL_SECONDS = 120   # every 2 minutes

    def __init__(self):
        super().__init__(BOT5)
        self._boxes: Dict[str, List[BreakoutBox]] = {}   # inst_id → list of boxes

    async def strategy_tick(self, inst_id: str, candles: dict) -> None:
        try:
            c = candles
            closes  = c["closes"]
            highs   = c["highs"]
            lows    = c["lows"]
            volumes = c["volumes"]

            if len(closes) < self.config["consolidation_bars"] + 5:
                return

            current_close  = closes[-1]
            current_high   = highs[-1]
            current_low    = lows[-1]
            current_volume = volumes[-1]
            current_bar    = len(closes)

            boxes = self._boxes.setdefault(inst_id, [])

            # ── Phase 1: Detect new consolidation boxes ───────────────────────
            if len(boxes) < self.config["max_boxes_monitored"]:
                is_con, box_high, box_low, range_pct = detect_consolidation(
                    highs[:-1],   # exclude current bar from box detection
                    lows[:-1],
                    period     = self.config["consolidation_bars"],
                    max_range_pct = self.config["consolidation_range_pct"],
                )
                if is_con and box_high > 0:
                    # Check we don't already have a box at this level
                    duplicate = any(
                        abs(b.box_high - box_high) / box_high < 0.001
                        for b in boxes
                    )
                    if not duplicate:
                        new_box = BreakoutBox(
                            inst_id    = inst_id,
                            box_high   = box_high,
                            box_low    = box_low,
                            box_height = box_high - box_low,
                        )
                        boxes.append(new_box)
                        log.debug("[%s] New box: %s high=%.4f low=%.4f range=%.2f%%",
                                   self.bot_id, inst_id, box_high, box_low, range_pct)

            # ── Phase 2: Check each box for breakout ──────────────────────────
            for box in list(boxes):
                if box.position_id:
                    # Already in a position from this box — manage it
                    await self._manage_position(box, current_close, highs, lows, closes)
                    continue

                if box.direction:
                    # Breakout already detected — looking for retest
                    box.bars_since_breakout += 1
                    if box.bars_since_breakout > self.config["retest_bars_max"]:
                        # Retest window expired without retest → enter at market anyway
                        if not box.position_id:
                            log.info("[%s] No retest window expired — entering %s at market",
                                      self.bot_id, inst_id)
                            await self._enter_trade(box, inst_id, current_close,
                                                     add_position=False)
                        continue

                    # Check for retest
                    if box.direction == "LONG":
                        # Retest: price pulls back to box high and holds
                        if current_low <= box.box_high * 1.002 and current_close > box.box_high:
                            log.info("[%s] RETEST CONFIRMED (LONG): %s @ %.4f",
                                      self.bot_id, inst_id, current_close)
                            box.retest_confirmed = True
                            await self._enter_trade(box, inst_id, current_close,
                                                     add_position=True)
                    else:
                        # Short retest: price pulls back to box low and holds
                        if current_high >= box.box_low * 0.998 and current_close < box.box_low:
                            log.info("[%s] RETEST CONFIRMED (SHORT): %s @ %.4f",
                                      self.bot_id, inst_id, current_close)
                            box.retest_confirmed = True
                            await self._enter_trade(box, inst_id, current_close,
                                                     add_position=True)
                    continue

                # ── Check for fresh breakout ──────────────────────────────────
                vol_ratio = self._vol_ratio(volumes)
                if vol_ratio < self.config["volume_multiplier"]:
                    continue   # volume filter: not a real breakout

                if current_close > box.box_high:
                    log.info("[%s] BREAKOUT UP: %s @ %.4f (vol ratio=%.1f×)",
                              self.bot_id, inst_id, current_close, vol_ratio)
                    box.direction        = "LONG"
                    box.breakout_price   = current_close
                    box.breakout_bar_idx = current_bar
                    box.bars_since_breakout = 0

                elif current_close < box.box_low:
                    log.info("[%s] BREAKOUT DOWN: %s @ %.4f (vol ratio=%.1f×)",
                              self.bot_id, inst_id, current_close, vol_ratio)
                    box.direction        = "SHORT"
                    box.breakout_price   = current_close
                    box.breakout_bar_idx = current_bar
                    box.bars_since_breakout = 0

            # Prune old boxes (no activity for > 48 bars)
            self._boxes[inst_id] = [
                b for b in boxes
                if not b.direction or b.bars_since_breakout <= 48 or b.position_id
            ]

        except Exception as e:
            log.error("[%s] strategy_tick error %s: %s", self.bot_id, inst_id, e, exc_info=True)

    # ── Enter trade from breakout ─────────────────────────────────────────────

    async def _enter_trade(self, box: BreakoutBox, inst_id: str,
                            current_price: float, add_position: bool = False) -> None:
        direction  = box.direction
        box_height = box.box_height

        # TP = 1.5× box height from the breakout level
        tp_mult = self.config["tp_multiplier"]
        if direction == "LONG":
            tp_price = box.box_high + (box_height * tp_mult)
            sl_price = box.box_low   # back inside box = invalidation
        else:
            tp_price = box.box_low  - (box_height * tp_mult)
            sl_price = box.box_high  # back inside box

        reward = abs(tp_price - current_price)
        risk   = abs(current_price - sl_price)
        rr     = reward / risk if risk > 0 else 0

        log.info("[%s] ENTERING: %s %s entry=%.4f tp=%.4f sl=%.4f R:R=%.1f",
                  self.bot_id, direction, inst_id, current_price, tp_price, sl_price, rr)

        pos_id = await self._request_trade(
            inst_id     = inst_id,
            direction   = direction,
            entry_price = current_price,
            sl_price    = sl_price,
            tp_price    = tp_price,
            win_prob    = 0.52,
            reward_risk = rr,
            metadata    = {
                "box_high":    box.box_high,
                "box_low":     box.box_low,
                "box_height":  box_height,
                "retest":      box.retest_confirmed,
                "strategy":    "breakout",
            },
        )

        if not pos_id:
            return

        box.position_id = pos_id

        # Add 50% to position on confirmed retest
        if add_position and box.retest_confirmed:
            await asyncio.sleep(1)
            log.info("[%s] Adding 50%% on retest: %s", self.bot_id, inst_id)
            # The extra 50% goes through risk check too
            extra_pos_id = await self._request_trade(
                inst_id     = inst_id,
                direction   = direction,
                entry_price = current_price,
                sl_price    = sl_price,
                tp_price    = tp_price,
                win_prob    = 0.58,   # retest = higher confidence
                reward_risk = rr,
                metadata    = {
                    "box_high":  box.box_high,
                    "retest_add":True,
                    "strategy":  "breakout_add",
                },
            )
            if extra_pos_id:
                log.info("[%s] Retest add position opened: %s", self.bot_id, extra_pos_id)

    # ── Manage open position ──────────────────────────────────────────────────

    async def _manage_position(self, box: BreakoutBox, current_price: float,
                                highs: list, lows: list, closes: list) -> None:
        if not box.position_id:
            return
        pm = get_position_manager()

        # Paper mode: check TP/SL
        from config import IS_PAPER
        if IS_PAPER:
            hit = await pm.check_sl_tp_paper(box.position_id, current_price)
            if hit:
                await pm.close_position(box.position_id, f"paper_{hit}",
                                         exit_price=current_price)
                box.position_id = None
                return

        # Check if position still exists
        open_positions = await get_open_positions(bot_id=self.bot_id)
        still_open = any(p["position_id"] == box.position_id for p in open_positions)
        if not still_open:
            box.position_id = None

    # ── Volume helpers ────────────────────────────────────────────────────────

    def _vol_ratio(self, volumes: list) -> float:
        lookback = self.config["volume_lookback"]
        if len(volumes) < lookback + 1:
            return 0.0
        avg = sum(volumes[-lookback - 1:-1]) / lookback
        return volumes[-1] / avg if avg else 0.0
