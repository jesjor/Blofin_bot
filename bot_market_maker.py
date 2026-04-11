"""
bots/bot_funding_arb.py — Bot 6: Funding Rate Arbitrage
Priority 2 — lowest risk, deploy first to generate passive yield.

Logic:
  - Every 8h funding cycle, check BloFin perp funding rates
  - If rate > +0.05%: shorts are being paid → open short perp
  - If rate < -0.03%: longs are being paid → open long perp
  - Hold for minimum 3 funding payments
  - Exit when rate normalises (falls below 0.01% or 2 consecutive down readings)
  - Guard: if unrealized loss > 2× collected funding → emergency exit
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional

from bot_base import BotBase
from config import BOT6
from database import (
    get_open_positions, update_position, insert_funding_rate,
    get_latest_funding_rate, log_risk_event,
)
from blofin_client import get_client
from position_tracker import get_position_manager
from alert_manager import get_alert_manager

log = logging.getLogger("bot6_funding")


class FundingArbBot(BotBase):
    BOT_ID   = "bot6_funding"
    BOT_NAME = "Funding Rate Arb"
    TIMEFRAME = "1H"
    LOOP_INTERVAL_SECONDS = 300   # check every 5 minutes (not per-candle)

    def __init__(self):
        super().__init__(BOT6)
        self._funding_history:   Dict[str, list] = {}   # inst_id → [rate, rate, ...]
        self._collected_funding: Dict[str, float] = {}  # position_id → cumulative funding
        self._payment_counts:    Dict[str, int]   = {}  # position_id → payments received

    # ── Main strategy tick ────────────────────────────────────────────────────

    async def strategy_tick(self, inst_id: str, candles: dict) -> None:
        """Check funding rates and manage positions for one asset."""
        try:
            client = await get_client()
            pm     = get_position_manager()

            # Fetch current funding rate
            funding_data = await client.get_funding_rate(inst_id)
            if not funding_data:
                return

            rate     = float(funding_data.get("fundingRate", 0))
            next_time_str = funding_data.get("nextFundingTime", "")

            await insert_funding_rate(inst_id, rate)

            # Track history for consecutive-decline logic
            hist = self._funding_history.setdefault(inst_id, [])
            hist.append(rate)
            if len(hist) > 10:
                hist.pop(0)

            log.debug("[%s] Funding rate: %.6f%%", inst_id, rate * 100)

            # Check existing position for this asset
            open_positions = await get_open_positions(bot_id=self.bot_id)
            existing = next(
                (p for p in open_positions if p["inst_id"] == inst_id), None
            )

            if existing:
                await self._manage_existing_position(existing, rate, candles)
            else:
                await self._check_entry(inst_id, rate, client)

        except Exception as e:
            log.error("[%s] Strategy tick error for %s: %s", self.bot_id, inst_id, e)

    # ── Entry logic ───────────────────────────────────────────────────────────

    async def _check_entry(self, inst_id: str, rate: float, client) -> None:
        entry_threshold = self.config["funding_entry_threshold"] / 100
        negative_threshold = self.config["funding_negative_threshold"] / 100

        # Determine trade direction
        direction = None
        if rate > entry_threshold:
            direction = "SHORT"    # longs paying → short perp to collect
        elif rate < negative_threshold:
            direction = "LONG"     # shorts paying → long perp to collect

        if not direction:
            return

        # Check basis (spot-perp spread) before entering
        if not await self._basis_ok(inst_id, client):
            log.info("[%s] Basis too wide for %s — skipping entry", self.bot_id, inst_id)
            return

        # Get current price for sizing
        price = await client.get_mid_price(inst_id)
        if not price:
            return

        balance = await client.get_usdt_balance()
        size_usdt = balance * (self.config["max_account_pct"])

        # No directional SL for pure funding arb — guard is the loss vs collected check
        # We set a wide emergency SL at 5% for absolute protection
        sl_pct = 0.05
        sl_price = (price * (1 - sl_pct) if direction == "LONG"
                    else price * (1 + sl_pct))

        log.info("[%s] FUNDING ARB ENTRY: %s %s rate=%.6f%%",
                  self.bot_id, inst_id, direction, rate * 100)

        pos_id = await self._request_trade(
            inst_id     = inst_id,
            direction   = direction,
            entry_price = price,
            sl_price    = sl_price,
            win_prob    = 0.70,    # funding arb has high win rate
            reward_risk = 3.0,
            metadata    = {
                "entry_funding_rate": rate,
                "strategy": "funding_arb",
            },
        )

        if pos_id:
            self._collected_funding[pos_id] = 0.0
            self._payment_counts[pos_id] = 0
            await get_alert_manager().send(
                f"💰 <b>Funding Arb Entry</b>\n"
                f"Asset: {inst_id}\n"
                f"Direction: {direction}\n"
                f"Rate: {rate*100:.4f}% per 8h\n"
                f"Annualised: {rate*3*365*100:.1f}%"
            )

    # ── Manage existing position ──────────────────────────────────────────────

    async def _manage_existing_position(self, pos: dict, rate: float, candles: dict) -> None:
        position_id = pos["position_id"]
        side = pos["side"]
        pm   = get_position_manager()

        # Accumulate funding collected estimate
        funding_collected = self._collected_funding.get(position_id, 0)
        size_notional = float(pos["size"]) * float(pos["entry_price"])
        payment = abs(rate) * size_notional
        self._collected_funding[position_id] = funding_collected + payment
        self._payment_counts[position_id] = self._payment_counts.get(position_id, 0) + 1

        log.debug("[%s] Position %s | payments=%d | collected=%.4f USDT",
                   self.bot_id, position_id,
                   self._payment_counts.get(position_id, 0),
                   self._collected_funding.get(position_id, 0))

        # Guard: loss vs collected funding
        client = await get_client()
        current_price = await client.get_mid_price(pos["inst_id"])
        entry = float(pos["entry_price"])
        size  = float(pos["size"])
        if side == "LONG":
            unrealized = (current_price - entry) * size
        else:
            unrealized = (entry - current_price) * size

        loss_ratio = self.config["loss_vs_collected_max"]
        if unrealized < 0 and abs(unrealized) > loss_ratio * funding_collected:
            log.warning("[%s] Emergency exit: loss %.2f > %.0f× collected %.2f",
                         self.bot_id, abs(unrealized), loss_ratio, funding_collected)
            await pm.close_position(position_id, "loss_exceeds_collected_funding",
                                     exit_price=current_price)
            await get_alert_manager().send(
                f"⚠️ Funding arb emergency exit {pos['inst_id']}: "
                f"unrealized loss {unrealized:.2f} > {loss_ratio}× collected"
            )
            return

        # Check exit conditions (after minimum payments)
        min_payments = self.config["min_funding_payments"]
        if self._payment_counts.get(position_id, 0) < min_payments:
            return

        exit_threshold = self.config["funding_exit_threshold"] / 100
        exit_consecutive = self.config["funding_exit_consecutive"]
        hist = self._funding_history.get(pos["inst_id"], [])

        should_exit = False
        if abs(rate) < exit_threshold:
            # Rate has normalised
            should_exit = True
            reason = "funding_rate_normalised"
        elif len(hist) >= exit_consecutive:
            # Check if rate has been declining for N consecutive periods
            declining = all(hist[i] < hist[i - 1] for i in range(-exit_consecutive + 1, 0))
            if declining and abs(rate) < abs(hist[-exit_consecutive]) * 0.5:
                should_exit = True
                reason = "funding_rate_declining"

        if should_exit:
            log.info("[%s] Exiting funding arb %s: collected=%.4f USDT",
                      self.bot_id, position_id,
                      self._collected_funding.get(position_id, 0))
            await pm.close_position(position_id, reason, exit_price=current_price)
            await get_alert_manager().send(
                f"✅ Funding arb closed {pos['inst_id']}\n"
                f"Collected: {self._collected_funding.get(position_id, 0):.4f} USDT\n"
                f"Payments: {self._payment_counts.get(position_id, 0)}"
            )
            # Cleanup
            self._collected_funding.pop(position_id, None)
            self._payment_counts.pop(position_id, None)

    # ── Basis check ───────────────────────────────────────────────────────────

    async def _basis_ok(self, inst_id: str, client) -> bool:
        """Spot-perp basis must be < 0.20% to ensure clean entry/exit."""
        try:
            perp_price  = await client.get_mid_price(inst_id)
            mark_price  = await client.get_mark_price(inst_id)
            if not perp_price or not mark_price:
                return True   # can't check, allow
            basis_pct = abs(perp_price - mark_price) / mark_price * 100
            ok = basis_pct <= self.config["basis_max_pct"]
            if not ok:
                log.debug("[%s] Basis too wide: %.4f%%", self.bot_id, basis_pct)
            return ok
        except Exception:
            return True
