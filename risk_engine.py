"""
risk_engine.py — The Universal Risk Engine.
EVERY trade decision from every bot passes through here before any order is placed.
This module has veto power over the entire system. Nothing bypasses it.

Checks performed on every trade request:
  1. System-level kill switch (manual override)
  2. Daily drawdown limit (6%)
  3. Monthly drawdown limit (15%)
  4. Max concurrent open positions (12)
  5. Per-bot allocation cap (20% of account)
  6. Per-trade risk cap (2% of account)
  7. Correlation guard (don't double-up correlated positions)
  8. Volatility kill switch (halt if vol spikes 3× historical)
  9. News blackout window (pause 5 min pre / 10 min post macro events)
 10. Kelly sizing calculation (half-Kelly)
 11. Market maker mode: skip directional checks
"""

import asyncio
import logging
import math
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, List

import numpy as np

from config import RISK, KNOWN_MACRO_EVENTS
from database import (
    get_state, set_state, log_risk_event,
    get_open_positions, get_today_pnl, get_monthly_pnl, get_candles,
)

log = logging.getLogger("risk_engine")

# Keys used in system_state table
KEY_SYSTEM_HALTED  = "system_halted"
KEY_HALT_REASON    = "halt_reason"
KEY_DAILY_STARTING = "daily_starting_balance"


# ── Decision dataclass ────────────────────────────────────────────────────────

class TradeRequest:
    """Describes a trade a bot wants to place. Pass to RiskEngine.approve()."""
    def __init__(
        self,
        bot_id: str,
        inst_id: str,
        direction: str,          # "LONG" | "SHORT"
        entry_price: float,
        stop_loss_price: float,
        account_balance: float,
        is_reduce_only: bool = False,
        is_market_maker: bool = False,
        win_probability: float = 0.55,   # estimated win rate for Kelly
        metadata: dict = None,
    ):
        self.bot_id          = bot_id
        self.inst_id         = inst_id
        self.direction       = direction
        self.entry_price     = entry_price
        self.stop_loss_price = stop_loss_price
        self.account_balance = account_balance
        self.is_reduce_only  = is_reduce_only
        self.is_market_maker = is_market_maker
        self.win_probability = win_probability
        self.metadata        = metadata or {}

    @property
    def risk_per_unit(self) -> float:
        return abs(self.entry_price - self.stop_loss_price)

    @property
    def risk_pct(self) -> float:
        return (self.risk_per_unit / self.entry_price) * 100 if self.entry_price else 0


class RiskDecision:
    """Result returned by RiskEngine.approve()."""
    def __init__(self, approved: bool, reason: str = "",
                 position_size_usdt: float = 0.0,
                 kelly_fraction_used: float = 0.0):
        self.approved             = approved
        self.reason               = reason
        self.position_size_usdt   = position_size_usdt   # how much capital to deploy
        self.kelly_fraction_used  = kelly_fraction_used

    def __bool__(self):
        return self.approved

    def __repr__(self):
        return f"RiskDecision(approved={self.approved}, size={self.position_size_usdt:.2f}, reason={self.reason!r})"


# ── Risk Engine ───────────────────────────────────────────────────────────────

class RiskEngine:
    """
    Stateless singleton. All state is read from DB on each call so the engine
    works correctly even after a crash and restart.
    """

    def __init__(self):
        self._vol_cache: Dict[str, Tuple[float, datetime]] = {}   # inst_id → (vol, ts)
        self._corr_cache: Dict[str, Tuple[np.ndarray, datetime]] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    async def approve(self, req: TradeRequest) -> RiskDecision:
        """
        Main gate. Returns RiskDecision — approved=True only if ALL checks pass.
        Always logs the decision.
        """
        # Reduce-only orders (closing positions) bypass most checks
        if req.is_reduce_only:
            return RiskDecision(approved=True, reason="reduce_only",
                                 position_size_usdt=req.account_balance * 0.01)

        checks = [
            self._check_kill_switch,
            self._check_daily_drawdown,
            self._check_monthly_drawdown,
            self._check_max_positions,
            self._check_bot_allocation,
            self._check_trade_risk,
            self._check_volatility,
            self._check_news_blackout,
        ]

        # Market maker skips directional correlation check
        if not req.is_market_maker:
            checks.append(self._check_correlation)

        for check in checks:
            decision = await check(req)
            if not decision.approved:
                await log_risk_event(
                    "BLOCK", decision.reason,
                    bot_id=req.bot_id,
                    metadata={"inst_id": req.inst_id, "direction": req.direction},
                )
                log.warning("RISK BLOCK [%s] %s %s: %s",
                             req.bot_id, req.inst_id, req.direction, decision.reason)
                return decision

        # All checks passed — calculate Kelly size
        size_usdt, kelly_f = self._kelly_size(req)

        log.info("RISK APPROVED [%s] %s %s @ %.4f | size=%.2f USDT (Kelly=%.3f)",
                  req.bot_id, req.inst_id, req.direction,
                  req.entry_price, size_usdt, kelly_f)

        return RiskDecision(
            approved=True,
            reason="all_checks_passed",
            position_size_usdt=size_usdt,
            kelly_fraction_used=kelly_f,
        )

    # ── Manual kill switch ────────────────────────────────────────────────────

    async def halt_system(self, reason: str) -> None:
        await set_state(KEY_SYSTEM_HALTED, "1")
        await set_state(KEY_HALT_REASON, reason)
        await log_risk_event("KILL_SWITCH", reason)
        log.critical("SYSTEM HALTED: %s", reason)

    async def resume_system(self) -> None:
        await set_state(KEY_SYSTEM_HALTED, "0")
        await log_risk_event("RESUME", "manual_resume")
        log.info("SYSTEM RESUMED by operator")

    async def is_halted(self) -> bool:
        return await get_state(KEY_SYSTEM_HALTED, "0") == "1"

    async def get_halt_reason(self) -> str:
        return await get_state(KEY_HALT_REASON, "")

    # ── Individual checks ─────────────────────────────────────────────────────

    async def _check_kill_switch(self, req: TradeRequest) -> RiskDecision:
        if await self.is_halted():
            reason = await self.get_halt_reason()
            return RiskDecision(False, f"system_halted: {reason}")
        return RiskDecision(True)

    async def _check_daily_drawdown(self, req: TradeRequest) -> RiskDecision:
        pnl = await get_today_pnl()
        if not pnl:
            return RiskDecision(True)   # No data yet, allow

        starting = float(pnl.get("starting_balance", req.account_balance))
        realized = float(pnl.get("realized_pnl", 0))
        unrealized = float(pnl.get("unrealized_pnl", 0))
        total_pnl = realized + unrealized
        drawdown_pct = abs(min(0, total_pnl) / starting * 100) if starting else 0

        if drawdown_pct >= RISK["max_daily_drawdown_pct"]:
            await self.halt_system(f"daily_drawdown_{drawdown_pct:.2f}pct")
            return RiskDecision(False,
                f"daily_drawdown_exceeded: {drawdown_pct:.2f}% >= {RISK['max_daily_drawdown_pct']}%")
        return RiskDecision(True)

    async def _check_monthly_drawdown(self, req: TradeRequest) -> RiskDecision:
        monthly_pnl = await get_monthly_pnl()
        if monthly_pnl >= 0:
            return RiskDecision(True)
        drawdown_pct = abs(monthly_pnl) / req.account_balance * 100
        if drawdown_pct >= RISK["max_monthly_drawdown_pct"]:
            await self.halt_system(f"monthly_drawdown_{drawdown_pct:.2f}pct")
            return RiskDecision(False,
                f"monthly_drawdown_exceeded: {drawdown_pct:.2f}%")
        return RiskDecision(True)

    async def _check_max_positions(self, req: TradeRequest) -> RiskDecision:
        positions = await get_open_positions()
        count = len(positions)
        if count >= RISK["max_concurrent_positions"]:
            return RiskDecision(False,
                f"max_positions_reached: {count}/{RISK['max_concurrent_positions']}")
        return RiskDecision(True)

    async def _check_bot_allocation(self, req: TradeRequest) -> RiskDecision:
        positions = await get_open_positions(bot_id=req.bot_id)
        bot_exposure = sum(
            float(p.get("entry_price", 0)) * float(p.get("size", 0))
            for p in positions
        )
        max_allowed = req.account_balance * (RISK["max_bot_allocation_pct"] / 100)
        if bot_exposure >= max_allowed:
            return RiskDecision(False,
                f"bot_allocation_exceeded: {bot_exposure:.2f} >= {max_allowed:.2f}")
        return RiskDecision(True)

    async def _check_trade_risk(self, req: TradeRequest) -> RiskDecision:
        max_risk_usdt = req.account_balance * (RISK["max_trade_risk_pct"] / 100)
        # Risk in USDT = (entry - SL) / entry * Kelly_position_size
        # We check the risk% of entry price here
        if req.risk_pct > RISK["max_trade_risk_pct"] * 3:
            # If stop is very far (>6% from entry for 2% risk), warn
            return RiskDecision(False,
                f"sl_too_far: {req.risk_pct:.2f}% from entry")
        return RiskDecision(True)

    async def _check_volatility(self, req: TradeRequest) -> RiskDecision:
        """Halt if 1h realized vol > 3× 7-day average vol."""
        try:
            current_vol, hist_avg = await self._get_vol(req.inst_id)
            if hist_avg > 0 and current_vol > hist_avg * RISK["vol_kill_multiplier"]:
                await self.halt_system(
                    f"vol_spike_{req.inst_id}_{current_vol:.4f}_vs_avg_{hist_avg:.4f}"
                )
                return RiskDecision(False,
                    f"volatility_kill_switch: {current_vol:.4f} > {hist_avg:.4f}×{RISK['vol_kill_multiplier']}")
        except Exception as e:
            log.warning("Vol check failed (allowing trade): %s", e)
        return RiskDecision(True)

    async def _check_news_blackout(self, req: TradeRequest) -> RiskDecision:
        now = datetime.now(timezone.utc)
        pre  = timedelta(minutes=RISK["news_blackout_minutes_pre"])
        post = timedelta(minutes=RISK["news_blackout_minutes_post"])
        for event_str in KNOWN_MACRO_EVENTS:
            try:
                event_dt = datetime.fromisoformat(event_str).replace(tzinfo=timezone.utc)
                if (event_dt - pre) <= now <= (event_dt + post):
                    return RiskDecision(False,
                        f"news_blackout: event at {event_str}")
            except Exception:
                continue
        return RiskDecision(True)

    async def _check_correlation(self, req: TradeRequest) -> RiskDecision:
        """
        Block if a new position would be highly correlated with 3+ existing positions.
        Uses a simplified check: count positions in the same direction on correlated assets.
        """
        positions = await get_open_positions()
        same_direction = [
            p for p in positions
            if p.get("side", "").upper() == req.direction.upper()
        ]
        if len(same_direction) < 3:
            return RiskDecision(True)   # Need at least 3 same-direction to matter

        # Check if the new asset has high correlation with existing ones
        try:
            corr = await self._get_correlation(req.inst_id, [p["inst_id"] for p in same_direction])
            if corr > RISK["correlation_max"]:
                return RiskDecision(False,
                    f"correlation_guard: corr={corr:.2f} > {RISK['correlation_max']} with existing positions")
        except Exception as e:
            log.warning("Correlation check failed (allowing): %s", e)

        return RiskDecision(True)

    # ── Kelly sizing ──────────────────────────────────────────────────────────

    def _kelly_size(self, req: TradeRequest) -> Tuple[float, float]:
        """
        Half-Kelly position sizing.
        Kelly % = (p×b - q) / b  where b = reward/risk ratio, p = win prob, q = 1-p
        Returns (position_size_in_USDT, kelly_fraction).
        """
        risk_pct = req.risk_pct / 100   # fraction of position that is at risk
        if risk_pct <= 0:
            return 0.0, 0.0

        # Estimate reward/risk from trade setup (default 2:1 if not provided)
        b = req.metadata.get("reward_risk_ratio", RISK["min_edge_ratio"])
        p = req.win_probability
        q = 1 - p

        kelly_fraction = (p * b - q) / b
        kelly_fraction = max(0.0, kelly_fraction)   # can't be negative
        half_kelly = kelly_fraction * RISK["kelly_fraction"]

        # Convert Kelly fraction to USDT position size
        # Kelly fraction = fraction of bankroll to bet ON THE RISK PORTION
        # position size = (account × kelly_fraction) / risk_pct
        max_risk_usdt = req.account_balance * RISK["max_trade_risk_pct"] / 100
        kelly_risk_usdt = req.account_balance * half_kelly

        # Cap at max trade risk
        risk_usdt = min(kelly_risk_usdt, max_risk_usdt)
        # Cap at bot allocation limit
        max_bot_usdt = req.account_balance * RISK["max_bot_allocation_pct"] / 100
        position_usdt = min(risk_usdt / risk_pct if risk_pct > 0 else 0, max_bot_usdt)

        return round(position_usdt, 2), round(half_kelly, 4)

    # ── Volatility helpers ────────────────────────────────────────────────────

    async def _get_vol(self, inst_id: str) -> Tuple[float, float]:
        """
        Returns (current_1h_vol, 7day_avg_vol) as annualized % return std dev.
        Caches for 5 minutes to avoid hammering the DB.
        """
        now = datetime.now(timezone.utc)
        cached = self._vol_cache.get(inst_id)
        if cached and (now - cached[1]).seconds < 300:
            return cached[0], cached[1] if len(cached) > 2 else (cached[0], 0)

        candles_1h = await get_candles(inst_id, "1H", limit=RISK["vol_lookback_hours"])
        if len(candles_1h) < 2:
            return 0.0, 0.0

        closes = np.array([float(c["close"]) for c in candles_1h])
        returns = np.diff(np.log(closes))

        current_vol = float(np.std(returns[-1:])) if len(returns) >= 1 else 0.0
        hist_avg_vol = float(np.std(returns)) if len(returns) > 1 else 0.0

        self._vol_cache[inst_id] = (current_vol, hist_avg_vol, now)
        return current_vol, hist_avg_vol

    async def _get_correlation(self, inst_id: str, other_ids: List[str]) -> float:
        """
        Returns max pairwise correlation between inst_id and any of other_ids.
        Uses 30-day daily close returns.
        """
        if not other_ids:
            return 0.0

        all_ids = list(set([inst_id] + other_ids))
        candle_map = {}
        for iid in all_ids:
            candles = await get_candles(iid, "1D",
                                         limit=RISK["correlation_lookback_days"] + 1)
            if len(candles) < 5:
                continue
            closes = np.array([float(c["close"]) for c in candles])
            candle_map[iid] = np.diff(np.log(closes))

        if inst_id not in candle_map:
            return 0.0

        target_rets = candle_map[inst_id]
        max_corr = 0.0
        for oid in other_ids:
            if oid not in candle_map or oid == inst_id:
                continue
            other_rets = candle_map[oid]
            min_len = min(len(target_rets), len(other_rets))
            if min_len < 5:
                continue
            corr = float(np.corrcoef(target_rets[-min_len:], other_rets[-min_len:])[0, 1])
            max_corr = max(max_corr, abs(corr))

        return max_corr


# ── Singleton ─────────────────────────────────────────────────────────────────

_risk_engine: Optional[RiskEngine] = None


def get_risk_engine() -> RiskEngine:
    global _risk_engine
    if _risk_engine is None:
        _risk_engine = RiskEngine()
    return _risk_engine
