"""
bots/bot_multi_momentum.py — Bot 7: Multi-Asset Momentum Diversification
Priority 8 — portfolio-level bot, rebalances daily.

Logic:
  - Rank top 10 assets by 30-day volume daily
  - Score each by momentum: (Close - Close[20]) / ATR(20)
  - Long top 3 momentum assets at 6% account each
  - Short bottom 2 momentum assets at 4% account each
  - Rebalance only if composition would change > 10%
  - Exclude if correlation > 0.85 with another held asset
  - Halt 72h if portfolio drawdown exceeds 5% in a week
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from bot_base import BotBase
from config import BOT7, PRIMARY_ASSETS
from database import get_open_positions
from blofin_client import get_client
from position_tracker import get_position_manager
from signal_engine import momentum_score, atr_value, parse_candles
from alert_manager import get_alert_manager

import numpy as np

log = logging.getLogger("bot7_momentum")


class MultiMomentumBot(BotBase):
    BOT_ID    = "bot7_momentum"
    BOT_NAME  = "Multi-Asset Momentum"
    TIMEFRAME = "1D"
    LOOP_INTERVAL_SECONDS = 3600   # check every hour, rebalance at midnight

    def __init__(self):
        super().__init__(BOT7)
        self._last_rebalance:   Optional[datetime] = None
        self._portfolio_week_start_value: float = 0.0
        self._week_start_time:  Optional[datetime] = None
        self._halted_until:     Optional[datetime] = None
        self._current_longs:    List[str] = []
        self._current_shorts:   List[str] = []

    # ── Main loop override ────────────────────────────────────────────────────

    async def start(self) -> None:
        """Override to run as a daily rebalancer instead of per-asset."""
        from database import upsert_bot_state
        log.info("[%s] Starting...", self.bot_id)
        self._running = True
        await upsert_bot_state(self.bot_id, "RUNNING")
        await self._rebalance_loop()

    async def strategy_tick(self, inst_id: str, candles: dict) -> None:
        """Not used — bot7 overrides the main loop."""
        pass

    # ── Rebalance loop ────────────────────────────────────────────────────────

    async def _rebalance_loop(self) -> None:
        from database import heartbeat as db_heartbeat, upsert_bot_state
        alert = get_alert_manager()

        while self._running:
            try:
                await db_heartbeat(self.bot_id)

                # Check 72h halt
                if self._halted_until and datetime.now(timezone.utc) < self._halted_until:
                    remaining = (self._halted_until - datetime.now(timezone.utc)).seconds // 60
                    log.info("[%s] Halted for %d more minutes", self.bot_id, remaining)
                    await asyncio.sleep(3600)
                    continue

                # Only rebalance once per day at configured hour
                now = datetime.now(timezone.utc)
                should_rebalance = (
                    self._last_rebalance is None or
                    (now - self._last_rebalance).total_seconds() >= 86400
                ) and now.hour == self.config["rebalance_hour_utc"]

                if should_rebalance:
                    await self._run_rebalance()
                    self._last_rebalance = now

                # Always check weekly drawdown
                await self._check_weekly_drawdown()

            except Exception as e:
                log.error("[%s] Rebalance loop error: %s", self.bot_id, e, exc_info=True)

            await asyncio.sleep(self.LOOP_INTERVAL_SECONDS)

    # ── Core rebalance ────────────────────────────────────────────────────────

    async def _run_rebalance(self) -> None:
        log.info("[%s] Running daily rebalance...", self.bot_id)
        alert  = get_alert_manager()
        client = await get_client()
        pm     = get_position_manager()

        # 1. Get universe
        universe = await self._get_universe(client)
        if len(universe) < 5:
            log.warning("[%s] Universe too small (%d assets)", self.bot_id, len(universe))
            return

        # 2. Score each asset
        scores: List[Tuple[str, float]] = []
        for inst_id in universe:
            try:
                raw = await client.get_candles(inst_id, "1D", limit=60)
                if not raw or len(raw) < 25:
                    continue
                c = parse_candles(raw)
                atr_vals = atr_value(c["highs"], c["lows"], c["closes"],
                                      self.config["atr_period"])
                if not atr_vals:
                    continue
                score = momentum_score(c["closes"], [atr_vals],
                                        self.config["momentum_period"])
                scores.append((inst_id, score))
            except Exception as e:
                log.warning("[%s] Failed to score %s: %s", self.bot_id, inst_id, e)

        if len(scores) < 5:
            return

        scores.sort(key=lambda x: x[1], reverse=True)
        log.info("[%s] Momentum scores: %s", self.bot_id,
                  [(s[0], round(s[1], 2)) for s in scores])

        # 3. Apply correlation filter
        target_longs  = await self._select_with_corr_filter(
            [s[0] for s in scores[:5]],    # top 5 candidates
            self.config["long_count"],
        )
        target_shorts = await self._select_with_corr_filter(
            [s[0] for s in scores[-5:]],   # bottom 5 candidates
            self.config["short_count"],
        )

        # Remove overlap
        target_longs  = [a for a in target_longs  if a not in target_shorts]
        target_shorts = [a for a in target_shorts if a not in target_longs]

        # 4. Check if rebalance is needed (> 10% change)
        long_changed  = set(target_longs)  != set(self._current_longs)
        short_changed = set(target_shorts) != set(self._current_shorts)
        if not long_changed and not short_changed:
            log.info("[%s] Portfolio unchanged — skipping rebalance", self.bot_id)
            return

        log.info("[%s] Rebalancing → longs=%s shorts=%s",
                  self.bot_id, target_longs, target_shorts)

        # 5. Close positions that are no longer in target
        open_positions = await get_open_positions(bot_id=self.bot_id)
        for pos in open_positions:
            iid  = pos["inst_id"]
            side = pos["side"]
            should_close = (
                (side == "LONG"  and iid not in target_longs) or
                (side == "SHORT" and iid not in target_shorts)
            )
            if should_close:
                price = await client.get_mid_price(iid)
                await pm.close_position(pos["position_id"], "rebalance", price)

        await asyncio.sleep(2)

        # 6. Open new positions
        balance = await client.get_usdt_balance()
        for inst_id in target_longs:
            if inst_id not in self._current_longs:
                price    = await client.get_mid_price(inst_id)
                size_usdt = balance * (self.config["long_weight_pct"] / 100)
                sl_price  = price * 0.92    # 8% wide stop for daily timeframe
                await self._request_trade(
                    inst_id=inst_id, direction="LONG",
                    entry_price=price, sl_price=sl_price,
                    win_prob=0.55, reward_risk=2.5,
                    metadata={"strategy": "momentum_long",
                               "target_pct": self.config["long_weight_pct"]},
                )

        for inst_id in target_shorts:
            if inst_id not in self._current_shorts:
                price     = await client.get_mid_price(inst_id)
                size_usdt = balance * (self.config["short_weight_pct"] / 100)
                sl_price  = price * 1.08    # 8% wide stop
                await self._request_trade(
                    inst_id=inst_id, direction="SHORT",
                    entry_price=price, sl_price=sl_price,
                    win_prob=0.52, reward_risk=2.0,
                    metadata={"strategy": "momentum_short",
                               "target_pct": self.config["short_weight_pct"]},
                )

        self._current_longs  = target_longs
        self._current_shorts = target_shorts

        await alert.send(
            f"📊 <b>Bot7 Rebalanced</b>\n"
            f"Longs: {', '.join(target_longs)}\n"
            f"Shorts: {', '.join(target_shorts)}"
        )

    # ── Universe selection ────────────────────────────────────────────────────

    async def _get_universe(self, client) -> List[str]:
        """Top N assets by 30-day volume. Falls back to PRIMARY_ASSETS."""
        try:
            tickers = await client.get_tickers("SWAP")
            vol_ranked = sorted(
                tickers,
                key=lambda t: float(t.get("volCcy24h", 0)),
                reverse=True,
            )
            universe = [t["instId"] for t in vol_ranked
                        if t.get("instId", "").endswith("USDT")]
            return universe[:self.config["universe_size"]]
        except Exception as e:
            log.warning("[%s] Universe fetch failed, using primary assets: %s", self.bot_id, e)
            return PRIMARY_ASSETS

    # ── Correlation filter ────────────────────────────────────────────────────

    async def _select_with_corr_filter(self, candidates: List[str],
                                        count: int) -> List[str]:
        """Select `count` assets from candidates, filtering out high-correlation pairs."""
        client = await get_client()
        returns_cache: Dict[str, np.ndarray] = {}

        for iid in candidates:
            try:
                raw = await client.get_candles(iid, "1D", limit=35)
                if raw and len(raw) >= 25:
                    closes = [float(r[4]) for r in raw]
                    import math
                    rets = np.array([math.log(closes[i] / closes[i-1])
                                     for i in range(1, len(closes))])
                    returns_cache[iid] = rets
            except Exception:
                pass

        selected = []
        for iid in candidates:
            if iid not in returns_cache:
                continue
            # Check correlation with already-selected assets
            too_correlated = False
            for sel_id in selected:
                if sel_id not in returns_cache:
                    continue
                min_len = min(len(returns_cache[iid]), len(returns_cache[sel_id]))
                if min_len < 5:
                    continue
                corr = float(np.corrcoef(
                    returns_cache[iid][-min_len:],
                    returns_cache[sel_id][-min_len:],
                )[0, 1])
                if abs(corr) > self.config["correlation_max"]:
                    too_correlated = True
                    break
            if not too_correlated:
                selected.append(iid)
            if len(selected) >= count:
                break

        return selected

    # ── Weekly drawdown guard ─────────────────────────────────────────────────

    async def _check_weekly_drawdown(self) -> None:
        client = await get_client()
        now    = datetime.now(timezone.utc)

        if self._week_start_time is None:
            self._week_start_time = now
            self._portfolio_week_start_value = await client.get_usdt_balance()
            return

        if (now - self._week_start_time).days >= 7:
            # New week
            self._week_start_time = now
            self._portfolio_week_start_value = await client.get_usdt_balance()
            return

        current_balance = await client.get_usdt_balance()
        start_val = self._portfolio_week_start_value
        if start_val <= 0:
            return

        drawdown_pct = (start_val - current_balance) / start_val * 100
        if drawdown_pct >= self.config["weekly_halt_drawdown_pct"]:
            halt_hours = self.config["halt_hours"]
            self._halted_until = now + timedelta(hours=halt_hours)
            alert = get_alert_manager()
            await alert.send(
                f"⚠️ Bot7 halted for {halt_hours}h: "
                f"weekly drawdown {drawdown_pct:.2f}% >= {self.config['weekly_halt_drawdown_pct']}%"
            )
            log.warning("[%s] Weekly drawdown halt: %.2f%% — halted until %s",
                         self.bot_id, drawdown_pct, self._halted_until)
