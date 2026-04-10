"""
bots/bot_market_maker.py — Bot 4: Market Maker
Priority 7 — most complex. Requires stable infrastructure.
"""

import asyncio
import logging
from typing import Dict, Optional
from bot_base import BotBase
from config import BOT4
from blofin_client import get_client
from database import update_order, insert_order, get_open_orders
from alert_manager import get_alert_manager
import uuid

log = logging.getLogger("bot4_mm")


class MarketMakerBot(BotBase):
    BOT_ID    = "bot4_mm"
    BOT_NAME  = "Market Maker"
    TIMEFRAME = "1m"
    LOOP_INTERVAL_SECONDS = 10   # tight loop

    def __init__(self):
        super().__init__(BOT4)
        self._bid_orders:    Dict[str, str] = {}   # inst_id → client_order_id
        self._ask_orders:    Dict[str, str] = {}
        self._inventory:     Dict[str, float] = {}  # inst_id → net position (+ = long)
        self._fill_counts:   Dict[str, Dict[str, int]] = {}  # inst_id → {bid: N, ask: N}
        self._paused_until:  Dict[str, float] = {}

    async def strategy_tick(self, inst_id: str, candles: dict) -> None:
        try:
            import time
            # Check if paused due to inventory loss
            if time.time() < self._paused_until.get(inst_id, 0):
                return

            client = await get_client()

            # Vol guard: don't MM in volatile markets
            closes = candles["closes"]
            if len(closes) < 5:
                return
            price_range_pct = (max(closes[-5:]) - min(closes[-5:])) / closes[-5] * 100
            if price_range_pct > self.config["vol_max_1h_pct"]:
                await self._cancel_all(inst_id)
                return

            # Get orderbook midpoint
            bid, ask = await client.get_best_bid_ask(inst_id)
            if not bid or not ask:
                return
            mid = (bid + ask) / 2
            spread_pct = self.config["spread_target_pct"] / 100

            # Inventory guard
            inventory = self._inventory.get(inst_id, 0)
            account_balance = await client.get_usdt_balance()
            max_inv = account_balance * (self.config["inventory_max_pct"] / 100)
            inventory_notional = abs(inventory) * mid

            if inventory_notional > max_inv:
                log.warning("[%s] Inventory limit hit: %s inv=%.4f", self.bot_id, inst_id, inventory)
                await self._rebalance_inventory(inst_id, inventory, mid, client)
                return

            # Skew based on recent fill imbalance
            fills = self._fill_counts.setdefault(inst_id, {"bid": 0, "ask": 0})
            skew = 0.0
            ratio = self.config["skew_threshold_ratio"]
            if fills["bid"] > fills["ask"] * ratio:
                skew = self.config["skew_adjustment_pct"] / 100   # skew toward ask
            elif fills["ask"] > fills["bid"] * ratio:
                skew = -self.config["skew_adjustment_pct"] / 100  # skew toward bid

            our_bid = mid * (1 - spread_pct + skew)
            our_ask = mid * (1 + spread_pct + skew)

            # Cancel and refresh quotes
            await self._cancel_all(inst_id)
            await asyncio.sleep(0.2)

            # Place bid
            bid_coid = f"mm_bid_{inst_id}_{uuid.uuid4().hex[:8]}"
            await client.place_order(inst_id, "buy", "limit",
                                      size=0.001, price=our_bid,
                                      client_order_id=bid_coid)
            self._bid_orders[inst_id] = bid_coid

            # Place ask
            ask_coid = f"mm_ask_{inst_id}_{uuid.uuid4().hex[:8]}"
            await client.place_order(inst_id, "sell", "limit",
                                      size=0.001, price=our_ask,
                                      client_order_id=ask_coid)
            self._ask_orders[inst_id] = ask_coid

            log.debug("[%s] Quotes: %s bid=%.4f ask=%.4f inv=%.4f",
                       self.bot_id, inst_id, our_bid, our_ask, inventory)

        except Exception as e:
            log.error("[%s] MM error %s: %s", self.bot_id, inst_id, e, exc_info=True)

    async def _cancel_all(self, inst_id: str) -> None:
        client = await get_client()
        for orders_dict in [self._bid_orders, self._ask_orders]:
            coid = orders_dict.pop(inst_id, None)
            if coid:
                try:
                    await client.cancel_order(inst_id, client_order_id=coid)
                except Exception:
                    pass

    async def _rebalance_inventory(self, inst_id: str, inventory: float,
                                    mid: float, client) -> None:
        """Reduce inventory that exceeds limits."""
        if inventory > 0:
            await client.place_order(inst_id, "sell", "market",
                                      size=abs(inventory) * 0.5, reduce_only=True)
        elif inventory < 0:
            await client.place_order(inst_id, "buy", "market",
                                      size=abs(inventory) * 0.5, reduce_only=True)
