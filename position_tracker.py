"""
position_tracker.py — Live position and order management.
Tracks trailing stops, handles partial exits, reconciles fills.

order_router.py logic is merged here for simplicity — one module
owns the full lifecycle from order placement to position close.
"""

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple

from config import IS_PAPER
from database import (
    insert_position, update_position, close_position, insert_trade,
    insert_order, update_order, get_open_positions, get_open_orders,
    log_risk_event,
)
from blofin_client import get_client
from alert_manager import get_alert_manager
from signal_engine import atr_value

log = logging.getLogger("positions")


class PositionManager:
    """
    Handles the full order → fill → position → close lifecycle.
    One instance shared across all bots.
    """

    def __init__(self):
        self._lock = asyncio.Lock()   # prevents concurrent order placement on same inst

    # ── Open a position ───────────────────────────────────────────────────────

    async def open_position(
        self,
        bot_id:      str,
        inst_id:     str,
        direction:   str,        # "LONG" | "SHORT"
        size_usdt:   float,
        entry_price: float,
        tp_price:    Optional[float] = None,
        sl_price:    Optional[float] = None,
        trailing_sl: bool = False,
        trailing_atr_mult: float = 2.0,
        metadata:    dict = None,
    ) -> Optional[str]:
        """
        Places a limit entry order and creates a position record.
        Returns position_id if successful, None on failure.
        """
        async with self._lock:
            client = await get_client()
            alert  = get_alert_manager()

            side     = "buy"  if direction == "LONG" else "sell"
            contract_size = await self._calc_contract_size(inst_id, size_usdt, entry_price)

            if contract_size <= 0:
                log.warning("Position size too small for %s (%.2f USDT)", inst_id, size_usdt)
                return None

            position_id    = f"pos_{bot_id}_{uuid.uuid4().hex[:12]}"
            client_order_id = f"entry_{position_id}"

            # Log order intent
            await insert_order({
                "order_id":        None,
                "client_order_id": client_order_id,
                "bot_id":          bot_id,
                "inst_id":         inst_id,
                "side":            side.upper(),
                "order_type":      "LIMIT",
                "price":           entry_price,
                "size":            contract_size,
                "status":          "PENDING",
                "tp_price":        tp_price,
                "sl_price":        sl_price,
            })

            try:
                resp = await client.place_order(
                    inst_id         = inst_id,
                    side            = side,
                    order_type      = "limit",
                    size            = contract_size,
                    price           = entry_price,
                    tp_price        = tp_price,
                    sl_price        = sl_price,
                    client_order_id = client_order_id,
                )
                exchange_order_id = resp.get("ordId") if isinstance(resp, dict) else None
                await update_order(client_order_id,
                                   order_id=exchange_order_id,
                                   status="OPEN")

            except Exception as e:
                log.error("Order placement failed [%s %s]: %s", bot_id, inst_id, e)
                await update_order(client_order_id, status="FAILED")
                await alert.send_error(bot_id, f"Order placement failed {inst_id}: {e}")
                return None

            # Create position record
            await insert_position({
                "position_id":      position_id,
                "bot_id":           bot_id,
                "inst_id":          inst_id,
                "side":             direction,
                "entry_price":      entry_price,
                "size":             contract_size,
                "tp_price":         tp_price,
                "sl_price":         sl_price,
                "trailing_sl":      trailing_sl,
                "trailing_atr_mult":trailing_atr_mult if trailing_sl else None,
                "highest_price":    entry_price,
                "lowest_price":     entry_price,
                "metadata":         metadata or {},
            })

            await alert.send_trade_opened(
                bot_id, inst_id, direction, entry_price,
                contract_size, tp_price, sl_price,
            )

            log.info("POSITION OPENED [%s] %s %s @ %.4f size=%.4f tp=%s sl=%s",
                      bot_id, inst_id, direction, entry_price,
                      contract_size, tp_price, sl_price)

            return position_id

    # ── Close a position ──────────────────────────────────────────────────────

    async def close_position(
        self,
        position_id: str,
        reason:      str,
        exit_price:  Optional[float] = None,
    ) -> bool:
        """
        Market-close a position. Calculates realized P&L and logs trade.
        Returns True on success.
        """
        positions = await get_open_positions()
        pos = next((p for p in positions if p["position_id"] == position_id), None)
        if not pos:
            log.warning("close_position: %s not found or already closed", position_id)
            return False

        client = await get_client()
        alert  = get_alert_manager()

        inst_id = pos["inst_id"]
        side    = pos["side"]
        size    = float(pos["size"])
        entry   = float(pos["entry_price"])

        try:
            resp = await client.close_position(inst_id, side, size)
            if exit_price is None:
                exit_price = await client.get_mid_price(inst_id)
        except Exception as e:
            log.error("close_position order failed [%s]: %s", position_id, e)
            await alert.send_error(pos["bot_id"], f"Close order failed {inst_id}: {e}")
            return False

        # Calculate P&L
        if side == "LONG":
            gross_pnl = (exit_price - entry) * size
        else:
            gross_pnl = (entry - exit_price) * size
        fees      = abs(gross_pnl) * 0.001    # ~0.1% BloFin taker fee estimate
        net_pnl   = gross_pnl - fees

        opened_at = pos.get("opened_at") or datetime.now(timezone.utc)
        closed_at = datetime.now(timezone.utc)
        duration  = int((closed_at - opened_at).total_seconds()) if opened_at else 0

        await close_position(position_id, exit_price, reason, net_pnl)

        # Generate exit commentary
        from commentary import generate_exit_commentary
        exit_comment = generate_exit_commentary(
            bot_id       = pos["bot_id"],
            inst_id      = inst_id,
            side         = side,
            entry_price  = entry,
            exit_price   = exit_price,
            size         = size,
            net_pnl      = net_pnl,
            close_reason = reason,
            duration_s   = duration,
            metadata     = pos.get("metadata") or {},
        )

        await insert_trade({
            "position_id":    position_id,
            "bot_id":         pos["bot_id"],
            "inst_id":        inst_id,
            "side":           side,
            "entry_price":    entry,
            "exit_price":     exit_price,
            "size":           size,
            "gross_pnl":      gross_pnl,
            "fees":           fees,
            "net_pnl":        net_pnl,
            "duration_seconds": duration,
            "close_reason":   reason,
            "exit_commentary": exit_comment,
            "opened_at":      opened_at,
            "closed_at":      closed_at,
        })

        await alert.send_trade_closed(
            pos["bot_id"], inst_id, net_pnl, reason, entry, exit_price
        )

        log.info("POSITION CLOSED [%s] %s %s | pnl=%.2f USDT | reason=%s",
                  pos["bot_id"], inst_id, side, net_pnl, reason)
        return True

    # ── Partial close ─────────────────────────────────────────────────────────

    async def partial_close(self, position_id: str, fraction: float,
                             reason: str = "partial_exit") -> bool:
        """
        Close `fraction` (0–1) of the position. Update size in DB.
        Used by Bot 2 trend follower to take 50% off at first target.
        """
        positions = await get_open_positions()
        pos = next((p for p in positions if p["position_id"] == position_id), None)
        if not pos:
            return False

        client     = await get_client()
        inst_id    = pos["inst_id"]
        side       = pos["side"]
        total_size = float(pos["size"])
        close_size = round(total_size * fraction, 4)

        try:
            close_side = "sell" if side == "LONG" else "buy"
            await client.place_order(
                inst_id    = inst_id,
                side       = close_side,
                order_type = "market",
                size       = close_size,
                reduce_only= True,
            )
            remaining = total_size - close_size
            await update_position(position_id, size=remaining)
            log.info("PARTIAL CLOSE [%s] %s %.4f / %.4f (%s)",
                      position_id, inst_id, close_size, total_size, reason)
            return True
        except Exception as e:
            log.error("Partial close failed [%s]: %s", position_id, e)
            return False

    # ── Trailing stop update ──────────────────────────────────────────────────

    async def update_trailing_stop(
        self,
        position_id:  str,
        current_price: float,
        candle_highs: List[float],
        candle_lows:  List[float],
        candle_closes:List[float],
        atr_period:   int = 14,
        atr_mult:     float = 2.0,
    ) -> Optional[float]:
        """
        Calculates and updates trailing SL based on ATR.
        Returns new SL price if updated, None if unchanged.
        """
        positions = await get_open_positions()
        pos = next((p for p in positions if p["position_id"] == position_id), None)
        if not pos or not pos.get("trailing_sl"):
            return None

        client   = await get_client()
        side     = pos["side"]
        inst_id  = pos["inst_id"]
        atr_mult = float(pos.get("trailing_atr_mult") or atr_mult)
        current_atr = atr_value(candle_highs, candle_lows, candle_closes, atr_period)
        if not current_atr:
            return None

        # Track highest/lowest price seen
        if side == "LONG":
            highest = max(float(pos.get("highest_price") or current_price), current_price)
            new_sl  = highest - atr_mult * current_atr
            old_sl  = float(pos.get("sl_price") or 0)
            if new_sl > old_sl:
                await update_position(position_id,
                                       sl_price=new_sl,
                                       highest_price=highest)
                try:
                    await client.set_tp_sl(inst_id, "long", sl_price=new_sl)
                except Exception as e:
                    log.warning("Failed to update trailing SL on exchange: %s", e)
                return new_sl
        else:
            lowest = min(float(pos.get("lowest_price") or current_price), current_price)
            new_sl = lowest + atr_mult * current_atr
            old_sl = float(pos.get("sl_price") or float("inf"))
            if new_sl < old_sl:
                await update_position(position_id,
                                       sl_price=new_sl,
                                       lowest_price=lowest)
                try:
                    await client.set_tp_sl(inst_id, "short", sl_price=new_sl)
                except Exception as e:
                    log.warning("Failed to update trailing SL on exchange: %s", e)
                return new_sl

        return None

    # ── Check if SL/TP hit (paper mode) ──────────────────────────────────────

    async def check_sl_tp_paper(self, position_id: str,
                                  current_price: float) -> Optional[str]:
        """No longer used - BloFin demo exchange handles SL/TP natively."""
        return None

    # ── Cancel unfilled order ─────────────────────────────────────────────────

    async def cancel_stale_entry(self, position_id: str,
                                   timeout_seconds: int = 60) -> bool:
        """
        Cancel entry order if not filled within timeout.
        Used by all bots to prevent hanging orders.
        """
        orders = await get_open_orders()
        order = next(
            (o for o in orders if o.get("client_order_id", "").endswith(position_id[-12:])),
            None,
        )
        if not order:
            return False

        created = order.get("created_at")
        if not created:
            return False
        age = (datetime.now(timezone.utc) - created).total_seconds()
        if age < timeout_seconds:
            return False

        client = await get_client()
        try:
            await client.cancel_order(
                order["inst_id"],
                client_order_id=order["client_order_id"],
            )
            await update_order(order["client_order_id"], status="CANCELLED")
            # Also mark position as cancelled
            from database import update_position as up
            await up(position_id, status="CANCELLED")
            log.info("Cancelled stale entry order for %s (age=%ds)", position_id, age)
            return True
        except Exception as e:
            log.warning("Failed to cancel stale order %s: %s", position_id, e)
            return False

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _calc_contract_size(self, inst_id: str, usdt_size: float,
                                   price: float) -> float:
        """
        Convert USDT notional to contract quantity.
        BloFin perps use 'contracts' where 1 contract = 1 unit of base.
        """
        if price <= 0:
            return 0.0
        return round(usdt_size / price, 4)


# ── Singleton ─────────────────────────────────────────────────────────────────

_position_manager: Optional[PositionManager] = None


def get_position_manager() -> PositionManager:
    global _position_manager
    if _position_manager is None:
        _position_manager = PositionManager()
    return _position_manager
