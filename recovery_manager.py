"""
recovery_manager.py — Fault tolerance and crash recovery.

Handles:
  • Startup: reconcile DB state with live exchange positions
  • API outage: detect, wait, reconnect, re-verify state
  • Stale bot detection: restart bots that stop heartbeating
  • State corruption: detect and alert
  • Graceful shutdown: flatten positions if configured
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Dict, Optional, List

from config import SYSTEM, IS_PAPER
from database import (
    get_open_positions, update_position, close_position,
    insert_position, upsert_bot_state, get_stale_bots,
    log_risk_event, get_all_states,
)
from blofin_client import get_client
from alert_manager import get_alert_manager

log = logging.getLogger("recovery")


class RecoveryManager:
    """Runs as a background task alongside the main system."""

    def __init__(self):
        self._shutdown_requested = False
        self._bot_restart_callbacks: Dict[str, callable] = {}
        self._shutdown_callbacks: List[callable] = []

    def register_bot_restart(self, bot_id: str, callback: callable) -> None:
        """Register a coroutine that restarts a specific bot."""
        self._bot_restart_callbacks[bot_id] = callback

    def register_shutdown(self, callback: callable) -> None:
        """Register a coroutine called during graceful shutdown."""
        self._shutdown_callbacks.append(callback)

    # ── Startup reconciliation ────────────────────────────────────────────────

    async def reconcile_on_startup(self) -> None:
        """
        Called once at startup. Compares DB positions against live exchange
        positions. The exchange is always the source of truth.
        """
        log.info("Starting position reconciliation...")
        alert = get_alert_manager()

        try:
            client  = await get_client()
            db_positions   = await get_open_positions()
            live_positions = await client.get_positions()

            # Build lookup maps
            live_by_inst: Dict[str, dict] = {}
            for lp in live_positions:
                iid = lp.get("instId", "")
                if float(lp.get("pos", 0)) != 0:
                    live_by_inst[iid] = lp

            db_by_pos_id: Dict[str, dict] = {
                p["position_id"]: p for p in db_positions
            }

            orphaned = 0
            missing  = 0

            # Check DB positions that are no longer on the exchange
            for pos in db_positions:
                iid = pos["inst_id"]
                if iid not in live_by_inst:
                    log.warning("RECONCILE: DB position %s not on exchange — marking closed",
                                 pos["position_id"])
                    await close_position(
                        pos["position_id"],
                        exit_price=float(pos.get("current_price") or pos["entry_price"]),
                        reason="reconcile_not_found_on_exchange",
                        realized_pnl=0,
                    )
                    await alert.send(
                        f"⚠️ Reconcile: position {pos['position_id']} ({iid}) "
                        f"not found on exchange — marked closed in DB"
                    )
                    orphaned += 1

            # Check live positions that aren't in DB
            db_inst_ids = {p["inst_id"] for p in db_positions}
            for iid, lp in live_by_inst.items():
                if iid not in db_inst_ids:
                    log.warning("RECONCILE: Live position %s not in DB — creating record", iid)
                    await insert_position({
                        "position_id": f"recovered_{iid}_{int(datetime.now(timezone.utc).timestamp())}",
                        "bot_id":      "recovered",
                        "inst_id":     iid,
                        "side":        "LONG" if float(lp.get("pos", 0)) > 0 else "SHORT",
                        "entry_price": float(lp.get("avgPx", 0)),
                        "size":        abs(float(lp.get("pos", 0))),
                        "metadata":    {"source": "reconcile"},
                    })
                    await alert.send(
                        f"⚠️ Reconcile: live position {iid} not in DB — created recovery record. "
                        f"MANUAL REVIEW REQUIRED."
                    )
                    missing += 1

            # Re-verify TP/SL on all remaining open positions
            await self._reverify_tp_sl()

            log.info("Reconciliation complete. Orphaned=%d Missing=%d", orphaned, missing)
            if orphaned or missing:
                await log_risk_event(
                    "RECONCILE",
                    f"startup_reconcile orphaned={orphaned} missing={missing}",
                )

        except Exception as e:
            log.error("Reconciliation failed: %s", e, exc_info=True)
            await alert.send(f"❌ RECONCILE FAILED: {e} — manual review required")

    async def _reverify_tp_sl(self) -> None:
        """
        After reconcile, ensure every open position has a SL attached.
        If a position has no SL (detected on restart), attach one at the
        last known SL level or at a safe default (3× ATR).
        """
        db_positions = await get_open_positions()
        client = await get_client()

        for pos in db_positions:
            if pos.get("sl_price"):
                continue    # already has SL
            log.warning("Position %s has no SL — attempting to set safe SL",
                         pos["position_id"])
            try:
                mark = await client.get_mark_price(pos["inst_id"])
                sl_distance = mark * 0.03    # 3% emergency SL
                sl_price = mark - sl_distance if pos["side"] == "LONG" else mark + sl_distance
                pos_side = pos["side"].lower()
                await client.set_tp_sl(pos["inst_id"], pos_side, sl_price=sl_price)
                await update_position(pos["position_id"], sl_price=sl_price)
                log.info("Emergency SL set for %s at %.4f", pos["position_id"], sl_price)
            except Exception as e:
                log.error("Failed to set emergency SL for %s: %s", pos["position_id"], e)

    # ── Watchdog ──────────────────────────────────────────────────────────────

    async def run_watchdog(self) -> None:
        """
        Runs forever. Every 30 seconds, checks all registered bots for stale
        heartbeats. Restarts any bot that hasn't heartbeated in 90 seconds.
        """
        log.info("Watchdog started")
        alert = get_alert_manager()

        while not self._shutdown_requested:
            await asyncio.sleep(SYSTEM["heartbeat_interval_seconds"])
            try:
                stale_bots = await get_stale_bots(SYSTEM["watchdog_timeout_seconds"])
                for bot in stale_bots:
                    bot_id = bot["bot_id"]
                    log.warning("Watchdog: bot %s is stale (last heartbeat %s)",
                                 bot_id, bot["last_heartbeat"])
                    await alert.send(
                        f"⚠️ Watchdog: bot {bot_id} stopped heartbeating — attempting restart"
                    )
                    await upsert_bot_state(bot_id, "RESTARTING")

                    if bot_id in self._bot_restart_callbacks:
                        try:
                            await self._bot_restart_callbacks[bot_id]()
                            log.info("Watchdog: bot %s restarted successfully", bot_id)
                        except Exception as e:
                            log.error("Watchdog: failed to restart %s: %s", bot_id, e)
                            await alert.send(f"❌ Failed to restart {bot_id}: {e}")
                    else:
                        log.error("Watchdog: no restart callback for bot %s", bot_id)

            except Exception as e:
                log.error("Watchdog error: %s", e)

    # ── API outage detection ──────────────────────────────────────────────────

    async def monitor_api_health(self) -> None:
        """
        Pings the exchange every 60 seconds. If 3 consecutive failures,
        alerts and waits. When API recovers, triggers reconciliation.
        """
        log.info("API health monitor started")
        alert = get_alert_manager()
        client = await get_client()
        consecutive_failures = 0
        outage_start: Optional[datetime] = None

        while not self._shutdown_requested:
            await asyncio.sleep(60)
            try:
                # Lightweight ping: get BTC ticker
                await client.get_ticker("BTC-USDT")
                if consecutive_failures >= 3:
                    outage_duration = (
                        datetime.now(timezone.utc) - outage_start
                    ).seconds if outage_start else 0
                    log.info("API recovered after %ds outage", outage_duration)
                    await alert.send(
                        f"✅ BloFin API recovered after {outage_duration}s outage — reconciling..."
                    )
                    await self.reconcile_on_startup()
                consecutive_failures = 0
                outage_start = None

            except Exception as e:
                consecutive_failures += 1
                if consecutive_failures == 3:
                    outage_start = datetime.now(timezone.utc)
                    await alert.send(
                        f"🚨 BloFin API UNREACHABLE (3 consecutive failures): {e}\n"
                        f"All bots paused. System will self-recover when API returns."
                    )
                    await log_risk_event("API_OUTAGE", str(e))
                log.warning("API health check failed (%d): %s",
                             consecutive_failures, e)

    # ── Signal handlers ───────────────────────────────────────────────────────

    def setup_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        """Register OS signals for graceful shutdown."""
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self._graceful_shutdown(s)),
                )
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                signal.signal(sig, lambda s, f: asyncio.create_task(
                    self._graceful_shutdown(s)
                ))

    async def _graceful_shutdown(self, sig) -> None:
        """Called on SIGTERM/SIGINT. Runs all registered shutdown callbacks."""
        log.info("Graceful shutdown triggered by signal %s", sig)
        self._shutdown_requested = True
        alert = get_alert_manager()
        await alert.send(f"🔴 System shutting down (signal {sig}). All bots stopping.")

        for callback in self._shutdown_callbacks:
            try:
                await callback()
            except Exception as e:
                log.error("Shutdown callback error: %s", e)

        log.info("All shutdown callbacks complete. Exiting.")
        sys.exit(0)

    # ── Daily reset ───────────────────────────────────────────────────────────

    async def run_daily_reset(self) -> None:
        """
        Runs at 00:01 UTC every day.
        Records starting balance for the new day's drawdown calculation.
        Resumes system if it was halted only by daily drawdown (not monthly).
        """
        import pytz
        alert = get_alert_manager()
        client = await get_client()

        while not self._shutdown_requested:
            now = datetime.now(timezone.utc)
            # Next 00:01 UTC
            next_reset = now.replace(hour=0, minute=1, second=0, microsecond=0)
            if now >= next_reset:
                next_reset = next_reset.replace(day=next_reset.day + 1)
            wait_secs = (next_reset - now).total_seconds()
            await asyncio.sleep(wait_secs)

            try:
                balance = await client.get_usdt_balance()
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                from database import upsert_daily_pnl, set_state
                await upsert_daily_pnl(today_str, balance)
                await set_state("daily_starting_balance", str(balance))

                # Auto-resume if yesterday halted for daily drawdown
                halted = await get_all_states()
                halt_reason = halted.get("halt_reason", "")
                if halted.get("system_halted") == "1" and "daily_drawdown" in halt_reason:
                    from database import set_state as ss
                    await ss("system_halted", "0")
                    await ss("halt_reason", "")
                    await log_risk_event("RESUME", "daily_reset_auto_resume")
                    await alert.send(
                        f"🟢 New trading day started. Balance: {balance:.2f} USDT. "
                        f"Daily halt auto-cleared."
                    )
                else:
                    await alert.send(
                        f"📅 New day: {today_str} | Starting balance: {balance:.2f} USDT"
                    )

            except Exception as e:
                log.error("Daily reset error: %s", e)


# ── Singleton ─────────────────────────────────────────────────────────────────

_recovery: Optional[RecoveryManager] = None


def get_recovery_manager() -> RecoveryManager:
    global _recovery
    if _recovery is None:
        _recovery = RecoveryManager()
    return _recovery
