"""
main.py — Master orchestrator for the BloFin 7-Bot Trading System.

Startup sequence:
  1. Logging
  2. Database (init schema)
  3. Alert manager (Telegram)
  4. BloFin REST client (verify API creds)
  5. Risk engine (load today's drawdown state)
  6. Recovery manager (reconcile positions)
  7. WebSocket (market data feeds)
  8. Health monitor (HTTP /health endpoint)
  9. Bots (in priority order: 6 → 2 → 5 → 3 → 1 → 4 → 7)
 10. Watchdog + API health monitor + daily reset (background tasks)

The system runs until SIGTERM/SIGINT, then gracefully shuts down.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

# ── Bootstrap ─────────────────────────────────────────────────────────────────

from alert_manager import setup_logging, get_alert_manager
setup_logging()

log = logging.getLogger("main")

from config import (
    TRADING_MODE, BLOFIN_API_KEY, DATABASE_URL,
    BOT1, BOT2, BOT3, BOT4, BOT5, BOT6, BOT7,
    PRIMARY_ASSETS, SYSTEM,
)


async def main() -> None:
    log.info("=" * 60)
    log.info("BloFin 7-Bot Trading System — Starting")
    log.info("Mode: %s", TRADING_MODE.upper())
    log.info("=" * 60)

    # ── 1. Validate environment ───────────────────────────────────────────────
    if not DATABASE_URL:
        log.critical("DATABASE_URL not set. Exiting.")
        sys.exit(1)
    if not BLOFIN_API_KEY:
        log.warning("BLOFIN_API_KEY not set — running in data-only mode")

    # ── 2. Database ───────────────────────────────────────────────────────────
    from database import init_db, upsert_daily_pnl, set_state
    await init_db()
    log.info("Database ready")

    # Record today's starting balance
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── 3. Alert manager ──────────────────────────────────────────────────────
    alert = get_alert_manager()
    await alert.start()

    # ── 4. BloFin REST client — verify connectivity ───────────────────────────
    from blofin_client import get_client
    try:
        client  = await get_client()
        balance = await client.get_usdt_balance()
        await upsert_daily_pnl(today_str, balance)
        await set_state("daily_starting_balance", str(balance))
        log.info("Exchange connected | Balance: %.2f USDT", balance)
        await alert.send(
            f"🟢 <b>System Starting</b>\n"
            f"Mode: {TRADING_MODE.upper()}\n"
            f"Balance: {balance:.2f} USDT\n"
            f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
        )
    except Exception as e:
        log.error("Failed to connect to BloFin: %s", e)
        await alert.send(f"⚠️ BloFin connection error on startup: {e}")
        # Don't exit — we'll keep retrying via the API health monitor

    # ── 5. Risk engine ────────────────────────────────────────────────────────
    from risk_engine import get_risk_engine
    risk = get_risk_engine()
    log.info("Risk engine ready")

    # ── 6. Recovery manager ───────────────────────────────────────────────────
    from recovery_manager import get_recovery_manager
    recovery = get_recovery_manager()

    if SYSTEM["position_reconcile_on_start"]:
        await recovery.reconcile_on_startup()

    # ── 7. WebSocket ──────────────────────────────────────────────────────────
    from blofin_websocket import get_ws
    ws = get_ws()
    await ws.subscribe_tickers(PRIMARY_ASSETS)
    await ws.subscribe_orders()
    await ws.subscribe_funding(PRIMARY_ASSETS)
    await ws.start()
    log.info("WebSocket feeds started")

    # ── 8. Dashboard + health endpoint ───────────────────────────────────────
    from dashboard import start_dashboard
    await start_dashboard()

    # ── 8.5 Set per-bot leverage on all trading pairs ────────────────────────
    all_bot_cfgs = [BOT1, BOT2, BOT3, BOT4, BOT5, BOT6, BOT7]
    asset_leverage: dict = {}
    for bc in all_bot_cfgs:
        lev = bc.get("leverage", 5)
        for asset in (bc.get("assets") or PRIMARY_ASSETS):
            asset_leverage[asset] = max(asset_leverage.get(asset, 1), lev)

    log.info("Setting leverage: %s", {a: f"{l}x" for a, l in asset_leverage.items()})
    try:
        _lev_client = await get_client()
        for asset, lev in asset_leverage.items():
            try:
                await _lev_client.set_leverage(asset, lev, "cross")
            except Exception as e:
                log.warning("Could not set leverage for %s: %s", asset, e)
    except Exception as e:
        log.warning("Leverage setup skipped (no client): %s", e)

    # ── 9. Instantiate bots ───────────────────────────────────────────────────
    from bots import (
        FundingArbBot, TrendFollowerBot, BreakoutBot,
        MeanReversionBot, ScalperBot, MarketMakerBot, MultiMomentumBot,
    )

    bots = []
    # Add only enabled bots (check config flag)
    if BOT6.get("enabled", True):
        bots.append(FundingArbBot())
    if BOT2.get("enabled", True):
        bots.append(TrendFollowerBot())
    if BOT5.get("enabled", True):
        bots.append(BreakoutBot())
    if BOT3.get("enabled", True):
        bots.append(MeanReversionBot())
    if BOT1.get("enabled", True):
        bots.append(ScalperBot())
    if BOT4.get("enabled", True):
        bots.append(MarketMakerBot())
    if BOT7.get("enabled", True):
        bots.append(MultiMomentumBot())

    log.info("Bots loaded: %s", [b.bot_id for b in bots])

    # ── 10. Register watchdog restart callbacks ───────────────────────────────
    bot_tasks: dict[str, asyncio.Task] = {}

    async def start_bot(bot) -> asyncio.Task:
        task = asyncio.create_task(bot.start(), name=bot.bot_id)
        bot_tasks[bot.bot_id] = task
        log.info("Bot started: %s", bot.bot_id)
        return task

    async def restart_bot_callback(bot_id: str):
        """Called by watchdog when a bot goes stale."""
        bot = next((b for b in bots if b.bot_id == bot_id), None)
        if not bot:
            log.error("Restart callback: bot %s not found", bot_id)
            return
        # Cancel existing task if running
        old_task = bot_tasks.get(bot_id)
        if old_task and not old_task.done():
            old_task.cancel()
            try:
                await old_task
            except asyncio.CancelledError:
                pass
        # Restart
        bot._running = True
        bot._paused  = False
        bot._error_count = 0
        await start_bot(bot)

    for bot in bots:
        recovery.register_bot_restart(
            bot.bot_id,
            lambda b=bot: restart_bot_callback(b.bot_id),
        )

    # Register graceful shutdown
    for bot in bots:
        recovery.register_shutdown(bot.stop)
    recovery.register_shutdown(ws.stop)
    recovery.register_shutdown(alert.stop)

    # ── 11. Start everything ──────────────────────────────────────────────────
    loop = asyncio.get_event_loop()
    recovery.setup_signal_handlers(loop)

    # Start bots
    for bot in bots:
        await start_bot(bot)
        await asyncio.sleep(2)   # stagger startup

    # Background tasks
    background_tasks = [
        asyncio.create_task(recovery.run_watchdog(),          name="watchdog"),
        asyncio.create_task(recovery.monitor_api_health(),    name="api_health"),
        asyncio.create_task(recovery.run_daily_reset(),       name="daily_reset"),
        asyncio.create_task(_daily_summary_loop(bots),        name="daily_summary"),
    ]

    log.info("=" * 60)
    log.info("All systems operational. %d bots running.", len(bots))
    log.info("=" * 60)

    # ── 12. Run forever ───────────────────────────────────────────────────────
    try:
        all_tasks = list(bot_tasks.values()) + background_tasks
        await asyncio.gather(*all_tasks, return_exceptions=True)
    except asyncio.CancelledError:
        log.info("Main gather cancelled — shutting down")
    finally:
        log.info("System shutdown complete")


# ── Daily summary ─────────────────────────────────────────────────────────────

async def _daily_summary_loop(bots) -> None:
    """Sends a daily P&L summary at 23:55 UTC."""
    while True:
        now = datetime.now(timezone.utc)
        # Next 23:55 UTC
        target = now.replace(hour=23, minute=55, second=0, microsecond=0)
        if now >= target:
            import asyncio
            await asyncio.sleep(86400 - (now - target).seconds)
        else:
            await asyncio.sleep((target - now).seconds)

        try:
            from blofin_client import get_client
            from database import get_today_pnl, get_open_positions
            client    = await get_client()
            balance   = await client.get_usdt_balance()
            pnl_row   = await get_today_pnl()
            open_pos  = await get_open_positions()
            daily_pnl = float(pnl_row.get("realized_pnl", 0)) if pnl_row else 0

            alert = get_alert_manager()
            await alert.send_daily_summary(
                balance       = balance,
                daily_pnl     = daily_pnl,
                open_positions= len(open_pos),
                trades_today  = 0,   # TODO: query trades table
            )
        except Exception as e:
            log.error("Daily summary error: %s", e)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as e:
        log.critical("Fatal error: %s", e, exc_info=True)
        sys.exit(1)
