"""
health_monitor.py — HTTP health check endpoint and system metrics.
Runs an aiohttp server on port 8080.
Railway, Docker, and uptime monitors hit /health to verify system is alive.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone

import aiohttp
from aiohttp import web

import psutil

from config import SYSTEM
from database import get_all_states, get_open_positions, get_state
from blofin_client import get_client

log = logging.getLogger("health")

_start_time = time.time()


async def health_handler(request: web.Request) -> web.Response:
    """
    GET /health — Returns 200 if system is operational, 503 if halted.
    Used by Railway/Docker health checks and external monitors.
    """
    try:
        states       = await get_all_states()
        is_halted    = states.get("system_halted", "0") == "1"
        halt_reason  = states.get("halt_reason", "")
        open_pos     = await get_open_positions()
        uptime_s     = int(time.time() - _start_time)

        payload = {
            "status":       "halted" if is_halted else "running",
            "mode":         "PAPER" if not __import__("config").IS_LIVE else "LIVE",
            "uptime_s":     uptime_s,
            "open_positions": len(open_pos),
            "halt_reason":  halt_reason if is_halted else None,
            "timestamp":    datetime.now(timezone.utc).isoformat(),
            "system": {
                "cpu_pct":  psutil.cpu_percent(),
                "mem_pct":  psutil.virtual_memory().percent,
            },
        }

        status = 503 if is_halted else 200
        return web.Response(
            status=status,
            content_type="application/json",
            text=json.dumps(payload, indent=2),
        )
    except Exception as e:
        log.error("Health check error: %s", e)
        return web.Response(status=500, text=str(e))


async def metrics_handler(request: web.Request) -> web.Response:
    """GET /metrics — Extended metrics for monitoring dashboards."""
    try:
        states    = await get_all_states()
        open_pos  = await get_open_positions()
        client    = await get_client()
        balance   = await client.get_usdt_balance()

        # Per-bot status from states
        bot_statuses = {k: v for k, v in states.items() if k.startswith("bot_")}

        payload = {
            "balance_usdt":    round(balance, 2),
            "open_positions":  len(open_pos),
            "system_halted":   states.get("system_halted", "0") == "1",
            "uptime_s":        int(time.time() - _start_time),
            "bot_states":      bot_statuses,
            "positions":       [
                {
                    "inst_id":    p["inst_id"],
                    "bot_id":     p["bot_id"],
                    "side":       p["side"],
                    "entry":      float(p["entry_price"]),
                    "pnl":        float(p.get("unrealized_pnl") or 0),
                }
                for p in open_pos
            ],
            "system_resources": {
                "cpu_pct":  psutil.cpu_percent(),
                "mem_pct":  psutil.virtual_memory().percent,
                "disk_pct": psutil.disk_usage("/").percent,
            },
        }
        return web.Response(
            content_type="application/json",
            text=json.dumps(payload, indent=2),
        )
    except Exception as e:
        return web.Response(status=500, text=str(e))


async def start_health_server() -> None:
    """Start the health check HTTP server. Non-blocking."""
    app = web.Application()
    app.router.add_get("/health",  health_handler)
    app.router.add_get("/metrics", metrics_handler)
    app.router.add_get("/",        health_handler)   # root alias

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", SYSTEM["health_check_port"])
    await site.start()
    log.info("Health server running on port %d", SYSTEM["health_check_port"])
