"""
blofin_websocket.py — Real-time WebSocket price feed from BloFin.

Subscribes to:
  - Ticker channel: best bid/ask, last price (used by scalper + MM)
  - Order updates: fill notifications for position tracking
  - Funding rate updates: used by Bot 6

Handles reconnection with exponential backoff automatically.
All messages are dispatched to registered callbacks.
"""

import asyncio
import hashlib
import hmac
import base64
import json
import logging
import time
from typing import Dict, Callable, Awaitable, List, Optional, Set

import websockets
from websockets.exceptions import ConnectionClosed

from config import (
    BLOFIN_API_KEY, BLOFIN_API_SECRET, BLOFIN_API_PASSPHRASE,
    BLOFIN_WS_PUBLIC, BLOFIN_WS_PRIVATE, SYSTEM,
)

log = logging.getLogger("websocket")

# Callback type: (channel, inst_id, data) -> None
WSCallback = Callable[[str, str, dict], Awaitable[None]]


def _ws_sign(secret: str) -> tuple:
    ts = str(int(time.time()))
    msg = ts + "GET" + "/users/self/verify"
    sig = base64.b64encode(
        hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()
    ).decode()
    return ts, sig


class BloFinWebSocket:
    """
    Manages public + private WebSocket connections.
    Auto-reconnects, auto-resubscribes after reconnect.
    """

    def __init__(self):
        self._public_ws:  Optional[websockets.WebSocketClientProtocol] = None
        self._private_ws: Optional[websockets.WebSocketClientProtocol] = None
        self._subscriptions: Set[str] = set()   # "channel:instId" strings
        self._callbacks: Dict[str, List[WSCallback]] = {}
        self._running = False
        self._ticker_cache: Dict[str, dict] = {}    # inst_id → latest ticker

    # ── Public API ────────────────────────────────────────────────────────────

    def on(self, channel: str, callback: WSCallback) -> None:
        """Register a callback for a channel (e.g., 'tickers', 'orders')."""
        self._callbacks.setdefault(channel, []).append(callback)

    async def subscribe_tickers(self, inst_ids: List[str]) -> None:
        for iid in inst_ids:
            self._subscriptions.add(f"tickers:{iid}")

    async def subscribe_orders(self) -> None:
        self._subscriptions.add("orders:SWAP")

    async def subscribe_funding(self, inst_ids: List[str]) -> None:
        for iid in inst_ids:
            self._subscriptions.add(f"funding-rate:{iid}")

    def get_ticker(self, inst_id: str) -> Optional[dict]:
        """Get latest cached ticker. Returns None if not yet received."""
        return self._ticker_cache.get(inst_id)

    def get_bid_ask(self, inst_id: str) -> tuple:
        ticker = self._ticker_cache.get(inst_id, {})
        return float(ticker.get("bidPx", 0)), float(ticker.get("askPx", 0))

    async def start(self) -> None:
        """Start both WebSocket connections as background tasks."""
        self._running = True
        asyncio.create_task(self._run_public())
        asyncio.create_task(self._run_private())
        log.info("WebSocket manager started")

    async def stop(self) -> None:
        self._running = False
        for ws in [self._public_ws, self._private_ws]:
            if ws:
                await ws.close()
        log.info("WebSocket manager stopped")

    # ── Public WebSocket (market data) ────────────────────────────────────────

    async def _run_public(self) -> None:
        backoff = SYSTEM["api_backoff_seconds"]
        attempt = 0
        while self._running:
            try:
                log.info("Public WS connecting...")
                async with websockets.connect(
                    BLOFIN_WS_PUBLIC,
                    ping_interval=SYSTEM["ws_ping_interval_seconds"],
                    ping_timeout=SYSTEM["ws_ping_timeout_seconds"],
                ) as ws:
                    self._public_ws = ws
                    attempt = 0
                    log.info("Public WS connected")

                    # Subscribe to all market channels
                    pub_subs = [s for s in self._subscriptions
                                 if not s.startswith("orders")]
                    if pub_subs:
                        await self._send_subscribe(ws, pub_subs)

                    async for raw in ws:
                        await self._handle_message(raw, "public")

            except ConnectionClosed as e:
                log.warning("Public WS closed: %s", e)
            except Exception as e:
                log.error("Public WS error: %s", e)

            if not self._running:
                break
            delay = backoff[min(attempt, len(backoff) - 1)]
            log.info("Public WS reconnecting in %ds (attempt %d)...", delay, attempt + 1)
            await asyncio.sleep(delay)
            attempt += 1

    # ── Private WebSocket (orders/fills) ─────────────────────────────────────

    async def _run_private(self) -> None:
        if not BLOFIN_API_KEY:
            log.info("Private WS skipped (no API key)")
            return

        backoff = SYSTEM["api_backoff_seconds"]
        attempt = 0
        while self._running:
            try:
                log.info("Private WS connecting...")
                async with websockets.connect(
                    BLOFIN_WS_PRIVATE,
                    ping_interval=SYSTEM["ws_ping_interval_seconds"],
                    ping_timeout=SYSTEM["ws_ping_timeout_seconds"],
                ) as ws:
                    self._private_ws = ws
                    attempt = 0

                    # Authenticate
                    ts, sig = _ws_sign(BLOFIN_API_SECRET)
                    await ws.send(json.dumps({
                        "op": "login",
                        "args": [{
                            "apiKey":     BLOFIN_API_KEY,
                            "passphrase": BLOFIN_API_PASSPHRASE,
                            "timestamp":  ts,
                            "sign":       sig,
                        }],
                    }))
                    # Wait for login confirmation
                    auth_resp = await asyncio.wait_for(ws.recv(), timeout=10)
                    log.debug("Private WS auth response: %s", auth_resp)

                    # Subscribe to private channels
                    priv_subs = [s for s in self._subscriptions
                                  if s.startswith("orders")]
                    if priv_subs:
                        await self._send_subscribe(ws, priv_subs)

                    log.info("Private WS authenticated and subscribed")

                    async for raw in ws:
                        await self._handle_message(raw, "private")

            except asyncio.TimeoutError:
                log.warning("Private WS auth timeout")
            except ConnectionClosed as e:
                log.warning("Private WS closed: %s", e)
            except Exception as e:
                log.error("Private WS error: %s", e)

            if not self._running:
                break
            delay = backoff[min(attempt, len(backoff) - 1)]
            await asyncio.sleep(delay)
            attempt += 1

    # ── Message handling ──────────────────────────────────────────────────────

    async def _handle_message(self, raw: str, source: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        if msg.get("event") in ("subscribe", "login", "error"):
            log.debug("WS event [%s]: %s", source, msg)
            return

        channel = msg.get("arg", {}).get("channel", "")
        inst_id = msg.get("arg", {}).get("instId", "")
        data    = msg.get("data", [])

        if not channel or not data:
            return

        # Cache tickers
        if channel == "tickers" and data:
            self._ticker_cache[inst_id] = data[0]

        # Dispatch to registered callbacks
        callbacks = self._callbacks.get(channel, [])
        for cb in callbacks:
            try:
                await cb(channel, inst_id, data[0] if data else {})
            except Exception as e:
                log.warning("WS callback error [%s]: %s", channel, e)

    async def _send_subscribe(self, ws, subscriptions: List[str]) -> None:
        args = []
        for sub in subscriptions:
            parts = sub.split(":", 1)
            channel = parts[0]
            inst_id = parts[1] if len(parts) > 1 else ""
            arg = {"channel": channel}
            if inst_id:
                arg["instId"] = inst_id
            args.append(arg)

        await ws.send(json.dumps({"op": "subscribe", "args": args}))
        log.info("WS subscribed: %s", [s for s in subscriptions])


# ── Singleton ─────────────────────────────────────────────────────────────────

_ws: Optional[BloFinWebSocket] = None


def get_ws() -> BloFinWebSocket:
    global _ws
    if _ws is None:
        _ws = BloFinWebSocket()
    return _ws
