"""
blofin_client.py — Authenticated REST API client for BloFin.
Every request is signed per BloFin spec. Retry logic is built in.
Paper mode stubs order placement but reads live market data.
"""

import asyncio
import hashlib
import hmac
import base64
import logging
import time
import uuid
import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import (
    BLOFIN_API_KEY, BLOFIN_API_SECRET, BLOFIN_API_PASSPHRASE,
    BLOFIN_REST_URL, IS_PAPER, SYSTEM,
)

log = logging.getLogger("blofin_client")


# ── Auth ──────────────────────────────────────────────────────────────────────

import uuid as _uuid

def _sign(secret: str, path: str, method: str, timestamp: str,
          nonce: str, body: str = "") -> str:
    """
    BloFin signature spec:
      prehash = path + method + timestamp + nonce + body
      hex_sig  = HMAC-SHA256(secret, prehash).hexdigest()
      sign     = base64(hex_sig.encode())
    """
    prehash    = f"{path}{method}{timestamp}{nonce}{body}"
    hex_sig    = hmac.new(secret.encode(), prehash.encode(), hashlib.sha256).hexdigest()
    return base64.b64encode(hex_sig.encode()).decode()


def _auth_headers(method: str, path: str, body: str = "") -> dict:
    ts    = str(int(time.time() * 1000))
    nonce = str(_uuid.uuid4())
    return {
        "ACCESS-KEY":        BLOFIN_API_KEY,
        "ACCESS-SIGN":       _sign(BLOFIN_API_SECRET, path, method, ts, nonce, body),
        "ACCESS-TIMESTAMP":  ts,
        "ACCESS-NONCE":      nonce,
        "ACCESS-PASSPHRASE": BLOFIN_API_PASSPHRASE,
        "Content-Type":      "application/json",
    }


# ── Client ────────────────────────────────────────────────────────────────────

class BloFinClient:
    """
    Async REST client. Use as async context manager or call init()/close().
    All public methods raise BloFinAPIError on unrecoverable errors.
    """

    class BloFinAPIError(Exception):
        def __init__(self, code: int, msg: str, detail: dict = None):
            self.code = code
            self.msg  = msg
            self.detail = detail or {}
            super().__init__(f"[{code}] {msg}")

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.TCPConnector] = None

    async def init(self) -> None:
        self._connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=10, connect=5)
        self._session = aiohttp.ClientSession(
            connector=self._connector,
            timeout=timeout,
            base_url=BLOFIN_REST_URL,
        )
        log.info("BloFin REST client ready (mode=%s)", "PAPER" if IS_PAPER else "LIVE")

    async def close(self) -> None:
        if self._session:
            await self._session.close()
        if self._connector:
            await self._connector.close()
        log.info("BloFin REST client closed")

    async def __aenter__(self):
        await self.init()
        return self

    async def __aexit__(self, *_):
        await self.close()

    # ── Core request ─────────────────────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(SYSTEM["api_retry_attempts"]),
        wait=wait_exponential(multiplier=1, min=1, max=300),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True,
    )
    async def _request(self, method: str, path: str, params: dict = None,
                        body: dict = None, signed: bool = False) -> dict:
        try:
            # Build full path including query string — BloFin signs the full path
            if params:
                query_string = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
                full_path = f"{path}?{query_string}"
            else:
                full_path = path

            body_str = json.dumps(body) if body else ""
            headers  = _auth_headers(method, full_path, body_str) if signed else {"Content-Type": "application/json"}

            async with self._session.request(
                method, path,
                params=params,
                data=body_str if body_str else None,
                headers=headers,
            ) as resp:
                raw = await resp.json(content_type=None)

                if resp.status == 429:
                    log.warning("Rate limited — sleeping 2 s")
                    await asyncio.sleep(2)
                    raise aiohttp.ClientError("rate_limit")

                if resp.status >= 500:
                    raise aiohttp.ClientError(f"server_error_{resp.status}")

                # Some BloFin endpoints return a raw list with no code/data wrapper
                if isinstance(raw, list):
                    return raw

                code = raw.get("code", "0")
                if str(code) not in ("0", "200"):
                    raise self.BloFinAPIError(int(code), raw.get("msg", "unknown"), raw)

                return raw.get("data", raw)

        except self.BloFinAPIError:
            raise
        except Exception as e:
            log.warning("Request error %s %s: %s", method, path, e)
            raise

    # ── Market data ──────────────────────────────────────────────────────────

    async def get_ticker(self, inst_id: str) -> dict:
        """Current best bid/ask, last price, 24h volume."""
        data = await self._request("GET", "/api/v1/market/tickers",
                                    params={"instId": inst_id})
        return data[0] if isinstance(data, list) else data

    async def get_tickers(self, inst_type: str = "SWAP") -> List[dict]:
        """All tickers for given instrument type."""
        return await self._request("GET", "/api/v1/market/tickers",
                                    params={"instType": inst_type})

    async def get_orderbook(self, inst_id: str, depth: int = 20) -> dict:
        """Order book with bids and asks."""
        data = await self._request("GET", "/api/v1/market/books",
                                    params={"instId": inst_id, "sz": str(depth)})
        # BloFin returns data as a list — take first element
        return data[0] if isinstance(data, list) and data else (data or {})

    async def get_candles(self, inst_id: str, bar: str = "1H",
                           limit: int = 100) -> List[list]:
        """
        OHLCV candles. bar format: 1m, 5m, 15m, 1H, 4H, 1D
        Returns list of [ts, open, high, low, close, vol, volCcy]
        """
        data = await self._request(
            "GET", "/api/v1/market/candles",
            params={"instId": inst_id, "bar": bar, "limit": str(limit)},
        )
        return data if isinstance(data, list) else []

    async def get_funding_rate(self, inst_id: str) -> dict:
        """Current and next funding rate for a perpetual."""
        data = await self._request("GET", "/api/v1/public/funding-rate",
                                    params={"instId": inst_id})
        if isinstance(data, list):
            return data[0] if data else {}
        return data or {}

    async def get_mark_price(self, inst_id: str) -> float:
        data = await self._request("GET", "/api/v1/public/mark-price",
                                    params={"instId": inst_id, "instType": "SWAP"})
        rows = data if isinstance(data, list) else [data]
        for row in rows:
            if row.get("instId") == inst_id:
                return float(row.get("markPx", 0))
        return 0.0

    # ── Account ───────────────────────────────────────────────────────────────

    async def get_balance(self) -> dict:
        """Futures account balance — used for all trading operations."""
        # Trading section endpoint, not the spot account balance
        data = await self._request("GET", "/api/v1/account/balance", signed=True)
        if isinstance(data, list):
            return data[0] if data else {}
        return data or {}

    async def get_futures_balance(self) -> dict:
        """Alternative futures balance endpoint."""
        data = await self._request("GET", "/api/v1/account/futures-balance", signed=True)
        if isinstance(data, list):
            return data[0] if data else {}
        return data or {}

    async def get_usdt_balance(self) -> float:
        """Return available USDT equity for futures trading."""
        # Try futures-specific balance first, fall back to general balance
        try:
            bal = await self.get_futures_balance()
            if bal:
                details = bal.get("details", [])
                for item in details:
                    if item.get("currency") == "USDT":
                        return float(item.get("equity") or item.get("available") or 0)
                if bal.get("totalEquity"):
                    return float(bal["totalEquity"])
        except Exception:
            pass

        # Fall back to general account balance
        bal = await self.get_balance()
        details = bal.get("details", [])
        for item in details:
            if item.get("currency") == "USDT":
                val = float(item.get("equity") or item.get("available") or 0)
                log.info("USDT balance: %.2f", val)
                return val
        if bal.get("totalEquity"):
            return float(bal["totalEquity"])
        log.warning("Could not parse balance: %s", str(bal)[:300])
        return 0.0

    async def get_positions(self, inst_id: str = None) -> List[dict]:
        """All open positions on the account."""
        params = {"instType": "SWAP"}
        if inst_id:
            params["instId"] = inst_id
        data = await self._request("GET", "/api/v1/account/positions",
                                    params=params, signed=True)
        return data if isinstance(data, list) else []

    # ── Order management ──────────────────────────────────────────────────────

    async def place_order(self, inst_id: str, side: str, order_type: str,
                           size: float, price: float = None,
                           tp_price: float = None, sl_price: float = None,
                           reduce_only: bool = False,
                           client_order_id: str = None) -> dict:
        """
        Place a single order.
        side: "buy" | "sell"
        order_type: "limit" | "market"
        Returns full exchange response.
        """
        if client_order_id is None:
            client_order_id = f"bb_{uuid.uuid4().hex[:16]}"

        body: Dict[str, Any] = {
            "instId":     inst_id,
            "marginMode": "cross",
            "side":       side.lower(),
            "orderType":  order_type.lower(),
            "size":       str(round(size, 4)),      # BloFin: size not sz
            "clientOrderId": client_order_id,       # BloFin: clientOrderId not clOrdId
        }
        if price and order_type.lower() == "limit":
            body["price"] = str(round(price, 2))
        if reduce_only:
            body["reduceOnly"] = "true"
        if tp_price:
            body["tpTriggerPrice"] = str(round(tp_price, 2))   # BloFin field name
            body["tpOrderPrice"]   = "-1"
        if sl_price:
            body["slTriggerPrice"] = str(round(sl_price, 2))   # BloFin field name
            body["slOrderPrice"]   = "-1"

        # Both PAPER and LIVE send real orders — PAPER uses demo exchange URL
        log.info("PLACING ORDER body: %s", body)
        result = await self._request("POST", "/api/v1/trade/order",
                                      body=body, signed=True)
        mode = "DEMO" if IS_PAPER else "LIVE"
        log.info("[%s] ORDER PLACED: %s %s %s @ %s sz=%s -> %s",
                  mode, inst_id, side, order_type, price, size,
                  result.get("ordId") if isinstance(result, dict) else result)
        return result

    async def cancel_order(self, inst_id: str, order_id: str = None,
                            client_order_id: str = None) -> dict:
        body: Dict[str, Any] = {"instId": inst_id}
        if order_id:
            body["orderId"] = order_id
        elif client_order_id:
            body["clientOrderId"] = client_order_id
        else:
            raise ValueError("Must provide order_id or client_order_id")

        return await self._request("POST", "/api/v1/trade/cancel-order",
                                    body=body, signed=True)

    async def cancel_all_orders(self, inst_id: str) -> None:
        """Cancel every pending order for an instrument."""
        pending = await self.get_pending_orders(inst_id)
        if not pending:
            return
        tasks = [
            self.cancel_order(inst_id, order_id=o.get("orderId") or o.get("ordId"))
            for o in pending
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                log.warning("Cancel error: %s", r)

    async def get_pending_orders(self, inst_id: str = None) -> List[dict]:
        params = {"instType": "SWAP"}
        if inst_id:
            params["instId"] = inst_id
        data = await self._request("GET", "/api/v1/trade/orders-pending",
                                    params=params, signed=True)
        return data if isinstance(data, list) else []

    async def get_order(self, inst_id: str, order_id: str = None,
                         client_order_id: str = None) -> Optional[dict]:
        params = {"instId": inst_id}
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["clientOrderId"] = client_order_id
        data = await self._request("GET", "/api/v1/trade/order",
                                    params=params, signed=True)
        return data if isinstance(data, dict) else (data[0] if data else None)

    async def close_position(self, inst_id: str, side: str,
                              size: float) -> dict:
        """Close position at market. side = direction of existing position."""
        close_side = "sell" if side.lower() in ("long", "buy") else "buy"
        return await self.place_order(
            inst_id=inst_id,
            side=close_side,
            order_type="market",
            size=size,
            reduce_only=True,
        )

    # ── Set TP/SL on existing position (algo order) ───────────────────────────

    async def set_tp_sl(self, inst_id: str, pos_side: str,
                         tp_price: float = None, sl_price: float = None) -> dict:
        body: Dict[str, Any] = {
            "instId":     inst_id,
            "marginMode": "cross",
            "posSide":    pos_side,
        }
        if tp_price:
            body["tpTriggerPrice"] = str(round(tp_price, 2))
            body["tpOrderPrice"]   = "-1"
        if sl_price:
            body["slTriggerPrice"] = str(round(sl_price, 2))
            body["slOrderPrice"]   = "-1"

        return await self._request("POST", "/api/v1/trade/order-algo",
                                    body=body, signed=True)

    # ── Convenience: mid price ────────────────────────────────────────────────

    async def get_mid_price(self, inst_id: str) -> float:
        ob = await self.get_orderbook(inst_id, depth=1)
        bids = ob.get("bids", [[0]])
        asks = ob.get("asks", [[0]])
        best_bid = float(bids[0][0]) if bids else 0
        best_ask = float(asks[0][0]) if asks else 0
        return (best_bid + best_ask) / 2 if (best_bid and best_ask) else 0.0

    async def get_best_bid_ask(self, inst_id: str) -> tuple:
        ob = await self.get_orderbook(inst_id, depth=1)
        bids = ob.get("bids", [[0]])
        asks = ob.get("asks", [[0]])
        return (float(bids[0][0]) if bids else 0,
                float(asks[0][0]) if asks else 0)


# ── Singleton factory ─────────────────────────────────────────────────────────

_client: Optional[BloFinClient] = None


async def get_client() -> BloFinClient:
    global _client
    if _client is None:
        _client = BloFinClient()
        await _client.init()
    return _client
