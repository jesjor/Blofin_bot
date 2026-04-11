"""
database.py — PostgreSQL layer for the BloFin trading system.
Creates schema on first run, provides typed CRUD helpers used by every module.
The DB is the single source of truth for all state — the exchange is second.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import asyncpg
from config import DATABASE_URL

log = logging.getLogger("database")

# ── Schema ──────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
-- System-wide state and kill switches
CREATE TABLE IF NOT EXISTS system_state (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- One row per bot, tracks its current lifecycle state
CREATE TABLE IF NOT EXISTS bot_state (
    bot_id          TEXT PRIMARY KEY,
    status          TEXT NOT NULL DEFAULT 'IDLE',
    last_heartbeat  TIMESTAMPTZ DEFAULT NOW(),
    metadata        JSONB DEFAULT '{}'::jsonb,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Every order placed (open, filled, cancelled, rejected)
CREATE TABLE IF NOT EXISTS orders (
    id              BIGSERIAL PRIMARY KEY,
    order_id        TEXT UNIQUE,          -- exchange order ID
    client_order_id TEXT UNIQUE,          -- our internal ID
    bot_id          TEXT NOT NULL,
    inst_id         TEXT NOT NULL,
    side            TEXT NOT NULL,        -- BUY | SELL
    order_type      TEXT NOT NULL,        -- LIMIT | MARKET
    price           NUMERIC(20,8),
    size            NUMERIC(20,8) NOT NULL,
    filled_size     NUMERIC(20,8) DEFAULT 0,
    avg_fill_price  NUMERIC(20,8),
    status          TEXT NOT NULL DEFAULT 'PENDING',
    reduce_only     BOOLEAN DEFAULT FALSE,
    tp_price        NUMERIC(20,8),
    sl_price        NUMERIC(20,8),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Open and closed positions
CREATE TABLE IF NOT EXISTS positions (
    id              BIGSERIAL PRIMARY KEY,
    position_id     TEXT UNIQUE,          -- our internal position ID
    bot_id          TEXT NOT NULL,
    inst_id         TEXT NOT NULL,
    side            TEXT NOT NULL,        -- LONG | SHORT
    entry_price     NUMERIC(20,8) NOT NULL,
    current_price   NUMERIC(20,8),
    size            NUMERIC(20,8) NOT NULL,
    tp_price        NUMERIC(20,8),
    sl_price        NUMERIC(20,8),
    trailing_sl     BOOLEAN DEFAULT FALSE,
    trailing_atr_mult NUMERIC(6,3),
    highest_price   NUMERIC(20,8),        -- for trailing stops
    lowest_price    NUMERIC(20,8),
    unrealized_pnl  NUMERIC(20,8) DEFAULT 0,
    realized_pnl    NUMERIC(20,8),
    status          TEXT NOT NULL DEFAULT 'OPEN',  -- OPEN | CLOSED
    close_reason    TEXT,
    opened_at       TIMESTAMPTZ DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    metadata        JSONB DEFAULT '{}'::jsonb
);

-- Completed trades for analytics
CREATE TABLE IF NOT EXISTS trades (
    id              BIGSERIAL PRIMARY KEY,
    position_id     TEXT NOT NULL,
    bot_id          TEXT NOT NULL,
    inst_id         TEXT NOT NULL,
    side            TEXT NOT NULL,
    entry_price     NUMERIC(20,8) NOT NULL,
    exit_price      NUMERIC(20,8) NOT NULL,
    size            NUMERIC(20,8) NOT NULL,
    gross_pnl       NUMERIC(20,8) NOT NULL,
    fees            NUMERIC(20,8) DEFAULT 0,
    net_pnl         NUMERIC(20,8) NOT NULL,
    duration_seconds INT,
    close_reason    TEXT,
    exit_commentary TEXT,
    opened_at       TIMESTAMPTZ NOT NULL,
    closed_at       TIMESTAMPTZ NOT NULL
);

-- Risk engine event log
CREATE TABLE IF NOT EXISTS risk_events (
    id          BIGSERIAL PRIMARY KEY,
    event_type  TEXT NOT NULL,   -- HALT | RESUME | BLOCK | KILL_SWITCH
    reason      TEXT NOT NULL,
    bot_id      TEXT,            -- NULL = system-wide
    metadata    JSONB DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Funding rate data (Bot 6)
CREATE TABLE IF NOT EXISTS funding_rates (
    id          BIGSERIAL PRIMARY KEY,
    inst_id     TEXT NOT NULL,
    funding_rate NUMERIC(12,8) NOT NULL,
    next_funding_time TIMESTAMPTZ,
    collected   NUMERIC(20,8) DEFAULT 0,  -- cumulative collected this cycle
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- OHLCV price cache
CREATE TABLE IF NOT EXISTS price_candles (
    id          BIGSERIAL PRIMARY KEY,
    inst_id     TEXT NOT NULL,
    timeframe   TEXT NOT NULL,
    open_time   TIMESTAMPTZ NOT NULL,
    open        NUMERIC(20,8) NOT NULL,
    high        NUMERIC(20,8) NOT NULL,
    low         NUMERIC(20,8) NOT NULL,
    close       NUMERIC(20,8) NOT NULL,
    volume      NUMERIC(20,8) NOT NULL,
    UNIQUE(inst_id, timeframe, open_time)
);

-- Signal log (every signal generated, even filtered ones)
CREATE TABLE IF NOT EXISTS signals (
    id          BIGSERIAL PRIMARY KEY,
    bot_id      TEXT NOT NULL,
    inst_id     TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    direction   TEXT,           -- LONG | SHORT
    price       NUMERIC(20,8),
    metadata    JSONB DEFAULT '{}'::jsonb,
    passed_risk BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Daily P&L snapshot for drawdown tracking
CREATE TABLE IF NOT EXISTS daily_pnl (
    id              BIGSERIAL PRIMARY KEY,
    date            DATE UNIQUE NOT NULL,
    starting_balance NUMERIC(20,8) NOT NULL,
    ending_balance  NUMERIC(20,8),
    realized_pnl    NUMERIC(20,8) DEFAULT 0,
    unrealized_pnl  NUMERIC(20,8) DEFAULT 0,
    drawdown_pct    NUMERIC(8,4) DEFAULT 0,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for hot queries
CREATE INDEX IF NOT EXISTS idx_orders_bot_id     ON orders(bot_id);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders(status);
CREATE INDEX IF NOT EXISTS idx_positions_status  ON positions(status);
CREATE INDEX IF NOT EXISTS idx_positions_bot_id  ON positions(bot_id);
CREATE INDEX IF NOT EXISTS idx_candles_lookup    ON price_candles(inst_id, timeframe, open_time DESC);
CREATE INDEX IF NOT EXISTS idx_signals_bot       ON signals(bot_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_funding_inst      ON funding_rates(inst_id, created_at DESC);
"""

# ── Pool ─────────────────────────────────────────────────────────────────────

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
        log.info("DB pool created")
    return _pool


async def init_db() -> None:
    """Create all tables and indexes. Safe to call on every startup."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
    log.info("DB schema initialised")


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        log.info("DB pool closed")


# ── System state ─────────────────────────────────────────────────────────────

async def set_state(key: str, value: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO system_state(key, value, updated_at)
            VALUES($1, $2, NOW())
            ON CONFLICT(key) DO UPDATE SET value=$2, updated_at=NOW()
            """,
            key, value,
        )


async def get_state(key: str, default: str = "") -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM system_state WHERE key=$1", key)
        return row["value"] if row else default


async def get_all_states() -> Dict[str, str]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM system_state")
        return {r["key"]: r["value"] for r in rows}


# ── Bot state ─────────────────────────────────────────────────────────────────

async def upsert_bot_state(bot_id: str, status: str, metadata: dict = None) -> None:
    pool = await get_pool()
    import json
    meta_json = json.dumps(metadata or {})
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO bot_state(bot_id, status, last_heartbeat, metadata, updated_at)
            VALUES($1, $2, NOW(), $3::jsonb, NOW())
            ON CONFLICT(bot_id) DO UPDATE
            SET status=$2, last_heartbeat=NOW(), metadata=$3::jsonb, updated_at=NOW()
            """,
            bot_id, status, meta_json,
        )


async def heartbeat(bot_id: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE bot_state SET last_heartbeat=NOW() WHERE bot_id=$1",
            bot_id,
        )


async def get_stale_bots(timeout_seconds: int = 90) -> List[Dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT bot_id, status, last_heartbeat
            FROM bot_state
            WHERE status NOT IN ('HALTED', 'DISABLED', 'STOPPED')
              AND last_heartbeat < NOW() - INTERVAL '1 second' * $1
            """,
            timeout_seconds,
        )
        return [dict(r) for r in rows]


# ── Orders ────────────────────────────────────────────────────────────────────

async def insert_order(order: dict) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO orders(
                order_id, client_order_id, bot_id, inst_id, side, order_type,
                price, size, status, reduce_only, tp_price, sl_price
            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT(client_order_id) DO NOTHING
            """,
            order.get("order_id"), order["client_order_id"], order["bot_id"],
            order["inst_id"], order["side"], order["order_type"],
            order.get("price"), order["size"], order.get("status", "PENDING"),
            order.get("reduce_only", False), order.get("tp_price"), order.get("sl_price"),
        )


async def update_order(client_order_id: str, **kwargs) -> None:
    pool = await get_pool()
    if not kwargs:
        return
    sets = ", ".join(f"{k}=${i+2}" for i, k in enumerate(kwargs))
    vals = list(kwargs.values())
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE orders SET {sets}, updated_at=NOW() WHERE client_order_id=$1",
            client_order_id, *vals,
        )


async def get_open_orders(bot_id: str = None) -> List[Dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        if bot_id:
            rows = await conn.fetch(
                "SELECT * FROM orders WHERE bot_id=$1 AND status IN ('PENDING','OPEN','PARTIAL')",
                bot_id,
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM orders WHERE status IN ('PENDING','OPEN','PARTIAL')"
            )
        return [dict(r) for r in rows]


# ── Positions ─────────────────────────────────────────────────────────────────

async def insert_position(pos: dict) -> None:
    pool = await get_pool()
    import json
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO positions(
                position_id, bot_id, inst_id, side, entry_price, size,
                tp_price, sl_price, trailing_sl, trailing_atr_mult,
                highest_price, lowest_price, metadata
            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb)
            ON CONFLICT(position_id) DO NOTHING
            """,
            pos["position_id"], pos["bot_id"], pos["inst_id"], pos["side"],
            pos["entry_price"], pos["size"], pos.get("tp_price"), pos.get("sl_price"),
            pos.get("trailing_sl", False), pos.get("trailing_atr_mult"),
            pos.get("highest_price", pos["entry_price"]),
            pos.get("lowest_price", pos["entry_price"]),
            json.dumps(pos.get("metadata", {})),
        )


async def update_position(position_id: str, **kwargs) -> None:
    pool = await get_pool()
    if not kwargs:
        return
    sets = ", ".join(f"{k}=${i+2}" for i, k in enumerate(kwargs))
    vals = list(kwargs.values())
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE positions SET {sets} WHERE position_id=$1",
            position_id, *vals,
        )


async def get_open_positions(bot_id: str = None) -> List[Dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        if bot_id:
            rows = await conn.fetch(
                "SELECT * FROM positions WHERE bot_id=$1 AND status='OPEN'", bot_id
            )
        else:
            rows = await conn.fetch("SELECT * FROM positions WHERE status='OPEN'")
        return [dict(r) for r in rows]


async def close_position(position_id: str, exit_price: float,
                          reason: str, realized_pnl: float) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE positions
            SET status='CLOSED', current_price=$2, realized_pnl=$3,
                close_reason=$4, closed_at=NOW()
            WHERE position_id=$1
            """,
            position_id, exit_price, realized_pnl, reason,
        )


# ── Trades ────────────────────────────────────────────────────────────────────

async def insert_trade(trade: dict) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO trades(
                position_id, bot_id, inst_id, side, entry_price, exit_price,
                size, gross_pnl, fees, net_pnl, duration_seconds, close_reason,
                exit_commentary, opened_at, closed_at
            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
            """,
            trade["position_id"], trade["bot_id"], trade["inst_id"], trade["side"],
            trade["entry_price"], trade["exit_price"], trade["size"],
            trade["gross_pnl"], trade.get("fees", 0), trade["net_pnl"],
            trade.get("duration_seconds"), trade.get("close_reason"),
            trade.get("exit_commentary", ""),
            trade["opened_at"], trade["closed_at"],
        )


# ── Risk events ───────────────────────────────────────────────────────────────

async def log_risk_event(event_type: str, reason: str, bot_id: str = None,
                          metadata: dict = None) -> None:
    pool = await get_pool()
    import json
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO risk_events(event_type, reason, bot_id, metadata)
            VALUES($1,$2,$3,$4::jsonb)
            """,
            event_type, reason, bot_id, json.dumps(metadata or {}),
        )


# ── Daily P&L ─────────────────────────────────────────────────────────────────

async def upsert_daily_pnl(date, starting_balance: float,
                            realized_pnl: float = 0,
                            unrealized_pnl: float = 0,
                            drawdown_pct: float = 0) -> None:
    pool = await get_pool()
    # asyncpg needs a datetime.date object, not a string
    from datetime import date as date_type
    if isinstance(date, str):
        from datetime import datetime
        date = datetime.strptime(date, "%Y-%m-%d").date()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO daily_pnl(date, starting_balance, realized_pnl, unrealized_pnl, drawdown_pct)
            VALUES($1::date, $2, $3, $4, $5)
            ON CONFLICT(date) DO UPDATE
            SET realized_pnl=$3, unrealized_pnl=$4, drawdown_pct=$5,
                ending_balance=$2+$3, updated_at=NOW()
            """,
            date, starting_balance, realized_pnl, unrealized_pnl, drawdown_pct,
        )


async def get_today_pnl() -> Optional[Dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM daily_pnl WHERE date=CURRENT_DATE"
        )
        return dict(row) if row else None


async def get_monthly_pnl() -> float:
    """Sum of all realized + unrealized P&L this calendar month."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT COALESCE(SUM(realized_pnl + unrealized_pnl), 0) AS total
            FROM daily_pnl
            WHERE date >= DATE_TRUNC('month', CURRENT_DATE)
            """
        )
        return float(row["total"]) if row else 0.0


# ── Price candles ─────────────────────────────────────────────────────────────

async def upsert_candles(candles: List[Dict]) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO price_candles(inst_id, timeframe, open_time, open, high, low, close, volume)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT(inst_id, timeframe, open_time) DO UPDATE
            SET high=$5, low=$6, close=$7, volume=$8
            """,
            [(c["inst_id"], c["timeframe"], c["open_time"],
              c["open"], c["high"], c["low"], c["close"], c["volume"])
             for c in candles],
        )


async def get_candles(inst_id: str, timeframe: str, limit: int = 200) -> List[Dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM price_candles
            WHERE inst_id=$1 AND timeframe=$2
            ORDER BY open_time DESC
            LIMIT $3
            """,
            inst_id, timeframe, limit,
        )
        return [dict(r) for r in reversed(rows)]


# ── Signals ───────────────────────────────────────────────────────────────────

async def log_signal(bot_id: str, inst_id: str, signal_type: str,
                      direction: str = None, price: float = None,
                      passed_risk: bool = False, metadata: dict = None) -> None:
    pool = await get_pool()
    import json
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO signals(bot_id, inst_id, signal_type, direction, price, passed_risk, metadata)
            VALUES($1,$2,$3,$4,$5,$6,$7::jsonb)
            """,
            bot_id, inst_id, signal_type, direction, price,
            passed_risk, json.dumps(metadata or {}),
        )


# ── Funding rates ─────────────────────────────────────────────────────────────

async def insert_funding_rate(inst_id: str, rate: float,
                               next_funding_time: datetime = None) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO funding_rates(inst_id, funding_rate, next_funding_time)
            VALUES($1,$2,$3)
            """,
            inst_id, rate, next_funding_time,
        )


async def get_latest_funding_rate(inst_id: str) -> Optional[Dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT * FROM funding_rates
            WHERE inst_id=$1
            ORDER BY created_at DESC LIMIT 1
            """,
            inst_id,
        )
        return dict(row) if row else None


# ── Dashboard queries ─────────────────────────────────────────────────────────

async def get_recent_trades(limit: int = 100) -> List[Dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT t.*, p.metadata as pos_metadata
            FROM trades t
            LEFT JOIN positions p ON p.position_id = t.position_id
            ORDER BY t.closed_at DESC
            LIMIT $1
            """,
            limit,
        )
        return [dict(r) for r in rows]


async def get_pnl_timeseries(days: int = 30) -> List[Dict]:
    """Daily P&L for charting."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT date, starting_balance, realized_pnl,
                   unrealized_pnl, drawdown_pct
            FROM daily_pnl
            WHERE date >= CURRENT_DATE - INTERVAL '1 day' * $1
            ORDER BY date ASC
            """,
            days,
        )
        return [dict(r) for r in rows]


async def get_signals_recent(limit: int = 200) -> List[Dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM signals
            ORDER BY created_at DESC
            LIMIT $1
            """,
            limit,
        )
        return [dict(r) for r in rows]


async def get_bot_stats() -> List[Dict]:
    """Per-bot trade statistics."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                bot_id,
                COUNT(*) as total_trades,
                SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN net_pnl <= 0 THEN 1 ELSE 0 END) as losses,
                COALESCE(SUM(net_pnl), 0) as total_pnl,
                COALESCE(AVG(net_pnl), 0) as avg_pnl,
                COALESCE(AVG(duration_seconds), 0) as avg_duration_s
            FROM trades
            GROUP BY bot_id
            ORDER BY total_pnl DESC
            """
        )
        return [dict(r) for r in rows]
