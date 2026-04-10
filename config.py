"""
config.py — Central configuration for the BloFin 7-Bot Trading System.
All constants live here. Bots read from this module only — no hardcoded
values anywhere else in the codebase.
"""

import os
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# API CREDENTIALS
# ============================================================
BLOFIN_API_KEY        = os.getenv("BLOFIN_API_KEY", "")
BLOFIN_API_SECRET     = os.getenv("BLOFIN_API_SECRET", "")
BLOFIN_API_PASSPHRASE = os.getenv("BLOFIN_API_PASSPHRASE", "")

BLOFIN_REST_URL = "https://openapi.blofin.com"
BLOFIN_WS_PUBLIC  = "wss://openapi.blofin.com/ws/public"
BLOFIN_WS_PRIVATE = "wss://openapi.blofin.com/ws/private"

# ============================================================
# ENVIRONMENT
# ============================================================
TRADING_MODE = os.getenv("TRADING_MODE", "paper")   # "paper" | "live"
IS_LIVE      = TRADING_MODE == "live"
IS_PAPER     = not IS_LIVE

DATABASE_URL = os.getenv("DATABASE_URL", "")
REDIS_URL    = os.getenv("REDIS_URL", "redis://localhost:6379/0")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ============================================================
# UNIVERSAL RISK ENGINE — NON-NEGOTIABLE
# ============================================================
RISK = {
    "max_daily_drawdown_pct":    float(os.getenv("MAX_DAILY_DRAWDOWN_PCT",  "6.0")),
    "max_monthly_drawdown_pct":  15.0,
    "max_trade_risk_pct":        float(os.getenv("MAX_TRADE_RISK_PCT",      "2.0")),
    "max_concurrent_positions":  int(os.getenv("MAX_CONCURRENT_POSITIONS",  "12")),
    "max_bot_allocation_pct":    float(os.getenv("MAX_BOT_ALLOCATION_PCT",  "20.0")),
    "vol_kill_multiplier":       3.0,    # halt if 1h vol > 3× 7-day avg
    "vol_lookback_hours":        168,    # 7 days
    "news_blackout_minutes_pre": 5,
    "news_blackout_minutes_post":10,
    "correlation_max":           0.85,   # block new position if correlation > this
    "correlation_lookback_days": 30,
    "kelly_fraction":            0.5,    # half-Kelly always
    "min_edge_ratio":            2.0,    # min risk:reward to take a trade
}

# ============================================================
# ASSETS
# ============================================================
PRIMARY_ASSETS = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
QUOTE_CURRENCY = "USDT"

# Instrument type for BloFin perpetuals
INST_TYPE = "swap"   # perpetual futures

# ============================================================
# BOT 1 — MOMENTUM SCALPER
# ============================================================
BOT1 = {
    "id":              "bot1_scalper",
    "name":            "Momentum Scalper",
    "assets":          ["BTC-USDT", "ETH-USDT", "SOL-USDT"],
    "timeframe_signal":"5m",
    "timeframe_entry": "1m",
    "rsi_period":       14,
    "rsi_long_threshold":  55,
    "rsi_short_threshold": 45,
    "volume_multiplier":   1.5,       # vol must be > 1.5× 20-bar avg
    "volume_lookback":     20,
    "ema_period":          20,
    "spread_max_pct":      0.05,      # max spread % to enter
    "tp_pct":              0.50,      # 0.50% take profit
    "sl_pct":              0.25,      # 0.25% stop loss
    "time_exit_minutes":   8,         # exit if not TP'd within 8 min
    "max_trades_per_hour": 6,         # per asset
    "cooldown_seconds":    180,       # 3 min after any close
    "atr_period":          14,
    "atr_min_multiplier":  1.0,       # don't trade if ATR < 30-bar avg × this
    "atr_lookback":        30,
    "max_account_pct":     0.20,      # bot-level allocation cap
    "enabled":             True,
}

# ============================================================
# BOT 2 — TREND FOLLOWER
# ============================================================
BOT2 = {
    "id":              "bot2_trend",
    "name":            "Trend Follower",
    "assets":          ["BTC-USDT", "ETH-USDT", "SOL-USDT"],
    "timeframe_signal":"4h",
    "timeframe_trend": "1d",
    "ema_fast":         50,
    "ema_slow":         200,
    "ema_cross_lookback":5,          # cross must have happened in last 5 bars
    "adx_period":       14,
    "adx_min":          25,           # trend strength minimum
    "adx_full_kelly":   40,           # ADX above this = full Kelly
    "rsi_period":       14,
    "rsi_min":          45,
    "rsi_max":          65,           # don't enter if overbought/oversold
    "atr_period":       14,
    "trailing_atr_mult":2.0,          # trailing stop = 2× ATR
    "partial_exit_pct": 0.50,         # take 50% off at first target
    "first_target_pct": 4.0,          # 4% profit = partial exit
    "max_account_pct":  0.20,
    "no_weekend_below_vol_percentile": 50,   # don't trade weekends in low vol
    "enabled":          True,
}

# ============================================================
# BOT 3 — MEAN REVERSION
# ============================================================
BOT3 = {
    "id":              "bot3_meanrev",
    "name":            "Mean Reversion",
    "assets":          ["BTC-USDT", "ETH-USDT"],
    "timeframe_signal":"1h",
    "rsi_period":       14,
    "rsi_oversold":     28,
    "rsi_overbought":   72,
    "bb_period":        20,
    "bb_std":           2.0,
    "adx_period":       14,
    "adx_max":          20,           # ONLY trade when market is ranging (ADX < 20)
    "mean_period":      20,           # reversion target = 20-SMA
    "tp_reversion_pct": 0.50,         # TP at 50% reversion to mean
    "sl_pct":           1.50,         # 1.5% stop loss
    "confirm_bars":     1,            # wait for 1 bar back inside BB before entering
    "max_account_pct":  0.20,
    "enabled":          True,
}

# ============================================================
# BOT 4 — MARKET MAKER
# ============================================================
BOT4 = {
    "id":              "bot4_mm",
    "name":            "Market Maker",
    "assets":          ["BTC-USDT", "ETH-USDT"],
    "spread_target_pct":    0.06,     # quote ±0.06% from mid
    "quote_refresh_seconds":10,
    "inventory_max_pct":    0.50,     # max inventory imbalance (% of account)
    "skew_threshold_ratio": 3.0,      # skew if one side fills 3× more
    "skew_adjustment_pct":  0.03,     # skew by 0.03%
    "inventory_loss_max_pct":1.0,     # flatten if unrealized loss > 1%
    "pause_minutes_after_loss":30,
    "vol_max_1h_pct":       2.5,      # pause if 1h vol > 2.5%
    "max_account_pct":      0.20,
    "enabled":              True,
}

# ============================================================
# BOT 5 — BREAKOUT HUNTER
# ============================================================
BOT5 = {
    "id":              "bot5_breakout",
    "name":            "Breakout Hunter",
    "assets":          ["BTC-USDT", "ETH-USDT", "SOL-USDT"],
    "timeframe_signal":"1h",
    "timeframe_confirm":"4h",
    "consolidation_bars":    20,      # look back 20 bars
    "consolidation_range_pct":2.5,   # range must be < 2.5%
    "volume_multiplier":     2.0,     # breakout bar vol > 2× 20-bar avg
    "volume_lookback":       20,
    "retest_bars_max":       3,       # retest must come within 3 bars
    "add_on_retest_pct":     0.50,    # add 50% position on confirmed retest
    "tp_multiplier":         1.5,     # TP = 1.5× box height from breakout
    "sl_back_inside":        True,    # SL = back inside the box (invalidation)
    "max_boxes_monitored":   5,       # max simultaneous setups per asset
    "max_account_pct":       0.20,
    "enabled":               True,
}

# ============================================================
# BOT 6 — FUNDING RATE ARBITRAGE
# ============================================================
BOT6 = {
    "id":              "bot6_funding",
    "name":            "Funding Rate Arb",
    "assets":          ["BTC-USDT", "ETH-USDT", "SOL-USDT"],
    "funding_interval_hours": 8,
    "funding_entry_threshold":0.05,   # enter if 8h rate > +0.05%
    "funding_exit_threshold": 0.01,   # exit if rate falls below 0.01%
    "funding_exit_consecutive":2,     # AND has fallen for 2 consecutive periods
    "funding_negative_threshold":-0.03,  # reverse position if rate < -0.03%
    "min_funding_payments":   3,      # hold for at least 3 payments
    "basis_max_pct":          0.20,   # max spot-perp basis to enter
    "loss_vs_collected_max":  2.0,    # exit if loss > 2× collected funding
    "max_account_pct":        0.20,
    "enabled":                True,
}

# ============================================================
# BOT 7 — MULTI-ASSET MOMENTUM DIVERSIFICATION
# ============================================================
BOT7 = {
    "id":              "bot7_momentum",
    "name":            "Multi-Asset Momentum",
    "assets":          [],             # empty — universe built dynamically at runtime
    "universe_size":   10,            # top 10 assets by 30-day volume
    "momentum_period": 20,            # 20-day momentum score
    "atr_period":      20,
    "long_count":      3,             # go long top 3
    "short_count":     2,             # go short bottom 2
    "long_weight_pct": 6.0,           # 6% account each long
    "short_weight_pct":4.0,           # 4% account each short
    "rebalance_hour_utc":0,           # rebalance at 00:00 UTC
    "rebalance_min_change_pct":10.0,  # only rebalance if position would change >10%
    "correlation_max": 0.85,          # exclude if corr > 0.85 with another held asset
    "weekly_halt_drawdown_pct":5.0,   # halt for 72h if portfolio down 5% in a week
    "halt_hours":      72,
    "max_account_pct": 0.20,
    "enabled":         True,
}

# ============================================================
# SYSTEM SETTINGS
# ============================================================
SYSTEM = {
    "heartbeat_interval_seconds":  30,
    "watchdog_timeout_seconds":    120,    # 4× heartbeat interval — plenty of margin
    "api_retry_attempts":          9,
    "api_backoff_seconds":         [1, 2, 4, 8, 16, 32, 60, 120, 300],
    "ws_reconnect_delay_seconds":  5,
    "ws_ping_interval_seconds":    20,
    "ws_ping_timeout_seconds":     10,
    "position_reconcile_on_start": True,
    "state_save_interval_seconds": 10,
    "log_level":                   "INFO",
    "log_file":                    "blofin_bot.log",
    "health_check_port":           8080,   # HTTP health endpoint
}

# ============================================================
# LOGGING FORMAT
# ============================================================
LOG_FORMAT = "%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# ============================================================
# MACRO EVENT SCHEDULE (UTC) — extend this list
# These windows trigger the news blackout in the risk engine.
# Format: "YYYY-MM-DD HH:MM"
# In production replace with a live economic calendar API.
# ============================================================
KNOWN_MACRO_EVENTS: list[str] = [
    # Add scheduled events here — populated at runtime from calendar API
]
