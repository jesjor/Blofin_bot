# ============================================================
# BLOFIN TRADING SYSTEM — ENVIRONMENT CONFIGURATION
# Copy this to .env and fill in ALL values before running.
# NEVER commit .env to version control.
# ============================================================

# --- BloFin API ---
BLOFIN_API_KEY=your_api_key_here
BLOFIN_API_SECRET=your_api_secret_here
BLOFIN_API_PASSPHRASE=your_passphrase_here

# --- Database (PostgreSQL) ---
DATABASE_URL=postgresql://user:password@host:5432/blofin_bot

# --- Redis (state + locks) ---
REDIS_URL=redis://localhost:6379/0

# --- Telegram alerts ---
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# --- Environment ---
# Options: "paper" | "live"
TRADING_MODE=paper

# --- Risk overrides (optional, defaults in config.py apply if blank) ---
# MAX_DAILY_DRAWDOWN_PCT=6.0
# MAX_TRADE_RISK_PCT=2.0
# MAX_CONCURRENT_POSITIONS=12
# MAX_BOT_ALLOCATION_PCT=20.0

# --- News API (optional, for blackout logic) ---
# NEWSAPI_KEY=your_key_here
