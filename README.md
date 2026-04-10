# BloFin 7-Bot Autonomous Trading System

**5,140 lines. 20 files. Zero human intervention required.**

## Architecture

```
main.py                  ← Master orchestrator
├── risk_engine.py       ← Universal gate (EVERY trade passes here)
├── recovery_manager.py  ← Crash recovery, API outage, watchdog
├── database.py          ← PostgreSQL state (source of truth)
├── blofin_client.py     ← Authenticated REST API
├── blofin_websocket.py  ← Real-time price feeds
├── position_tracker.py  ← Order → fill → position lifecycle
├── signal_engine.py     ← All technical indicators (no TA library)
├── alert_manager.py     ← Telegram + rotating log files
├── health_monitor.py    ← HTTP /health on port 8080
├── bot_base.py          ← Shared lifecycle for all bots
└── bots/
    ├── bot_funding_arb.py      # Bot 6 — Priority 2
    ├── bot_trend_follower.py   # Bot 2 — Priority 3
    ├── bot_breakout.py         # Bot 5 — Priority 4
    ├── bot_mean_reversion.py   # Bot 3 — Priority 5
    ├── bot_scalper.py          # Bot 1 — Priority 6
    ├── bot_market_maker.py     # Bot 4 — Priority 7
    └── bot_multi_momentum.py   # Bot 7 — Priority 8
```

## Deployment

### 1. Environment
```bash
cp .env.example .env
# Fill in: BLOFIN_API_KEY, BLOFIN_API_SECRET, BLOFIN_API_PASSPHRASE
# Fill in: DATABASE_URL (PostgreSQL)
# Fill in: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
# Set:     TRADING_MODE=paper  ← START HERE. Always.
```

### 2. Install
```bash
pip install -r requirements.txt
```

### 3. Run (paper mode first)
```bash
python main.py
```

### 4. Go live (only after dry-run validation)
```bash
# In .env: TRADING_MODE=live
python main.py
```

## Risk Rules (non-negotiable, enforced in code)
| Rule | Value |
|------|-------|
| Daily drawdown halt | 6% |
| Monthly drawdown halt | 15% |
| Max risk per trade | 2% of account |
| Max concurrent positions | 12 |
| Max per-bot allocation | 20% |
| Kelly fraction | 0.5 (half-Kelly) |
| Volatility kill switch | vol > 3× 7-day average |
| News blackout | ±5min pre / ±10min post macro event |

## Bot Priority (deploy in order, validate each before adding next)
1. **Universal Risk Engine** — already wired into every bot
2. **Bot 6: Funding Rate Arb** — passive yield, lowest risk
3. **Bot 2: Trend Follower** — simplest directional logic
4. **Bot 5: Breakout Hunter** — high-conviction setups
5. **Bot 3: Mean Reversion** — ranging markets only
6. **Bot 1: Momentum Scalper** — requires low latency
7. **Bot 4: Market Maker** — most complex, needs stable infra
8. **Bot 7: Multi-Momentum** — portfolio-level, daily rebalance

To deploy bots incrementally, set `"enabled": False` for any bot in config.py.

## Safety Systems
- **Crash recovery**: On restart, reconciles DB vs exchange positions. Exchange is truth.
- **API outage**: Detects 3 consecutive failures → pauses bots → alerts → auto-recovers
- **Stale bot detection**: Watchdog restarts any bot that stops heartbeating (90s timeout)
- **Emergency SL**: Any position found without a stop-loss gets a 3% emergency SL attached
- **Daily reset**: At 00:01 UTC, records new starting balance. Auto-resumes daily-drawdown halts.
- **Graceful shutdown**: SIGTERM/SIGINT triggers orderly stop of all bots before exit

## Health Check
```
GET http://localhost:8080/health   → 200 running / 503 halted
GET http://localhost:8080/metrics  → full system state JSON
```

## Monitoring
All trades and system events are sent to Telegram automatically:
- 🟢 Trade opened
- 🔴/🟢 Trade closed with P&L
- 🚨 System halted (drawdown, vol spike)
- ⚠️ Bot error, API outage
- 📅 Daily P&L summary at 23:55 UTC
