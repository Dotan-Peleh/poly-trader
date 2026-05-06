# poly-trader

Binary-options trading bot for **Polymarket hourly BTC up/down markets**, focused on **late-window edge** — firing trades only in the last 1-15 minutes of each market when probability sharpens and the market hasn't fully repriced.

**Status:** Phase 0 scaffolding · **Owner:** dotan.spiegler@gmail.com · **Sister project:** [crypto-trader](https://github.com/Dotan-Peleh/crypto-trader)

---

## The strategy in one sentence

Price the binary option as a digital using current BTC, the hour's reference price, time-to-resolution, and recent realized vol — fire if our model probability differs from Polymarket's implied probability by ≥4%, sized at quarter-Kelly capped at 5% bankroll.

---

## Why this works (and where the edge is)

A Polymarket "Will BTC be up at 3pm ET?" binary resolves on whether the 3:00pm price exceeds the 2:00pm reference. As we approach close:

| Time to close | BTC at +0.4% — true P(YES wins) | Why |
|---|---|---|
| 60 min | ~60-65% | a -0.4% reversion in 1h is a normal move (~0.5% RV) |
| 15 min | ~78-82% | a -0.4% reversion in 15min is a 2σ event |
| 5 min | ~95-98% | a -0.4% reversion in 5min is a 4σ event |
| 1 min | ~99% | basically locked in |

**Market makers often use stale volatility estimates.** When per-1m realized vol drops below the 1h average, the implied probability lags the true probability — and that gap is our edge.

---

## The model (Black-Scholes for digital options)

```
move_needed   = (R − P) / P              R = reference price, P = current price
σ_per_min     = EWMA realized vol over last 30 minutes
σ_remaining   = σ_per_min × √(T − t)     T = resolution time, t = now (in minutes)
z             = move_needed / σ_remaining
P(YES wins)   = 1 − Φ(z)                 normal CDF

edge          = P(model) − P(implied, from Polymarket mid)

Fire YES if edge > +4% AND time_to_close > 60s
Fire NO  if edge < −4% AND time_to_close > 60s
Skip if |edge| < 4%
```

Sizing:
```
kelly_fraction = (p × b − q) / b        b = payout/risk ratio at current price
size = bankroll × min(kelly_fraction / 4, 0.05)
```

---

## Architecture (mirrors crypto-trader's patterns where they apply)

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          GCP project: crypto-agent-494710                    │
│                                                                              │
│  ┌─────────────────────────────────┐                                         │
│  │ VM poly-trader (us-central1)    │   ← single source of truth              │
│  │  • main.py (systemd)            │                                         │
│  │  • SQLite + claude_decisions    │                                         │
│  │  • Binance WS BTC tick stream   │                                         │
│  │  • Polymarket CLOB API client   │                                         │
│  └────────────┬────────────────────┘                                         │
│               │ DB → GCS, heartbeat                                          │
│               ▼                                                              │
│  ┌────────────────────────────────────┐    ┌──────────────────────────┐     │
│  │ GCS: poly-trader-backups (or       │    │ Cloud Function watchdog  │     │
│  │       crypto-trader-backups/poly/) │←───│  • polls heartbeat        │     │
│  │  • /live/poly_trader.db             │    │  • Telegram alert on stale│     │
│  │  • /halt.flag                       │───→│                          │     │
│  │  • /heartbeat.json                  │    └──────────────────────────┘     │
│  │  • /tick_data/yyyy-mm-dd/...        │                                     │
│  └────┬───────────────────────────┬────┘                                     │
│       │                           │                                          │
│       ▼                           ▼                                          │
│  ┌────────────────────────────────────────┐  ┌────────────────────────┐    │
│  │ EXISTING crypto-trader-dashboard       │  │ Telegram (shared bot)  │    │
│  │ (reused — saves a second Cloud Run)    │  │ messages prefixed with │    │
│  │  • Existing tabs: Overview, Trades, …  │  │ 🎲 [POLY] for the      │    │
│  │  • NEW: 🎲 Polymarket section          │  │ binary bot             │    │
│  │    reads gs://.../poly_live/poly.db    │  │                        │    │
│  │  • Single OAuth, single dashboard URL  │  │                        │    │
│  └────────────────────────────────────────┘  └────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Phase plan

| Phase | Days | Output | Risk |
|---|---|---|---|
| **0 — Repo + infra skeleton** | 1-2 | This commit, VM up, Cloud Run dashboard up, Telegram namespaced | $0 |
| **1 — Data feeds** | 2-4 | Binance WS tick recorder + Polymarket CLOB client + reference tracker | $0 |
| **2 — Pricer + paper firing** | 4-7 | Digital option pricer, paper trades logged with P&L vs reality | $0 |
| **3 — Claude gate** | 7-10 | Same Claude pattern as crypto-trader, learning loop on `claude_decisions` | $0 |
| **4 — Paper-only validation** | 10-24 | 14 days of paper trades (~80-120 fires) — check calibration vs hit rate | $0 |
| **5 — Live $100** | 24-30 | Polygon wallet seeded, real trades, micro-size only | $100 |

---

## Honest expectations

| Metric | Realistic |
|---|---|
| Late-window directional accuracy (T-5min) | 65-72% |
| Late-window directional accuracy (T-15min) | 58-62% |
| Breakeven (Polymarket 2% winnings fee + ~1.5% spread) | ~52% |
| P(profitable at 6 months, $100 capital) | 55-65% |
| Expected return if profitable | +20-50% over 6mo |
| Expected loss if unprofitable | -15-30% over 6mo |
| Per-trade size (Kelly/4) | 1-3% bankroll typical |

The late-window focus has materially better odds than full 5-min direction prediction (which is closer to 30-40% P(profitable)). Why: less noise, more deterministic outcomes, market makers slower to update vol assumptions.

---

## File structure

```
poly-trader/
├── README.md                          # this file
├── requirements.txt
├── main.py                            # APScheduler orchestrator
│
├── config/
│   └── settings.py                    # Pydantic config + live-mode safety gates
├── data/
│   ├── binance_ws.py                  # 1s BTC trade stream → SQLite
│   ├── polymarket_client.py           # CLOB API: markets, books, recent trades
│   ├── reference_tracker.py           # Resolve "ref price" per active market
│   └── storage.py                     # SQLAlchemy ORM
├── models/
│   ├── realized_vol.py                # EWMA vol estimator (5-min half-life)
│   └── digital_option.py              # CORE: P(YES wins) pricer
├── strategies/
│   └── late_window.py                 # The decision engine
├── claude_decision/
│   ├── decision.py                    # Claude gate (mirrors crypto-trader)
│   └── memory.py                      # decision log + weekly self-reflection
├── execution/
│   ├── polymarket_orders.py           # Paper sim + (Phase 5) live order signing
│   └── wallet.py                      # Bankroll + fee tracking
├── risk/
│   ├── position_sizer.py              # Kelly/4 + cap
│   └── portfolio.py                   # Circuit breaker + daily limit
├── monitor/
│   ├── notifier.py                    # Telegram (shared @Cryptotraderbotbot)
│   ├── heartbeat.py                   # GCS heartbeat (REST API)
│   └── halt_flag.py                   # GCS halt-flag reader
├── # NO local dashboard/ — UI lives in the existing crypto-trader repo
├── #   under a new 🎲 Polymarket section in dashboard/app.py.
├── #   poly-trader writes to gs://.../poly_live/poly.db; the existing
├── #   dashboard pulls + reads it alongside the crypto DB.
├── watchdog/
│   ├── main.py                        # Cloud Function — same pattern as crypto-trader
│   └── requirements.txt
├── scripts/
│   └── reset_paper_state.py
├── deploy/
│   ├── sync_db_to_gcs.sh              # Cron: DB → GCS every minute
│   └── backup_db.sh                   # Cron: daily snapshot
├── tests/
│   └── test_pricer.py                 # CORE tests for digital_option.py
└── docs/
    ├── ARCHITECTURE.md
    └── STRATEGY.md
```

---

## Cost (monthly)

| | Paper | Live |
|---|---|---|
| GCE VM (e2-micro, us-central1) | $5 | $5 |
| Cloud Run dashboard | **$0 — reuses existing crypto-trader-dashboard via new 🎲 Polymarket section** | $0 |
| Cloud Storage | $0.50 | $0.50 |
| Cloud Functions watchdog | $0 (free tier) | $0 |
| Cloud Scheduler | $0 (free tier) | $0 |
| Anthropic Claude API | ~$3-5 | ~$3-5 |
| Polymarket trading fees | $0 | ~$1-3 (2% on winnings only) |
| Polygon gas | $0 | ~$0.50-1 |
| **Total** | **~$9/mo** | **~$11-14/mo** |

---

## Differences from crypto-trader

| | crypto-trader | poly-trader |
|---|---|---|
| Trade type | spot long/short | binary YES/NO |
| Decision frequency | every 15 min | every 5 sec during last 15 min of each hourly market |
| Position duration | hours to days | minutes (always closes at HH:00 ET) |
| Sizing model | quarter-Kelly × ATR | quarter-Kelly × Bernoulli payout ratio |
| Stop-loss | 2× ATR | n/a (binary always settles) |
| Edge source | trend, pullback, funding harvest | implied-prob vs model-prob mispricing |
| Liquidity concern | KuCoin spreads | Polymarket order book depth ($1k-100k per market) |
| Settlement | KuCoin spot | Polygon USDC, oracle resolution |

---

## License

Private project. Not for redistribution.
