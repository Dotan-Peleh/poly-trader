# poly-trader Infrastructure

Complete operational reference for the smart-money copy-trading bot on Polymarket.

---

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     EXTERNAL DATA SOURCES                            │
│                                                                       │
│  Polymarket APIs              Polygon Chain          Anthropic API   │
│  ┌─────────────┐              ┌─────────────┐        ┌─────────────┐│
│  │ Gamma       │              │ Alchemy RPC │        │ Claude      ││
│  │ Data API    │              │ WebSocket   │        │ Haiku 4.5   ││
│  │ CLOB API    │              │ (logs)      │        │             ││
│  │ Leaderboard │              └─────────────┘        └─────────────┘│
│  └─────────────┘                                                     │
└────────────────────┬─────────────────┬──────────────────┬────────────┘
                     │                 │                  │
                     ▼                 ▼                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  VM: crypto-trader (europe-west1-b)                  │
│                  e2-medium · 4 GB RAM · 2 vCPU · 50 GB disk         │
│                                                                       │
│  ┌──────────────────────┐    ┌────────────────────────────────────┐ │
│  │  poly-trader.service │    │  crypto-trader.service             │ │
│  │  (this repo)          │    │  (sister bot, separate codebase)  │ │
│  │                       │    │                                     │ │
│  │  • smart_money copy   │    │  • Binance/KuCoin spot algo        │ │
│  │  • polygon_stream     │    │  • cash_and_carry, hot_movers,     │ │
│  │  • firehose ingest    │    │    simple_continuation, etc.        │ │
│  │  • exit_manager       │    └────────────────────────────────────┘ │
│  │  • claude gate        │                                           │
│  │  • twice-daily summary│    ┌────────────────────────────────────┐ │
│  └──────────────────────┘    │  crypto-trader-watchdog.service    │ │
│                               │  (sidecar — polls /health, kicks   │ │
│  Memory caps via systemd:    │   any unresponsive bot every 10m)   │ │
│    MemoryMax=550M each       └────────────────────────────────────┘ │
│    Auto-restart on death                                              │
│    RuntimeMaxSec=86400 (daily flush)                                  │
└─────────────────────┬────────────────────────────────────────────────┘
                      │ minutely sync via cron + sync_db_to_gcs.sh
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│              GCS: gs://crypto-trader-backups-494710                   │
│                                                                       │
│   /heartbeat.json              ← crypto-trader liveness               │
│   /poly_live/heartbeat.json    ← poly-trader liveness                 │
│   /poly_live/poly.db           ← SQLite snapshot (~100 MB after prune)│
│   /poly_live/wallet.json       ← real Polymarket wallet state         │
│   /poly_live/mode.txt          ← runtime live/paper flag              │
│   /live/crypto_trader.db       ← crypto-trader DB snapshot            │
└─────────────────────┬───────────────────────────┬────────────────────┘
                      │ HTTP poll on demand        │ scheduled poll
                      ▼                            ▼
┌──────────────────────────────────────┐  ┌──────────────────────────┐
│  Cloud Run: crypto-trader-dashboard  │  │  Cloud Function:         │
│  (us-central1)                       │  │  bot-watchdog            │
│                                       │  │  (us-central1)           │
│  • Streamlit app                     │  │                          │
│  • IAP-protected                     │  │  Every 10 min:           │
│  • Tabs: Crypto, Polymarket, ...     │  │  • Read heartbeat.json   │
│  • Reads poly.db from GCS, caches    │  │  • If > 30 min stale,    │
│    in /tmp, refreshes on TTL         │  │    Telegram DOWN alert   │
│  • Live whale scanner, Claude gate   │  │  • On recovery, BACK UP  │
│    metrics, per-whale P&L            │  │                          │
└──────────────────────────────────────┘  └──────────────────────────┘
                                                       │
                                                       ▼
                                          ┌──────────────────────────┐
                                          │  Telegram bot            │
                                          │  • COPY (buy) alerts     │
                                          │  • CLOSE (sell) alerts   │
                                          │  • Cap-reached alerts    │
                                          │  • Twice-daily summaries │
                                          │  • Down/recovery alerts  │
                                          └──────────────────────────┘
```

---

## 2. Strategy Architecture

The bot runs three strategies side-by-side, tagged in `Decision.notes` for A/B comparison:

### 2.1 smart_copy (primary, active)

The actively-maintained strategy. Phase-by-phase rollout:

| Phase | Component | What it does |
|-------|-----------|--------------|
| 1 | `data/firehose.py` | REST polls `data-api.polymarket.com/trades?limit=500&ascending=false` every 60 sec. Captures every Polymarket trade. Discovers wallet universe. |
| 2 | `strategies/smart_money.py:refresh_smart_wallets()` | Every 12 h: pulls `lb-api.polymarket.com/profit?window=All&limit=500`, filters to wallets active in last 14 days with ≥$2M lifetime P&L, ranks by composite score (lifetime × log(volume)). Stores in `smart_wallet_rankings`. |
| 2.5 | `data/polygon_stream.py` | Subscribes via Alchemy WebSocket to `ConditionalTokens.TransferSingle` events. **Server-side filter** by `topics[3] = [tracked wallet addresses]` so we only pay Alchemy CU for events involving our whales. |
| 3 | `strategies/smart_money.py:execute_copy_trades()` | On each tracked-wallet entry signal: passes through mechanical gates (book quality, edge, depth, price drift, stale entry), computes quality score, checks adaptive threshold + daily cap, asks Claude for final verdict, fires paper trade. |
| 4 | `strategies/smart_money.py:execute_copy_exits()` | When tracked wallet reduces position ≥30%, closes our matching copy at current book bid. |

#### Gates pipeline

```
on-chain TransferSingle event
        │
        ▼
[1] match against smart_wallet_rankings  ← server-side Alchemy filter
        │
        ▼
[2] mechanical filters:
    • Market exists in our DB (auto-add via gamma if not)
    • Resolves > 2 min from now
    • Book sum yes_ask + no_ask < 1.05 (not placeholder)
    • Whale's entry not stale (last fill < 15 min ago)
    • Price drift |our_ask - smart_avg| < 0.25
    • Tail filter: 0.05 ≤ our_ask ≤ 0.95
    • Idempotency: no existing open copy on this market
    • Portfolio gate: < 10 concurrent opens
    • Edge in [4%, 10%]
        │
        ▼
[3] quality score:
    score = log(1+lifetime_pnl) × edge_pct × (1+log(1+size)) × (1-tail_pen/2)
        │
        ▼
[4] adaptive threshold:
    today_count / 200 → threshold rises from 50 → 250
    if today_count ≥ DAILY_COPY_CAP=200: REJECT
    if score < threshold: REJECT
        │
        ▼
[5] Claude gate (Haiku 4.5):
    Sees whale stats, trade, today's cap progress, 7-day calibration.
    Default APPROVE; REJECT only on structural issues mechanical gates miss.
        │
        ▼
fire paper trade, tag:
    notes = "smart_copy:<NAME>|<addr>|score=N|claude=APPROVE|conf=X"
```

### 2.2 v2_meanrev (legacy, still running)

Pre-window vol-based strategy on BTC 5-min binaries. Documented but largely
superseded by smart_copy. Tag: `v2_meanrev` (or `v2_meanrev_inferred` if
early-exit overwrote the tag).

### 2.3 v1 (legacy, frozen)

Original momentum-following BTC binary strategy. No new fires; kept tagged
for historical A/B comparison.

---

## 3. Component Inventory

### 3.1 In this repo

| File | Purpose |
|------|---------|
| `main.py` | Service bootstrap, scheduler with all jobs (decision_tick, exit_manager_tick, firehose ingest/rank/prune, smart_money refresh/poll, summary cron, polygon_stream init) |
| `config/settings.py` | All tunables + Secret Manager loading |
| `data/firehose.py` | REST-polled trade firehose + wallet ranking + DB pruning |
| `data/polygon_stream.py` | Polygon WebSocket subscriber for real-time wallet hits |
| `data/polymarket_client.py` | Gamma/CLOB API wrappers, book fetching, `track_market_by_condition_id` |
| `data/storage.py` | SQLAlchemy ORM (Decision, PolymarketMarket, PolymarketBookSnapshot, BtcTick) |
| `strategies/smart_money.py` | Phases 1-4 of copy strategy (rank, poll, copy, exit) |
| `strategies/pre_window.py` | Legacy v2_meanrev strategy |
| `strategies/late_window.py` | Legacy v1 strategy |
| `strategies/exit_manager.py` | Early-exit logic (TP/SL/time-bailout) |
| `strategies/summary.py` | Twice-daily Telegram summary with learning-loop breakdowns |
| `claude_decision/smart_money_gate.py` | Smart-money-specific Claude prompt + parser |
| `claude_decision/decision.py` | Legacy vol-strategy Claude prompt |
| `execution/wallet.py` | Paper bankroll, `record_paper_trade`, `settle_paper_trade` |
| `execution/portfolio_guard.py` (`risk/portfolio.py`) | Concurrent-trade cap, daily circuit breaker |
| `risk/position_sizer.py` | Quarter-Kelly sizing |
| `monitor/notifier.py` | Telegram bot wrapper |
| `monitor/health.py` | HTTP /health endpoint + dead-man watcher thread |
| `deploy/hardening.conf` | systemd drop-in: MemoryMax=550M, RuntimeMaxSec=86400, Restart=always |
| `deploy/crypto-trader-watchdog.sh` | Sidecar watchdog that kicks unresponsive bots |
| `deploy/crypto-trader-watchdog.service` | systemd unit for the sidecar |

### 3.2 On the VM (not in repo)

| Path | Purpose |
|------|---------|
| `/etc/systemd/system/poly-trader.service` | Main poly-trader unit |
| `/etc/systemd/system/poly-trader.service.d/override.conf` | Env vars (NO_PROXY, TRADING_MODE) |
| `/etc/systemd/system/poly-trader.service.d/hardening.conf` | Memory caps + auto-restart |
| `/etc/systemd/system/crypto-trader.service` | Sister bot |
| `/etc/systemd/system/crypto-trader-watchdog.service` | Sidecar watchdog |
| `/usr/local/bin/crypto-trader-watchdog.sh` | Watchdog script |
| `/home/dotanwork/poly-trader/poly_trader.db` | Live SQLite database |
| `/home/dotanwork/poly-trader/var/smart_wallets.json` | Seeded leaderboard whales |
| `/home/dotanwork/poly-trader/var/smart_wallet_positions.json` | Per-wallet position snapshot cache for delta detection |
| `crontab -e` for user `dotanwork`: `* * * * * /home/dotanwork/crypto-trader/deploy/sync_db_to_gcs.sh` | DB → GCS sync, every minute |

### 3.3 GCP services

| Service | Region | Purpose |
|---------|--------|---------|
| `crypto-trader` VM | europe-west1-b | All bot processes |
| `crypto-trader-backups-494710` GCS bucket | multi-region | DB snapshots, heartbeats, wallet state |
| `crypto-trader-dashboard` Cloud Run | us-central1 | Streamlit dashboard |
| `bot-watchdog` Cloud Function (v2) | us-central1 | Heartbeat watchdog → Telegram |
| `bot-watchdog-cron` Cloud Scheduler | us-central1 | Triggers watchdog every 10 min |
| Secret Manager secrets | global | API keys |

### 3.4 Secrets in Secret Manager (`crypto-agent-494710`)

| Secret name | What |
|-------------|------|
| `polygon-alchemy-key` | Alchemy Polygon RPC API key |
| `anthropic-api-key` | Claude API key |
| `telegram-bot-token` | Telegram bot auth |
| `telegram-chat-id` | Target chat for alerts |
| `polymarket-funder` | Polymarket proxy wallet address |
| `polymarket-pk` | Private key (live mode signing) |
| `polymarket-api-key` / `polymarket-api-secret` / `polymarket-api-passphrase` | Polymarket CLOB credentials |

All loaded automatically by `config/settings.py` at bot startup if env var isn't already set.

---

## 4. Data Model

### 4.1 Tables in `poly_trader.db`

#### `decisions`
The primary fact table for every paper/live trade we make.

| Column | Type | Purpose |
|--------|------|---------|
| `id` | INTEGER PK | |
| `ts` | DATETIME | When we fired |
| `condition_id` | VARCHAR(80) | Market identifier |
| `side` | VARCHAR(8) | 'YES' or 'NO' |
| `paid_per_unit` | FLOAT | Our entry price (0..1) |
| `size_usd` | FLOAT | Dollar amount staked |
| `units_bought` | FLOAT | Tokens received |
| `model_yes_prob` | FLOAT | Our model's estimate |
| `implied_yes_prob` | FLOAT | Market price at entry |
| `edge` | FLOAT | model - implied |
| `resolution_yes` | INTEGER NULL | 1=YES won, 0=NO won, NULL=unresolved |
| `pnl_usd` | FLOAT NULL | Realized P&L (filled on resolution) |
| `resolved_at` | DATETIME NULL | When the trade closed |
| `mode` | VARCHAR(8) | 'paper' or 'live' |
| `notes` | TEXT | **Strategy tag + learning-loop fields** |

#### `notes` field format
This is the key field for the learning loop. Multiple formats coexist:

| Pattern | Meaning |
|---------|---------|
| `v2_meanrev` | Pre-window strategy fire, resolved by market |
| `smart_copy:NAME|addr|score=N|claude=APPROVE|conf=0.X` | Smart-money copy, all metadata |
| `early_exit:tp_1.81x|smart_copy:NAME|...` | exit_manager closed before resolution — prefixed; original tag preserved |
| `smart_exit:NAME_30pct|smart_copy:NAME|...` | Closed because tracked whale reduced position |
| `<anything>|swept_for_500usd_reset` | Manually swept (paper hygiene) |

#### `polymarket_markets`
Markets we know about. `track_market_by_condition_id` auto-adds new ones from smart-money signals.

#### `polymarket_book_snapshots`
Order book state at sample times. Pruned to last 24h hourly.

#### `btc_ticks`
BTC price stream from Binance. Pruned to last 6h hourly (only the recent window is needed for vol estimation).

#### `trades_firehose`
Every trade pulled from Polymarket's `/trades` endpoint. Pruned to last 24h.

#### `smart_wallet_rankings`
The currently-tracked smart-money roster. Rewritten every 15 min by the rank job.

#### `polygon_stream_hits`
Last 500 on-chain trades by tracked wallets. Powers the dashboard's live scanner tab.

### 4.2 Pruning policy

| Table | Retention | Trigger |
|-------|-----------|---------|
| `trades_firehose` | 24 h | Hourly cron via `_firehose_prune` |
| `btc_ticks` | 6 h | Hourly cron |
| `polymarket_book_snapshots` | 24 h | Hourly cron |
| `polygon_stream_hits` | Last 500 rows | Hourly cron |
| `decisions` | Forever | — |
| `polymarket_markets` | Forever (small) | — |
| `smart_wallet_rankings` | Replaced every 15 min | Auto |

After delete: `VACUUM` runs to reclaim space. **DB must be ≤ ~200 MB or the 1-min GCS sync fails.**

---

## 5. Scheduler Jobs

All run via APScheduler in `main.py:main()`. Times are UTC.

| Job | Interval | Function | Purpose |
|-----|----------|----------|---------|
| `polymarket_refresh` | 2 min | `polymarket_refresh_tick` | Pull active 5-min markets from gamma API |
| `polymarket_book` | 20 sec | `polymarket_book_tick` | Snapshot order books for markets resolving in next 35 min |
| `decision_tick` | 5 sec | `decision_tick` | Run pre_window strategy (legacy v2) |
| `exit_manager_tick` | 10 sec | `exit_manager_tick` | Check open positions for early exit |
| `settle_tick` | 60 sec | `settle_tick` | Mark resolved markets and compute P&L |
| `wallet_snapshot_tick` | 30 sec | `wallet_snapshot_tick` | Pull real Polymarket wallet state → GCS |
| `heartbeat_tick` | 5 min | `heartbeat_tick` | Write liveness file to GCS for watchdog |
| `smart_money_refresh` | every 12 h at :05 | `smart_money.refresh_tick` | Re-rank smart wallets from leaderboard |
| `smart_money_poll` | 30 sec | `smart_money.poll_tick` | Detect new opens / exits via /positions API |
| `firehose_ingest` | 60 sec | `firehose.ingest_tick` | Capture all Polymarket trades |
| `firehose_rank` | every 15 min | `firehose.rank_wallets_from_firehose` | Update wallet rankings from firehose data |
| `firehose_prune` | every hour at :07 | `firehose.prune_old_rows` | Trim old data + VACUUM |
| `poly_summary_morning` | 08:00 UTC | `summary.poly_summary` | Telegram morning summary |
| `poly_summary_evening` | 20:00 UTC | `summary.poly_summary` | Telegram evening summary |

The `polygon_stream` WebSocket runs in its own daemon thread (started during `main()`), not via the scheduler.

---

## 6. Cost Structure

Current monthly burn (paper-mode, 200 copies/day cap):

| Service | Plan | Monthly | Notes |
|---------|------|---------|-------|
| GCE e2-medium 24/7 | sustained-use | ~$24 | 4 GB / 2 vCPU |
| GCS storage | standard | ~$2 | ~3 GB of snapshots |
| Cloud Run dashboard | request-based | $1-3 | Scales to zero when not used |
| Cloud Functions (watchdog) | free tier | $0 | 6 invocations/hour × 30 days |
| Cloud Scheduler | free tier | $0 | 3 jobs |
| Secret Manager | free tier | $0 | < 10k accesses/mo |
| Alchemy Polygon RPC | Pay As You Go | $5-15 | With server-side filter on ~15 tracked wallets |
| Anthropic Claude (Haiku 4.5) | Pay As You Go | ~$30 max | 200 copies/day × $0.005/call |
| Polymarket APIs | free | $0 | gamma, data, CLOB endpoints all public |
| Telegram Bot API | free | $0 | |
| **Total** | | **~$60-75/mo** | |

### Cost optimization principles applied
1. **Server-side filtering on Alchemy** — `topics[3] IN [our wallets]` cuts event delivery by ~99%
2. **DB pruning** — keeps poly.db ≤ 200 MB so GCS sync succeeds within 1-min budget
3. **Claude daily cap** — 200/day hard ceiling on copies = bounded API spend
4. **Cloud Run scales to zero** — dashboard doesn't burn when not viewed

---

## 7. Operations Runbook

### 7.1 Deploy code change to bot
```bash
cd /Users/dotanwork/poly-trader

# Push relevant files to VM
gcloud compute scp <files> crypto-trader:/tmp/staging/ --zone=europe-west1-b

# On VM, move into place + restart
gcloud compute ssh crypto-trader --zone=europe-west1-b -- '
  mv /tmp/staging/* /home/dotanwork/poly-trader/<paths>/
  python3 -c "import ast; ast.parse(open(\"main.py\").read()); print(\"syntax OK\")"
  sudo systemctl restart poly-trader
  sleep 8 && sudo systemctl is-active poly-trader
'

# Commit + push (the git push is documentation; deploys are SCP-based)
git add . && git commit -m "..." && git push origin main
```

### 7.2 Deploy dashboard
```bash
cd /Users/dotanwork/crypto-trader/dashboard
gcloud run deploy crypto-trader-dashboard --source . --region=us-central1
```

### 7.3 Diagnose bot freeze
```bash
# 1. Check heartbeat staleness
gcloud storage ls -l gs://crypto-trader-backups-494710/poly_live/heartbeat.json

# 2. SSH to VM
gcloud compute ssh crypto-trader --zone=europe-west1-b

# Inside:
sudo journalctl -u poly-trader --since "10 minutes ago" | tail -50
sudo systemctl status poly-trader
free -h          # memory pressure?
ps aux | grep python3   # which process?
```

### 7.4 Hard reset VM
```bash
# Only if VM is wedged and SSH won't respond
gcloud compute instances reset crypto-trader --zone=europe-west1-b
# Wait ~3 min for systemd to bring services back up
```

### 7.5 Manually trigger summary
```bash
gcloud compute ssh crypto-trader --zone=europe-west1-b -- '
  cd /home/dotanwork/poly-trader && python3 -c "
from strategies.summary import poly_summary
from monitor.notifier import Notifier
poly_summary(\"manual\", \"🧪\", Notifier())
"'
```

### 7.6 Manually re-rank smart wallets
```bash
gcloud compute ssh crypto-trader --zone=europe-west1-b -- '
  cd /home/dotanwork/poly-trader && python3 -c "
from data.storage import engine
from data.firehose import rank_wallets_from_firehose
print(rank_wallets_from_firehose(engine, lookback_days=7, top_n=500))
"'
```

### 7.7 Sweep all open positions (paper hygiene)
```bash
gcloud compute ssh crypto-trader --zone=europe-west1-b -- '
  python3 -c "
import sqlite3
c = sqlite3.connect(\"/home/dotanwork/poly-trader/poly_trader.db\")
n = c.execute(\"UPDATE decisions SET resolution_yes=0, pnl_usd=-size_usd, "
   "resolved_at=datetime(\\\"now\\\"), notes=COALESCE(notes,\\\"\\\")||\\\"|swept\\\" "
   "WHERE size_usd>0 AND resolution_yes IS NULL\").rowcount
c.commit()
print(\"swept\", n)
"'
```

### 7.8 Switch live ↔ paper mode
```bash
# Mode flag is read by the bot every 60 sec from GCS — no restart needed.
echo -n "paper" | gcloud storage cp - gs://crypto-trader-backups-494710/poly_live/mode.txt
# or
echo -n "live"  | gcloud storage cp - gs://crypto-trader-backups-494710/poly_live/mode.txt
```

Also flippable via the dashboard's mode toggle on the Polymarket page.

### 7.9 Lower the Alchemy bill
Two levers, in order of impact:

1. **Reduce tracked wallet count** — `polygon_stream.py:_load_tracked_wallets(max_n=15)` is the default. Drop to 10 = 33% fewer events filter-matched = 33% cheaper.
2. **Raise wallet quality threshold** — `firehose.py:rank_wallets_from_firehose` filters `realized < 500_000`. Bump to 1_000_000 to include only top-tier whales (also = fewer tracked).

After change, restart poly-trader. New subscription uses the smaller list.

---

## 8. Monitoring & Alerting

### 8.1 What's monitored

| Signal | Source | Telegram alert? |
|--------|--------|-----------------|
| `heartbeat.json` mtime in GCS | bot-watchdog Cloud Function | 🚨 DOWN if > 30 min stale; ✅ BACK UP on recovery |
| Smart-money copy fires | bot (synchronous in execute_copy_trades) | 🐳➡️🤖 per fire |
| Smart-money exit closes | bot (execute_copy_exits) | 🐳⬅️🤖 per close |
| Daily 200-cap reached | bot | 🛑 once per day |
| Twice-daily summary | scheduler | 🌅 08:00 UTC / 🌙 20:00 UTC |
| Bot process death | systemd Restart=always | (auto-restart, no Telegram) |
| Memory cap hit | systemd OOMPolicy=stop | (auto-restart by systemd) |
| Polygon WebSocket disconnect | polygon_stream outer driver | (auto-reconnect, no Telegram) |

### 8.2 Dashboard URL
https://crypto-trader-dashboard-521710920484.us-central1.run.app/

Polymarket tab shows:
- Live wallet (real Polymarket account)
- Smart-money copy metrics (6-metric performance row + 4-metric quality row)
- Per-whale P&L breakdown
- Recent trades
- Live whale scanner (last 50 stream hits)

### 8.3 Cloud Run logs
```bash
gcloud logs read 'resource.type=cloud_run_revision AND resource.labels.service_name=crypto-trader-dashboard' --limit 50
```

---

## 9. Learning Loop

The summary report (twice daily on Telegram) parses the `notes` field of every settled trade and breaks down win rate by:

- **Strategy version** (v1 / v2_meanrev / smart_copy)
- **Per-whale** (`smart_copy:NAME|...`)
- **By Claude verdict** (APPROVE / REJECT — for future use after we have ≥30 Claude-gated resolves)
- **By score quartile** (Q1 low → Q4 high — tells us if the composite score actually predicts wins)
- **By side** (YES vs NO)
- **By calibration bucket** (model_yes_prob ≥0.55 / 0.50-0.55 / 0.45-0.50 / ≤0.45)

After enough samples (~50 per bucket), the buckets that consistently under/over-perform should trigger threshold tuning:
- If Claude REJECTs we manually passed end up winning > 50%, Claude is over-cautious → relax the prompt.
- If Q4 (top score) wins < Q1, the score function is wrong → recalibrate.
- If a specific whale's copies are net-negative over 30+ samples, remove them from the seed list.

---

## 10. Known Limitations & Future Work

### 10.1 Known limits
- **Polymarket `/trades` API has ~5-min cache** — firehose discovers wallets but with 5-min lag. Real-time wallet hits come from polygon_stream.
- **Stream signals lack market context** — Polygon RPC event has token_id but not market title. We resolve via the `polymarket_markets` table. New markets we haven't seen yet skip the stream path; the 30s `/positions` poll catches them via auto-track.
- **Claude verdicts cached 60 sec per (wallet, market, side)** — if the same signal fires repeatedly in quick succession, we don't re-spend on Claude.
- **Daily cap is UTC midnight** — not user-local. Could shift to Israel time if needed.

### 10.2 Future
- Add `polymarket_stream` to crypto-trader so it can also access smart-money flow data (currently isolated to poly-trader).
- Per-whale Kelly scaling: scale our size based on the whale's recent win rate. Currently same Kelly for all whales.
- Multi-venue: extend smart_money pattern to Kalshi (we already have `kalshi_client.py`).
- BigQuery export for richer analytics over longer windows.

---

## 11. Quick Reference

**Repo**: https://github.com/Dotan-Peleh/poly-trader
**Sister repo (crypto-trader + dashboard)**: https://github.com/Dotan-Peleh/crypto-trader
**Dashboard**: https://crypto-trader-dashboard-521710920484.us-central1.run.app/
**GCP project**: crypto-agent-494710 (account: dotan.spiegler@gmail.com)
**VM**: crypto-trader (e2-medium, europe-west1-b)
**GCS bucket**: gs://crypto-trader-backups-494710

**Daily ops**:
- Bot runs 24/7 unattended
- Twice-daily summaries push to Telegram at 08:00 + 20:00 UTC
- Down/recovery alerts fire automatically if heartbeats go stale
- Dashboard refreshes every 60 sec; data freshness depends on VM → GCS sync (1 min)

**To start the day**:
- Read the morning summary on Telegram (~11:00 IL)
- If anything looks off (bot count not growing, P&L unexpected), open dashboard
- All changes happen via code commits + SCP deploy (no live UI changes to strategies)
