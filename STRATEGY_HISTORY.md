# Strategy History

A short ledger of strategies we tried, what happened, and why each was retired.
The goal is to avoid relearning the same lessons.

---

## v2_meanrev (pre_window) — retired 2026-05-19

**Premise.** Bet against recent short-horizon BTC moves on Polymarket 5-minute
binaries, firing 6–30 minutes before resolution. The thesis was that retail
mean-reverts after small BTC moves and the implied probability lags the
true reversion probability.

**Inputs.** A 3-feature model:
- `BASE_YES_PRIOR = 0.505`
- ± `MAX_REVERSION_BIAS = 0.07` from recent BTC return / vol
- ± `MAX_IMBALANCE_BIAS = 0.03` from Polymarket book depth tilt
- Clamped to `[0.30, 0.70]`

**Gates.** Implied in `[0.40, 0.60]`, |edge| ≤ 10%, tail filter
`[0.10, 0.90]`, placeholder-book reject (`yes_ask + no_ask > 1.05`),
cooldown after 3/5 consecutive losses, `pre_window_edge_min_override = 0.07`.

### What we learned

Two consecutive fixes failed to unlock edge:

| Phase | Window | n | WR | Wilson 95% CI | Breakeven | ROI |
|---|---|---|---|---|---|---|
| PRE-fix LIVE | 2026-05-09 → 05-12 | 323 | 44.0% | [38.7%, 49.4%] | 50.7% | −1.5% |
| PRE-fix PAPER (later contamination) | 2026-05-12 evening | 518 | 21.8% | — | — | −30.6% |
| POST-fix PAPER | 2026-05-17 18:00 → 05-19 11:00 | 200 | 38.0% | [31.6%, 44.9%] | 51.0% | −3.6% |

**The 2026-05-17 execution-cost commit (`0a71fe4`)** fixed three real bugs:
- Paper exits priced at mid instead of bid (over-stated proceeds)
- `time_bailout` floor moved from `<0%` to `<-3%` (was eating spread)
- `edge_min` raised 4% → 7%

It delivered exactly the structural change predicted (fire rate dropped
8.0 → 5.0/hr, −37.5%) but the resulting trades remained statistically
losers at 95%: the Wilson CI ceiling 44.9% sits 6 percentage points below
the fee-adjusted breakeven 51.0%.

### Why recalibration was not pursued

Recalibration (isotonic / Platt) only helps if the model has
**discrimination** — different inputs produce meaningfully different
outputs that correlate with outcomes.

In the post-fix sample, **all 200 trades fell in a single 10pp bucket**
(`model_our_side ∈ [0.50, 0.60]`, average 0.558). The model is effectively
a constant predictor with noise. Calibration of a constant just shifts
the constant — it does not create discrimination where there is none.
A calibrated output that mapped 0.558 → 0.380 would simply produce
near-zero fires, because `calibrated_prob < implied_prob + edge_threshold`
for almost every market.

### Structural reasons for the lack of edge

- Polymarket 5-minute BTC binaries are dominated by HFT market makers
  with sub-millisecond latency, real vol surfaces from options markets,
  and cross-asset signals. Retail running APScheduler against REST APIs
  is the wrong contestant.
- Polymarket charges 2% on winnings + ~1.5% structural spread, eating
  ~4% per round-trip. Net edge requires ≥3pp above that, which the
  3-feature model could not produce.
- Pin risk near the reference price: small moves flip the binary, and
  the model has no special insight into the last-second microstructure.

### Disposition

- Scheduler job `decision_tick` removed from `main.py`.
- `strategies/pre_window.py` moved to `strategies/_archived/pre_window.py`.
- `exit_manager` and `settle_tick` continue to run; smart_money copy
  continues to fire (with per-wallet concentration caps added the same
  day — see commit `36e2f58`).
- Historical decisions remain in the DB tagged `strategy='v2_meanrev'`
  for post-hoc analysis.

### Conditions under which to revisit

Revisit only when **all three** hold:

1. A new input feature (e.g. live order-flow imbalance, cross-exchange
   basis, funding-rate divergence, native options IV) produces a model
   whose calibration plot is **non-flat** — different predicted-prob
   buckets actually show different realized hit rates.
2. Polymarket V2 deposit-wallet auth (issue #67) is unblocked so the
   strategy could ever run live.
3. smart_money copy is mature enough that pre_window's potential upside
   is worth the dev time at the margin.

Until then: the calibration project is sunk-cost rationalization.

---

## Active strategies

- **smart_money (copy-trading)** — polls top wallets by lifetime PnL via
  Polymarket data-api + Polygon RPC stream; copies entries that pass
  edge / quality / claude-gate, exits when source wallet reduces ≥30%.
  Per-wallet concentration caps (max 3 copies / $50 notional per UTC
  day) added 2026-05-19 after live data showed top-2 wallets driving
  113% of PnL.
