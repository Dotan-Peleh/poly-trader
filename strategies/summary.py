"""
Twice-daily Telegram summary for poly-trader.

Surfaces enough info that the user can see at a glance whether the
strategy is actually making money, and Claude can read the same data
on a future session and propose targeted improvements.

Sections:
  • Headline:     wins / losses / win-rate / cumulative P&L since last summary
  • All-time:     same metrics over the full bot history
  • By side:      YES bets vs NO bets (catches asymmetric signal failure)
  • Calibration:  predicted-prob bucket × actual hit-rate (catches model miscalibration)
  • Open:         current open positions and their MTM/edge
  • Last 5:       most-recent trade outcomes for sanity-check

Schedule: 08:00 UTC ("morning") + 20:00 UTC ("evening") — chosen to land
in the user's Israel-time daytime hours (~11:00 / ~23:00 IL).
"""
import logging
from datetime import datetime, timedelta
from typing import List, Tuple

from sqlalchemy.orm import Session

from config.settings import settings
from data.storage import engine, Decision

logger = logging.getLogger(__name__)


def _is_win(side: str, resolution_yes: int) -> bool:
    return (resolution_yes == 1 and side == "YES") or (resolution_yes == 0 and side == "NO")


def _summarize(rows: List[Decision]) -> dict:
    """Reduce a list of resolved Decision rows to the standard metric block."""
    wins = sum(1 for r in rows if r.resolution_yes is not None and _is_win(r.side, r.resolution_yes))
    resolved = [r for r in rows if r.resolution_yes is not None]
    losses = len(resolved) - wins
    pnl = sum((r.pnl_usd or 0.0) for r in resolved)
    win_rate = (wins / len(resolved) * 100) if resolved else 0.0
    return {
        "fires": len(rows),
        "resolved": len(resolved),
        "wins": wins,
        "losses": losses,
        "win_rate_pct": win_rate,
        "pnl_usd": pnl,
    }


def _calibration_buckets(rows: List[Decision]) -> List[Tuple[str, int, int, float]]:
    """Group fires by their predicted YES probability and report actual hit rate.

    Buckets are anchored at our model's bounded fair_yes_prob in [0.30, 0.70]:
      • Strong YES   model >= 0.55
      • Lean YES     0.50 < model < 0.55
      • Lean NO      0.45 < model <= 0.50
      • Strong NO    model <= 0.45

    For each bucket: (label, n_resolved, n_yes_outcomes, hit_rate%).
    Hit rate = fraction where actual outcome was YES, regardless of which
    side we bet. Lets us see if the model's probability is well-calibrated:
    Strong YES bucket should have hit_rate near or above its midpoint.
    """
    buckets = [
        ("Strong YES (≥0.55)", lambda p: p >= 0.55),
        ("Lean YES (0.50-0.55)", lambda p: 0.50 < p < 0.55),
        ("Lean NO (0.45-0.50)", lambda p: 0.45 < p <= 0.50),
        ("Strong NO (≤0.45)", lambda p: p <= 0.45),
    ]
    out = []
    resolved = [r for r in rows if r.resolution_yes is not None and r.model_yes_prob is not None]
    for label, predicate in buckets:
        in_bucket = [r for r in resolved if predicate(r.model_yes_prob)]
        if not in_bucket:
            out.append((label, 0, 0, 0.0))
            continue
        yes_outcomes = sum(1 for r in in_bucket if r.resolution_yes == 1)
        hit_rate = yes_outcomes / len(in_bucket) * 100
        out.append((label, len(in_bucket), yes_outcomes, hit_rate))
    return out


def _side_breakdown(rows: List[Decision]) -> dict:
    out = {"YES": {"n": 0, "wins": 0, "pnl": 0.0},
           "NO":  {"n": 0, "wins": 0, "pnl": 0.0}}
    for r in rows:
        if r.resolution_yes is None or r.side not in out:
            continue
        out[r.side]["n"] += 1
        if _is_win(r.side, r.resolution_yes):
            out[r.side]["wins"] += 1
        out[r.side]["pnl"] += r.pnl_usd or 0.0
    return out


def build_summary_message(label: str, emoji: str, hours_back: int = 12) -> str:
    """Render the summary as Telegram-flavored HTML."""
    # NOTE: don't filter by Decision.mode — historical rows have mode='live'
    # from a stale env-var setting, but the bot operates in paper via the
    # GCS flag (effective_mode). Rolling all rows up is correct since this
    # DB is poly-trader-specific and only contains this bot's history.
    cutoff = datetime.utcnow() - timedelta(hours=hours_back)
    with Session(engine) as session:
        all_fires = list(session.query(Decision)
                         .filter(Decision.size_usd > 0)
                         .order_by(Decision.id.asc()).all())
        recent = [r for r in all_fires if r.ts and r.ts >= cutoff]
        open_now = [r for r in all_fires if r.resolution_yes is None]
        last_5 = sorted(
            [r for r in all_fires if r.resolution_yes is not None],
            key=lambda r: r.id,
        )[-5:]

    # Split by strategy tag so we can compare across versions.
    # Tags seen in notes (substring match anywhere in the string):
    #   "v2_meanrev" / "v2_meanrev_inferred"  — v2 mean-reversion
    #   "smart_copy:<wallet_name>|<addr>"     — smart-money copy trades
    #   "early_exit:..." prefix may be prepended by exit_manager
    def _is_v2(r):
        n = r.notes or ""
        return "v2_meanrev" in n and "smart_copy" not in n
    def _is_smart_copy(r):
        return "smart_copy" in (r.notes or "")
    v1_fires = [r for r in all_fires if not _is_v2(r) and not _is_smart_copy(r)]
    v2_fires = [r for r in all_fires if _is_v2(r)]
    smart_fires = [r for r in all_fires if _is_smart_copy(r)]

    rec = _summarize(recent)
    alltime = _summarize(all_fires)
    v1_block = _summarize(v1_fires) if v1_fires else None
    v2_block = _summarize(v2_fires) if v2_fires else None
    smart_block = _summarize(smart_fires) if smart_fires else None
    sides = _side_breakdown(all_fires)
    cal = _calibration_buckets(all_fires)

    # Per-smart-wallet breakdown (for the copy strategy)
    smart_by_wallet = {}
    for r in smart_fires:
        n = r.notes or ""
        # parse "smart_copy:NAME|addr" possibly prefixed by "early_exit:..."
        idx = n.find("smart_copy:")
        if idx < 0:
            continue
        rest = n[idx + len("smart_copy:"):]
        name = rest.split("|", 1)[0]
        if name not in smart_by_wallet:
            smart_by_wallet[name] = []
        smart_by_wallet[name].append(r)

    pnl_emoji = "🟢" if rec["pnl_usd"] >= 0 else "🔴"

    lines = []
    lines.append(f"{emoji} <b>POLY-TRADER {label.upper()} SUMMARY</b>")
    lines.append("")
    lines.append(f"<b>Last {hours_back}h</b>")
    lines.append(f"  fires: {rec['fires']} | resolved: {rec['resolved']}")
    lines.append(f"  W/L: {rec['wins']}W / {rec['losses']}L "
                 f"({rec['win_rate_pct']:.0f}% win rate)")
    lines.append(f"  P&L: {pnl_emoji} <b>${rec['pnl_usd']:+.2f}</b>")
    lines.append("")
    lines.append(f"<b>All-time</b>  ({alltime['resolved']} resolved)")
    lines.append(f"  {alltime['wins']}W / {alltime['losses']}L "
                 f"({alltime['win_rate_pct']:.0f}% win rate)")
    lines.append(f"  P&amp;L: <b>${alltime['pnl_usd']:+.2f}</b>")
    lines.append("")
    if v1_block or v2_block or smart_block:
        lines.append("<b>Strategy A/B</b>")
        if v1_block:
            lines.append(f"  v1 (momentum): {v1_block['resolved']} resolved, "
                         f"{v1_block['wins']}W ({v1_block['win_rate_pct']:.0f}%) "
                         f"${v1_block['pnl_usd']:+.2f}")
        if v2_block:
            lines.append(f"  v2_meanrev:    {v2_block['resolved']} resolved, "
                         f"{v2_block['wins']}W ({v2_block['win_rate_pct']:.0f}%) "
                         f"${v2_block['pnl_usd']:+.2f}")
        if smart_block:
            lines.append(f"  smart_copy:    {smart_block['resolved']} resolved, "
                         f"{smart_block['wins']}W ({smart_block['win_rate_pct']:.0f}%) "
                         f"${smart_block['pnl_usd']:+.2f}")
        lines.append("")
    if smart_by_wallet:
        lines.append("<b>By smart wallet</b>")
        # show wallets with most copies first
        for name, rows in sorted(smart_by_wallet.items(), key=lambda kv: -len(kv[1]))[:8]:
            blk = _summarize(rows)
            lines.append(f"  {name[:18]:18s} {blk['resolved']:>2}R "
                         f"{blk['wins']}W ({blk['win_rate_pct']:.0f}%) "
                         f"${blk['pnl_usd']:+.2f}")
        lines.append("")

    # ── LEARNING LOOP: per-feature win-rate breakdown ──
    # Tag format: smart_copy:NAME|addr|score=N|claude=APPROVE|conf=0.X
    # Parse and bucket so we can see what features correlate with wins.
    import re as _re
    def _feature(r, key):
        n = r.notes or ""
        m = _re.search(rf"\|{key}=([^|]+)", n)
        return m.group(1) if m else None
    if smart_fires:
        # Bucket by Claude verdict
        by_claude = {"APPROVE": [], "REJECT-passed-anyway": [], "no-claude": []}
        for r in smart_fires:
            v = _feature(r, "claude")
            if v == "APPROVE":
                by_claude["APPROVE"].append(r)
            elif v == "REJECT":
                by_claude["REJECT-passed-anyway"].append(r)
            else:
                by_claude["no-claude"].append(r)
        lines.append("<b>Learning: by Claude verdict</b>")
        for k, rows in by_claude.items():
            if not rows: continue
            blk = _summarize(rows)
            lines.append(f"  {k}: {blk['resolved']}R {blk['wins']}W "
                         f"({blk['win_rate_pct']:.0f}%) ${blk['pnl_usd']:+.2f}")
        lines.append("")

        # Bucket by quality score quartile
        scored = []
        for r in smart_fires:
            s = _feature(r, "score")
            if s:
                try: scored.append((float(s), r))
                except: pass
        if scored:
            scored.sort()
            q = len(scored) // 4 or 1
            buckets = {
                "Q1 (low score)":  scored[:q],
                "Q2":              scored[q:2*q],
                "Q3":              scored[2*q:3*q],
                "Q4 (high score)": scored[3*q:],
            }
            lines.append("<b>Learning: by quality-score quartile</b>")
            for k, items in buckets.items():
                if not items: continue
                rows = [r for _, r in items]
                blk = _summarize(rows)
                if blk['resolved'] == 0:
                    continue
                lines.append(f"  {k}: {blk['resolved']}R {blk['wins']}W "
                             f"({blk['win_rate_pct']:.0f}%) ${blk['pnl_usd']:+.2f}")
            lines.append("")

    # Daily cap progress
    try:
        from strategies.smart_money import _todays_copy_count, DAILY_COPY_CAP
        today_n = _todays_copy_count()
        lines.append(f"<b>Today's cap</b>: {today_n} / {DAILY_COPY_CAP} fires used")
        lines.append("")
    except Exception:
        pass
    lines.append("<b>By side</b>")
    for side in ("YES", "NO"):
        d = sides[side]
        wr = (d["wins"] / d["n"] * 100) if d["n"] else 0.0
        lines.append(f"  {side}: {d['n']} trades | {d['wins']}W "
                     f"({wr:.0f}%) | ${d['pnl']:+.2f}")
    lines.append("")
    lines.append("<b>Calibration</b>  (hit_rate = actual YES outcomes)")
    for bucket, n, yes, hit in cal:
        if n == 0:
            lines.append(f"  {bucket}: no fires")
        else:
            # Star if hit_rate is consistent with bucket direction
            consistent = "✓" if (
                ("Strong YES" in bucket and hit >= 55)
                or ("Lean YES" in bucket and hit >= 50)
                or ("Lean NO" in bucket and hit <= 50)
                or ("Strong NO" in bucket and hit <= 45)
            ) else "⚠️"
            lines.append(f"  {bucket}: {n} fires, {hit:.0f}% YES outcomes {consistent}")
    lines.append("")
    if open_now:
        lines.append(f"<b>Open ({len(open_now)})</b>")
        for r in open_now[:5]:
            edge_pct = (r.edge or 0) * 100
            lines.append(f"  {r.side} @ {r.paid_per_unit:.2f} "
                         f"(edge {edge_pct:+.1f}%, ${r.size_usd:.2f})")
    if last_5:
        lines.append("")
        lines.append("<b>Last 5</b>")
        for r in last_5:
            ok = _is_win(r.side, r.resolution_yes)
            mark = "✅" if ok else "❌"
            pnl = r.pnl_usd or 0.0
            lines.append(f"  {mark} {r.side} edge={(r.edge or 0)*100:+.1f}% → ${pnl:+.2f}")

    return "\n".join(lines)


def poly_summary(label: str, emoji: str, notifier) -> None:
    """Compose + send the twice-daily summary. Called by APScheduler."""
    try:
        msg = build_summary_message(label=label, emoji=emoji, hours_back=12)
        if notifier is not None:
            notifier.send(msg)
        logger.info(f"poly_summary {label}: sent ({len(msg)} chars)")
    except Exception as e:
        logger.error(f"poly_summary {label} failed: {e}")
