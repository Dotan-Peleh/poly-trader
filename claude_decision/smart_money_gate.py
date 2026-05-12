"""
Claude smart-money copy gate — final approval before we mirror a whale.

Plumbed into execute_copy_trades AFTER all mechanical filters pass.
Cost budget: ~100 calls/day × $0.01 ≈ $1/day with Haiku.

Inputs Claude sees per call:
  • Whale stats (pseudonym, lifetime $, recent 30d win rate from rankings)
  • Trade specifics (market title, side, our entry price, edge, size)
  • Market shape (depth, time to resolution, implied prob)
  • Recent calibration (last 7-day stats for our copy strategy)
  • Today's progress vs 100-fire cap (so it tightens late in day)

Output: APPROVE / REJECT (no half-measures — keep prompt simple).
"""
import json
import logging
import time
from typing import Optional

from config.settings import settings

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """You are the final gate on a smart-money copy bot for \
Polymarket prediction markets. The bot watches ~16 wallets ($2M-$12M \
lifetime P&L each) trade on chain, and when they open a position the \
bot wants to mirror it.

Your job: given the proposed copy, decide APPROVE or REJECT.

The bot has already passed mechanical gates:
  • Whale's lifetime P&L verified > $2M
  • Whale's last fill on this market < 15 min old
  • Implied probability in [0.10, 0.90] (no lottery tails)
  • Computed edge in [4%, 10%] (skip both noise and screaming-market)
  • Book depth > $200 on our side
  • Market resolves > 2 min from now

So when you see a trade here, the math says it's worth doing.

REJECT only when something structural is wrong that the mechanical \
gates can't see:
  • Market title indicates a vol shock just happened (event-driven, \
    e.g. "Will the Fed cut today" 5 min before announcement)
  • Whale's trade pattern suggests a hedge, not a directional view \
    (e.g. they hold both YES and NO on related markets)
  • Sample is too small — first copy of a new whale with no track record
  • The market has extreme staleness (resolved > 24h ago for an open \
    position somehow)
  • Conflict with our recent track record — if we've copied this whale \
    5 times this week and lost 4, weight that

DEFAULT to APPROVE. The user has explicitly chosen to copy these \
whales because mathematical filters say so. Your job is rare veto, \
not gatekeeping.

Output STRICT JSON only:
{
  "decision": "APPROVE" | "REJECT",
  "confidence": 0.0-1.0,
  "reason": "one short sentence (≤140 chars) citing the specific data point"
}
"""


_cache: dict = {}
_today_calls: dict = {"date": "", "count": 0}


def _today_key() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def _bump_daily_call_count() -> int:
    """Increment + return today's call count. Reset at UTC midnight."""
    today = _today_key()
    if _today_calls["date"] != today:
        _today_calls["date"] = today
        _today_calls["count"] = 0
    _today_calls["count"] += 1
    return _today_calls["count"]


def _recent_calibration_summary() -> str:
    """Pull recent smart-copy outcomes from DB so Claude can weight them.
    Returns a 1-2 line text summary, e.g.:
       'Last 7d: 23 copies, 11 wins (48%), P&L $-3.20'."""
    try:
        from sqlalchemy.orm import Session
        from sqlalchemy import text as _t
        from data.storage import engine
        with Session(engine) as s:
            r = s.execute(_t("""
                SELECT
                  COUNT(*) AS n,
                  SUM(CASE WHEN (side='YES' AND resolution_yes=1)
                            OR (side='NO' AND resolution_yes=0)
                           THEN 1 ELSE 0 END) AS wins,
                  COALESCE(SUM(pnl_usd), 0) AS pnl
                FROM decisions
                WHERE notes LIKE '%smart_copy%'
                  AND ts > datetime('now', '-7 days')
                  AND resolution_yes IS NOT NULL
            """)).first()
        if not r or not r[0]:
            return "no recent calibration data yet."
        n, wins, pnl = r[0], r[1] or 0, r[2] or 0
        wr = (wins / n * 100) if n else 0
        return f"Last 7d smart-copy: {n} resolved, {wins} wins ({wr:.0f}%), P&L ${pnl:+.2f}."
    except Exception as e:
        logger.debug(f"calibration summary failed: {e}")
        return "calibration query unavailable."


def claude_gate(
    *,
    wallet_name: str,
    wallet_lifetime_pnl: float,
    market_title: str,
    side: str,
    paid_per_unit: float,
    edge: float,
    size_usd: float,
    minutes_to_close: float,
    today_fire_count: int,
    daily_cap: int,
    quality_score: float,
) -> dict:
    """Returns {decision, confidence, reason}. APPROVE by default on
    failure so we don't lose trades to API blips."""
    if not (settings.anthropic_api_key and settings.claude_advisor_enabled):
        return {"decision": "APPROVE", "confidence": 0.5,
                "reason": "claude advisor disabled — default approve"}

    cap = settings.claude_daily_call_cap
    if _today_calls.get("date") == _today_key() and _today_calls.get("count", 0) >= cap:
        return {"decision": "APPROVE", "confidence": 0.5,
                "reason": f"claude daily cap {cap} reached — default approve"}

    cache_key = f"{wallet_name}:{market_title[:40]}:{side}"
    if cache_key in _cache:
        ts, verdict = _cache[cache_key]
        if time.time() - ts < 60:
            return verdict

    calibration = _recent_calibration_summary()
    user_msg = f"""SMART-MONEY COPY PROPOSAL — APPROVE or REJECT?

Whale:   {wallet_name}  (lifetime P&L ${wallet_lifetime_pnl:,.0f})
Market:  {market_title[:140]}
Side:    {side}
Entry:   ${paid_per_unit:.3f}
Edge:    {edge*100:+.1f}%  (vs implied)
Size:    ${size_usd:.2f}
T-:      {minutes_to_close:.0f} min to resolution
Score:   {quality_score:.1f} (composite quality)

Today:   {today_fire_count} of {daily_cap} daily cap used ({today_fire_count/daily_cap*100:.0f}%)

Calibration: {calibration}

Reply STRICT JSON only."""

    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=settings.anthropic_api_key)
        resp = client.messages.create(
            model=settings.claude_model,
            max_tokens=200,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
            timeout=12,
        )
        text = resp.content[0].text.strip()
        _bump_daily_call_count()
        # Strip code fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        verdict = json.loads(text.strip())
        if verdict.get("decision") not in ("APPROVE", "REJECT"):
            verdict["decision"] = "APPROVE"
        _cache[cache_key] = (time.time(), verdict)
        return verdict
    except Exception as e:
        logger.warning(f"claude_gate failed (defaulting APPROVE): {e}")
        return {"decision": "APPROVE", "confidence": 0.5,
                "reason": f"claude error — default approve ({type(e).__name__})"}


def todays_call_count() -> int:
    if _today_calls.get("date") != _today_key():
        return 0
    return int(_today_calls.get("count", 0))
