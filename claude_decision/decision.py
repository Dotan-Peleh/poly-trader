"""
Claude decision gate for binary-options trades.

Same architecture as crypto-trader/strategies/claude_decision.py:
  • System prompt instructs Claude as a world-class trader
  • User prompt has the proposed trade + context + recent track record
  • Cache 60s per (condition_id, side) to bound cost
  • Daily call budget caps total spend
  • Fail-OPEN on API errors so we don't block trades on outage

Output: APPROVE / REJECT / REQUEST_SIZE_CUT / CLAUDE_UNAVAILABLE
"""
import json
import logging
import time
from typing import Optional

from config.settings import settings

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """You are a world-class binary-options trader who has spent \
12+ years pricing crypto derivatives. The bot uses a Black-Scholes-style \
digital-option pricer to compute model probability, then asks you to gate \
the actual fire. Your job: agree, disagree, or scale-down.

CORE PRINCIPLES (non-negotiable):
1. CAPITAL PRESERVATION > RETURNS. Better to skip a thin edge than blow up.
2. LATE-WINDOW FOCUS. We trade only inside the last 12 min of a 15-min market.
   That's where prob sharpens. If T-time > 10 min, prefer HALF_SIZE — vol
   estimate has more error.
3. VOLATILITY UNCERTAINTY. The pricer assumes BTC realized vol is constant
   per-minute. If recent ticks show a vol shock (FOMC, big news), our σ is
   stale and the gap may be artifact, not edge. REJECT under suspected vol shock.
4. BOOK DEPTH. Thin order books mean wide effective spreads — we'll get a
   worse fill than the mid we modeled against. If book is thin (<\$50 depth
   total), HALF_SIZE.
5. PIN RISK. If BTC is within 1 σ_per_min of reference at the time of the
   call, the outcome is genuinely 50/50 and small edges are noise. REJECT.
6. DEFAULT TO REJECT under any uncertainty. Trades skipped today are trades
   we can take tomorrow with better information.

Output STRICT JSON only:
{
  "decision": "APPROVE" | "REJECT" | "REQUEST_SIZE_CUT",
  "confidence": 0.0-1.0,
  "reason": "one short sentence citing 2-3 specific data points"
}
"""


_cache: dict = {}
_CACHE_TTL_SEC = 60.0
_call_count: dict = {"date": "", "count": 0}


def _today_utc() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def _budget_remaining() -> int:
    today = _today_utc()
    if _call_count["date"] != today:
        _call_count["date"] = today
        _call_count["count"] = 0
    return max(0, settings.claude_daily_call_cap - _call_count["count"])


def _client():
    if not (settings.claude_advisor_enabled and settings.anthropic_api_key):
        return None
    try:
        from anthropic import Anthropic
        return Anthropic(api_key=settings.anthropic_api_key)
    except ImportError:
        logger.warning("anthropic library not installed")
        return None


def _build_prompt(market, quote, intent, wallet) -> str:
    minutes = quote.minutes_to_close
    move_needed_pct = (quote.reference_price - quote.current_price) / quote.current_price * 100
    sigma_total = quote.sigma_per_minute * (minutes ** 0.5)
    return f"""PROPOSED TRADE:
Side: <b>{intent.side}</b>     ({"YES wins if BTC stays above reference" if intent.side == "YES" else "NO wins if BTC drops below reference"})

Current state:
  BTC now:        ${quote.current_price:,.2f}
  Reference:      ${quote.reference_price:,.2f}
  Move needed:    {move_needed_pct:+.2f}% (positive = needs to FALL for YES to lose)
  Time left:      {minutes:.1f} min
  σ per minute:   {quote.sigma_per_minute*100:.3f}%
  σ remaining:    {sigma_total*100:.3f}% (across the {minutes:.0f} min until close)

Pricing:
  Implied YES (market mid): {quote.implied_yes_prob:.3f}
  Model YES (digital BS):   {intent.model_yes_prob:.3f}
  Edge:                     {intent.edge*100:+.1f}%

Bankroll context:
  Available: ${wallet.available_balance():,.2f}
  Open positions: {wallet.open_count()}

Decide: APPROVE / REJECT / REQUEST_SIZE_CUT. Output JSON only."""


def claude_gate(market, quote, intent, wallet) -> dict:
    """Returns {decision, confidence, reason} dict."""
    cache_key = (market.condition_id, intent.side)
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < _CACHE_TTL_SEC:
        return cached[1]

    if _budget_remaining() <= 0:
        return {"decision": "CLAUDE_UNAVAILABLE", "confidence": 0.0,
                "reason": "claude budget exhausted (fail-open)"}

    client = _client()
    if client is None:
        return {"decision": "CLAUDE_UNAVAILABLE", "confidence": 0.0,
                "reason": "claude not configured (fail-open)"}

    try:
        user_msg = _build_prompt(market, quote, intent, wallet)
        resp = client.messages.create(
            model=settings.claude_model,
            max_tokens=200,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
            timeout=15,
            temperature=0.2,
        )
        _call_count["count"] = _call_count.get("count", 0) + 1
        text = resp.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1].lstrip("json").strip()
        data = json.loads(text)

        decision = (data.get("decision") or "REJECT").upper()
        if decision not in ("APPROVE", "REJECT", "REQUEST_SIZE_CUT"):
            decision = "REJECT"
        confidence = float(data.get("confidence", 0))
        reason = (data.get("reason") or "")[:240]

        result = {"decision": decision, "confidence": confidence, "reason": reason}
        _cache[cache_key] = (time.time(), result)
        return result

    except Exception as e:
        logger.warning(f"[{market.condition_id}] claude_gate error (fail-open): {e}")
        return {"decision": "CLAUDE_UNAVAILABLE", "confidence": 0.0,
                "reason": f"claude error: {str(e)[:80]}"}
