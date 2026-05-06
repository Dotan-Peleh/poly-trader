"""
Polymarket order placement — live trading via py-clob-client.

Architecture:
  • Paper mode: existing record_paper_trade() in execution/wallet.py logs
    a synthetic fill at the current ask price. No SDK needed.
  • Live mode: this module wraps py_clob_client.ClobClient with our
    L1+L2 auth and submits actual market-style limit orders.

Auth model (per docs.polymarket.com/api-reference/authentication):
  • L1 = EIP-712 from polymarket-pk private key (signs every order)
  • L2 = HMAC-SHA256 from polymarket-api-key + secret + passphrase
  • The ClobClient handles both internally once initialised.

Funder (signature type):
  • SignatureType.POLY_GNOSIS_SAFE (=2) is the default for new users
    who connected a wallet directly to Polymarket. Set chain_id=137.

Safety caps (LIVE only):
  • Max $5 per trade for the first 24h after going live
  • Daily loss halt at -$10 (halts new entries until UTC midnight)
  • Min trade $1 (Polymarket's exchange minimum)
"""
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from config.settings import settings

logger = logging.getLogger(__name__)


# Live-mode safety caps (overridable via env later if needed)
LIVE_MAX_TRADE_USD = 5.0           # hard cap per trade — first 24h
LIVE_MAX_TRADE_USD_BUMP_HOURS = 24  # after this many hours, max_position_pct rules
LIVE_DAILY_LOSS_HALT_USD = 10.0     # halt new entries at this realized daily loss


_client_cache: dict = {"client": None, "ts": 0.0}


def _get_client():
    """Lazily build (and cache) a ClobClient instance.

    Reads the 5 polymarket secrets from settings (which loads from GCP
    Secret Manager in live mode). Raises RuntimeError if any is missing.
    """
    if _client_cache["client"] is not None and time.time() - _client_cache["ts"] < 600:
        return _client_cache["client"]

    missing = [
        n for n, v in [
            ("polymarket-pk (settings.polymarket_private_key)", _pk()),
            ("polymarket-funder", settings.polymarket_funder_address),
            ("polymarket-api-key", settings.polymarket_api_key),
            ("polymarket-api-secret", settings.polymarket_api_secret),
            ("polymarket-api-passphrase", settings.polymarket_api_passphrase),
        ] if not v
    ]
    if missing:
        raise RuntimeError(
            f"Polymarket live trading requires these secrets in GCP Secret "
            f"Manager: {missing}"
        )

    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
    from py_clob_client.constants import POLYGON

    creds = ApiCreds(
        api_key=settings.polymarket_api_key,
        api_secret=settings.polymarket_api_secret,
        api_passphrase=settings.polymarket_api_passphrase,
    )
    client = ClobClient(
        host=settings.polymarket_clob_base,
        chain_id=POLYGON,
        key=_pk(),
        creds=creds,
        signature_type=2,                       # POLY_GNOSIS_SAFE
        funder=settings.polymarket_funder_address,
    )
    _client_cache["client"] = client
    _client_cache["ts"] = time.time()
    logger.info("py-clob-client initialised (live mode)")
    return client


def _pk() -> str:
    """Read the trading wallet private key from Secret Manager via the
    settings module. Lazy and cached — reuses the helper already in
    config/settings.py for consistency."""
    val = getattr(settings, "polymarket_private_key", "")
    if val:
        return val
    # Pull on demand — settings loads only on bot start; if we just got a fresh
    # secret without restarting, this picks it up.
    try:
        from config.settings import _fetch_secret_manager
        v = _fetch_secret_manager("polymarket-pk")
        if v:
            settings.polymarket_private_key = v
            return v
    except Exception:
        pass
    return ""


@dataclass(frozen=True)
class OrderFill:
    success: bool
    side: str                         # 'YES' or 'NO'
    units: float
    paid_per_unit: float              # average fill price
    size_usd: float
    raw_response: dict


def place_market_order(token_id: str, side: str, size_usd: float,
                        ask_price: float) -> OrderFill:
    """Place a marketable limit order on Polymarket.

    Args:
      token_id: CLOB token id for YES or NO
      side: 'YES' or 'NO' (informational; the actual buy/sell side is BUY)
      size_usd: USD to spend on this trade
      ask_price: best ask we observed; we cap our limit slightly above
                 to allow for some movement during placement

    Polymarket order semantics:
      • To take a YES position, you BUY shares of the YES token
      • To take a NO position, you BUY shares of the NO token
      • Limit price is in [0, 1]; pay 'price' per share, get $1 if win
    """
    if size_usd < 1.0:
        return OrderFill(False, side, 0.0, 0.0, 0.0,
                          {"error": f"size {size_usd} below $1 min"})
    if not (0 < ask_price < 1):
        return OrderFill(False, side, 0.0, 0.0, 0.0,
                          {"error": f"bad ask price {ask_price}"})

    client = _get_client()
    from py_clob_client.clob_types import OrderArgs, OrderType

    # Limit slightly above the observed ask to take the offer — books move
    # in milliseconds at 5-min close so we want to be a maker-cross, not stale
    limit_price = round(min(ask_price + 0.005, 0.995), 3)
    units = round(size_usd / limit_price, 4)

    args = OrderArgs(
        token_id=token_id,
        price=limit_price,
        size=units,
        side="BUY",
    )
    try:
        signed = client.create_order(args)
        # GTC = good-til-cancel (lives in the book if not immediately filled);
        # FOK = fill-or-kill (cancel if can't immediately fill)
        # We use IOC-equivalent (GTC + immediate cancel after timeout
        # in the calling code) for the smoke trades.
        resp = client.post_order(signed, OrderType.GTC)
        success = bool(resp and resp.get("success"))
        # Pull avg fill price if returned, else use limit
        filled = resp.get("makingAmount") or units
        avg_price = limit_price
        return OrderFill(
            success=success,
            side=side,
            units=float(filled),
            paid_per_unit=avg_price,
            size_usd=size_usd,
            raw_response=resp or {},
        )
    except Exception as e:
        logger.error(f"place_market_order failed: {e}")
        return OrderFill(False, side, 0.0, 0.0, size_usd,
                          {"error": str(e)[:240]})


def cap_for_smoke_period(proposed_size_usd: float, bot_started_at: datetime) -> float:
    """First 24h cap: $5 per trade regardless of Kelly math."""
    age_hours = (datetime.utcnow() - bot_started_at).total_seconds() / 3600
    if age_hours < LIVE_MAX_TRADE_USD_BUMP_HOURS:
        return min(proposed_size_usd, LIVE_MAX_TRADE_USD)
    return proposed_size_usd


def daily_loss_halts(realized_pnl_usd_today: float) -> bool:
    """True if we should HALT new entries until UTC midnight."""
    return realized_pnl_usd_today <= -LIVE_DAILY_LOSS_HALT_USD


def live_readiness_check() -> tuple[bool, list[str]]:
    """Confirm all 5 secrets are present and the SDK is importable.
    Returns (ready, missing_list)."""
    missing = []
    if not getattr(settings, "polymarket_funder_address", ""):
        missing.append("polymarket-funder")
    if not getattr(settings, "polymarket_api_key", ""):
        missing.append("polymarket-api-key")
    if not getattr(settings, "polymarket_api_secret", ""):
        missing.append("polymarket-api-secret")
    if not getattr(settings, "polymarket_api_passphrase", ""):
        missing.append("polymarket-api-passphrase")
    if not _pk():
        missing.append("polymarket-pk")
    try:
        import py_clob_client  # noqa: F401
    except ImportError:
        missing.append("py-clob-client (SDK not installed)")
    return (len(missing) == 0, missing)
