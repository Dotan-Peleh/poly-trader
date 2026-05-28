"""
Polymarket order placement — live trading via py-clob-client-v2.

Polymarket migrated from CLOB v1 to v2 on 2026-04-30. The v1 SDK
(`py-clob-client`) is now archived and every order it signs gets
`{error: 'order_version_mismatch'}` back. We use the v2 SDK
(`py-clob-client-v2`) and Polymarket's new deposit-wallet model.

Architecture (CLOB v2):
  • Paper mode: existing record_paper_trade() in execution/wallet.py
    writes a synthetic fill. No SDK needed.
  • Live mode: this module wraps py_clob_client_v2.ClobClient with
    L1+L2 auth and submits FAK orders signed via ERC-1271 against the
    user's per-account deposit wallet (an ERC-1967 proxy deployed by
    Polymarket's safe-factory).

Maker/signer model (v2):
  • maker  = deposit wallet (derived deterministically from the EOA
             via the safe-factory CREATE2 formula)
  • signer = deposit wallet (POLY_1271)
  • actual signature comes from the EOA private key — the deposit
    wallet's contract validates it via ERC-1271

The user's funds must live in the deposit wallet for the CLOB to
match orders against it. Polymarket's UI provides a one-time
migration from the legacy Magic Link proxy to the v2 deposit wallet.

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


_client_cache: dict = {"client": None, "ts": 0.0, "deposit_wallet": ""}


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


def _derive_deposit_wallet() -> str:
    """Return the user's v2 deposit-wallet address.

    Source order:
      1. settings.polymarket_deposit_wallet (loaded from Secret Manager
         under `polymarket-deposit-wallet` — this is what Polymarket's
         own UI shows under Portfolio → Deposit → Transfer Crypto, and
         is authoritative).
      2. CREATE2 derivation via the relayer SDK (works only if
         Polymarket actually uses the safe-factory we look up via
         get_contract_config — empirically, they don't always).

    For the current Polymarket V2 flow the configured secret is the
    only reliable source. The CREATE2 path is kept as a fallback for
    new accounts where we haven't captured the address yet.
    """
    cached = _client_cache.get("deposit_wallet")
    if cached:
        return cached

    # 1) Configured address (authoritative — copied from Polymarket UI)
    configured = getattr(settings, "polymarket_deposit_wallet", "") or ""
    if configured:
        _client_cache["deposit_wallet"] = configured
        logger.info(f"deposit_wallet from Secret Manager: {configured}")
        return configured

    # 2) Fallback: best-effort CREATE2 derivation
    pk = _pk()
    if not pk:
        return ""
    try:
        from eth_account import Account
        from py_builder_relayer_client.client import (
            derive as _derive_safe,
            get_contract_config,
        )
        eoa = Account.from_key(pk).address
        cfg = get_contract_config(137)  # Polygon mainnet
        factory = cfg.safe_factory
        dw = _derive_safe(eoa, factory)
        _client_cache["deposit_wallet"] = dw
        logger.warning(
            f"deposit_wallet derived via CREATE2 fallback: EOA={eoa} → {dw}. "
            "If Polymarket rejects orders with 'maker address not allowed', "
            "copy the correct address from polymarket.com → Portfolio → "
            "Deposit → Transfer Crypto and add to Secret Manager under "
            "`polymarket-deposit-wallet`."
        )
        return dw
    except Exception as e:
        logger.error(f"deposit_wallet derivation failed: {e}")
        return ""


def deposit_wallet_address() -> str:
    """Public helper — exposes the derived deposit wallet so the wallet
    snapshot writer can query its on-chain pUSD balance."""
    return _derive_deposit_wallet()


def _get_client():
    """Lazily build (and cache) a ClobClient v2 instance.

    Reads the 5 polymarket secrets from settings (which loads from GCP
    Secret Manager in live mode). Raises RuntimeError if any is missing.
    """
    if _client_cache["client"] is not None and time.time() - _client_cache["ts"] < 600:
        return _client_cache["client"]

    missing = []
    if not _pk():
        missing.append("polymarket-pk")
    if not settings.polymarket_api_key:
        missing.append("polymarket-api-key")
    if not settings.polymarket_api_secret:
        missing.append("polymarket-api-secret")
    if not settings.polymarket_api_passphrase:
        missing.append("polymarket-api-passphrase")
    dw = _derive_deposit_wallet()
    if not dw:
        missing.append("deposit-wallet (could not derive)")
    if missing:
        raise RuntimeError(
            f"Polymarket live trading requires these secrets in GCP Secret "
            f"Manager: {missing}"
        )

    from py_clob_client_v2 import ClobClient, ApiCreds, SignatureTypeV2

    creds = ApiCreds(
        api_key=settings.polymarket_api_key,
        api_secret=settings.polymarket_api_secret,
        api_passphrase=settings.polymarket_api_passphrase,
    )
    client = ClobClient(
        host=settings.polymarket_clob_base,
        chain_id=137,                       # Polygon
        key=_pk(),
        creds=creds,
        signature_type=SignatureTypeV2.POLY_1271,
        funder=dw,                          # v2 deposit wallet (NOT the old Magic Link proxy)
    )
    _client_cache["client"] = client
    _client_cache["ts"] = time.time()
    logger.info(f"py-clob-client-v2 initialised (live mode, deposit_wallet={dw})")
    return client


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
    """Place a marketable FAK order on Polymarket v2.

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
    from py_clob_client_v2 import (
        OrderArgs, OrderType, PartialCreateOrderOptions, Side,
    )

    # Limit slightly above the observed ask to take the offer — books move
    # in milliseconds at 5-min close so we want to be a maker-cross, not stale
    limit_price = round(min(ask_price + 0.005, 0.995), 3)
    units = round(size_usd / limit_price, 4)

    args = OrderArgs(
        token_id=token_id,
        price=limit_price,
        size=units,
        side=Side.BUY,
    )
    try:
        # create_and_post_order handles signing + POST atomically and
        # internally resolves tick_size / neg_risk / fee for the market.
        # FAK = Fill-And-Kill: take what's available at our limit and
        # cancel the rest. Prevents orders from lingering on the book
        # unfilled (the previous GTC choice caused phantom decisions —
        # bot recorded a "filled $11" trade while only $1 actually
        # matched, then settle_tick wrote fake P&L when the market
        # resolved on tape the bot never held).
        resp = client.create_and_post_order(
            order_args=args,
            options=PartialCreateOrderOptions(tick_size="0.01"),
            order_type=OrderType.FAK,
        )
        resp = resp or {}

        # v2 fill semantics: response includes makingAmount / takingAmount
        # for what actually matched. For a BUY:
        #   makingAmount = USD we spent
        #   takingAmount = shares we received
        try:
            paid_usd = float(resp.get("makingAmount") or 0.0)
        except (TypeError, ValueError):
            paid_usd = 0.0
        try:
            shares = float(resp.get("takingAmount") or 0.0)
        except (TypeError, ValueError):
            shares = 0.0

        truly_filled = paid_usd > 0.001 and shares > 0.001
        api_success = bool(resp.get("success"))
        success = api_success and truly_filled

        if api_success and not truly_filled:
            logger.warning(
                f"place_market_order: CLOB returned success=true but no fill "
                f"(makingAmount={paid_usd}, takingAmount={shares}, "
                f"status={resp.get('status')!r}). Treating as failed."
            )

        avg_price = (paid_usd / shares) if shares > 0 else limit_price
        return OrderFill(
            success=success,
            side=side,
            units=shares,            # actual shares filled, not requested
            paid_per_unit=avg_price, # actual avg fill, not the limit
            size_usd=paid_usd,       # actual USD spent, not requested
            raw_response=resp,
        )
    except Exception as e:
        logger.error(f"place_market_order failed: {e}")
        return OrderFill(False, side, 0.0, 0.0, 0.0,
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
    """Confirm all 5 secrets are present and the v2 SDK is importable.
    Returns (ready, missing_list)."""
    missing = []
    if not getattr(settings, "polymarket_api_key", ""):
        missing.append("polymarket-api-key")
    if not getattr(settings, "polymarket_api_secret", ""):
        missing.append("polymarket-api-secret")
    if not getattr(settings, "polymarket_api_passphrase", ""):
        missing.append("polymarket-api-passphrase")
    if not _pk():
        missing.append("polymarket-pk")
    try:
        import py_clob_client_v2  # noqa: F401
    except ImportError:
        missing.append("py-clob-client-v2 (SDK not installed)")
    try:
        import py_builder_relayer_client  # noqa: F401
    except ImportError:
        missing.append("py-builder-relayer-client (deposit-wallet helper not installed)")
    if not _derive_deposit_wallet():
        missing.append("deposit-wallet derivation failed")
    return (len(missing) == 0, missing)


def live_collateral_usd() -> float:
    """Tradeable USDC collateral the CLOB actually recognizes for our funder,
    via get_balance_allowance(COLLATERAL).

    This is the authoritative "what can we trade" number. The raw on-chain
    balanceOf of the deposit wallet reads $0 because Polymarket holds the
    collateral in its own ledger, not as a raw ERC-20 on the deposit wallet
    — so the prior _fetch_balance().effective gate wrongly saw $0 and skipped
    every live order despite a funded account.
    """
    try:
        from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
        client = _get_client()
        resp = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        ) or {}
        bal = resp.get("balance")
        return float(bal) / 1_000_000.0 if bal is not None else 0.0
    except Exception as e:
        logger.error(f"live_collateral_usd failed: {e}")
        return 0.0
