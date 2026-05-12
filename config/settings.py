"""
Pydantic settings + live-mode safety gates.
Mirrors crypto-trader's pattern but scoped to binary-options trading.
"""
import logging
from typing import Literal
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


def _fetch_secret_manager(name: str, project: str = "crypto-agent-494710") -> str:
    """Read a secret from GCP Secret Manager.

    Two-tier auth so the same helper works on a GCE VM AND on a laptop:
      1. Metadata-server token (default on GCE — fast, no deps)
      2. Application Default Credentials (works on a laptop after
         `gcloud auth application-default login`) — falls through to here.
    """
    import urllib.request
    import json as _json
    import base64

    token = None
    # 1) GCE metadata server (works on GCE VMs, fails fast on a laptop)
    try:
        token_url = (
            "http://metadata.google.internal/computeMetadata/v1/"
            "instance/service-accounts/default/token"
        )
        req = urllib.request.Request(token_url, headers={"Metadata-Flavor": "Google"})
        with urllib.request.urlopen(req, timeout=2) as r:
            token = _json.loads(r.read().decode("utf-8"))["access_token"]
    except Exception:
        token = None

    # 2) ADC fallback — requires `google-auth` (already pulled in by
    #    google-cloud-storage which the bot uses for GCS sync). Works on
    #    a laptop after `gcloud auth application-default login`.
    if not token:
        try:
            import google.auth
            from google.auth.transport.requests import Request as _GReq
            creds, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            creds.refresh(_GReq())
            token = creds.token
        except Exception as e:
            logger.debug(f"Secret Manager ADC fallback failed for {name}: {e}")
            return ""

    if not token:
        return ""

    try:
        url = (
            f"https://secretmanager.googleapis.com/v1/projects/{project}"
            f"/secrets/{name}/versions/latest:access"
        )
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = _json.loads(r.read().decode("utf-8"))
        return base64.b64decode(data["payload"]["data"]).decode("utf-8")
    except Exception as e:
        logger.debug(f"Secret Manager fetch failed for {name}: {e}")
        return ""


class Settings(BaseSettings):
    # Trading mode
    trading_mode: Literal["paper", "live"] = "paper"
    starting_capital: float = 500.0           # USD paper bankroll — realistic
                                              # size for smart-money copy strategy
                                              # ($30 was too small after Polygon gas)

    # Strategy thresholds (pre_window v2 — fires BEFORE measurement window opens)
    edge_threshold: float = 0.04              # 4% min |fair − implied| to fire
    # Late_window (legacy) bounds — kept so settle_resolved_markets and other
    # late_window code paths still resolve. Pre_window has its own bounds below.
    min_seconds_to_close: int = 120
    max_minutes_to_close: int = 55
    # Pre-window bounds: fire 6–30 min before resolution. At T-30 BTC has
    # weak predictive signal; at T-6 the next 5-min window is about to open
    # and momentum/imbalance signals are freshest.
    pre_window_min_minutes: int = 6
    pre_window_max_minutes: int = 30
    kelly_fraction_divisor: float = 4.0       # quarter-Kelly
    max_position_pct: float = 0.05            # cap per trade at 5% bankroll
    max_concurrent_trades: int = 10        # smart-money copy expects 5-10 concurrent
    daily_loss_limit_pct: float = 0.20        # halt entries at -20% daily
    # Raised from 0.08 → 0.20 on 2026-05-10 so v2_meanrev has runway to
    # generate 50+ trades despite v1's $15.52 daily loss tripping the
    # breaker. Tighten back to 0.08 once v2 has settled calibration data.

    # Vol estimator
    vol_window_minutes: int = 30              # EWMA half-life
    min_sigma_per_minute: float = 0.0001      # floor: 0.01%/min (avoid div-by-zero)

    # Kalshi (primary venue — KXBTC15M binary product)
    kalshi_api_base: str = "https://api.elections.kalshi.com/trade-api/v2"
    kalshi_series_ticker: str = "KXBTC15M"    # 15-minute BTC up/down
    kalshi_api_key_id: str = ""               # populated in live mode from Secret Manager
    # signing private key never stored in code — Secret Manager only

    # Polymarket (PRIMARY VENUE — 5-min BTC up/down via Gamma + CLOB)
    polymarket_gamma_base: str = "https://gamma-api.polymarket.com"
    polymarket_clob_base: str = "https://clob.polymarket.com"
    polymarket_chain_id: int = 137
    polygon_rpc_url: str = "https://polygon-rpc.com"
    # Window length for the BTC events we trade (minutes).
    # Counter-intuitive: 5-min markets are the right ones to trade — but
    # ONLY in the pre-window phase (before the 5-min measurement window
    # opens), where books are at clean 50/50 with $300+ depth per side.
    # 15-min and 60-min markets had no real two-sided liquidity in our recon.
    polymarket_window_minutes: int = 5

    # Live trading credentials (HMAC, not raw private key — generated
    # from Polymarket UI Settings → API Keys, stored in Secret Manager)
    polymarket_api_key: str = ""
    polymarket_api_secret: str = ""
    polymarket_api_passphrase: str = ""
    polymarket_funder_address: str = ""    # the proxy/Safe wallet address, NOT your EOA
    polymarket_private_key: str = ""       # NEVER store in plaintext settings; loaded
                                            # from Secret Manager at runtime in live mode
    # Signature type for the ClobClient:
    #   0 = EOA (direct signing, rare)
    #   1 = POLY_PROXY (Magic Link email/Google login users — DEFAULT)
    #   2 = GNOSIS_SAFE (MetaMask / wallet-connect users)
    polymarket_signature_type: int = 1

    # Alchemy Polygon RPC — for real-time WebSocket on OrderFilled events.
    # Loaded from Secret Manager (polygon-alchemy-key) at runtime.
    polygon_alchemy_api_key: str = ""

    # Binance WS (free, no auth)
    binance_ws_url: str = "wss://stream.binance.com:9443/ws/btcusdt@trade"

    # Claude
    anthropic_api_key: str = ""
    claude_advisor_enabled: bool = False
    claude_model: str = "claude-haiku-4-5-20251001"
    claude_daily_call_cap: int = 200          # ~$0.16/day with Haiku

    # Telegram (shared bot with crypto-trader)
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_message_prefix: str = "🎲 [POLY]"

    # Database
    database_url: str = "sqlite:///poly_trader.db"

    # GCS paths (shared bucket with crypto-trader)
    gcs_bucket: str = "crypto-trader-backups-494710"
    gcs_db_path: str = "poly_live/poly.db"
    gcs_heartbeat_path: str = "poly_live/heartbeat.json"
    gcs_halt_flag_path: str = "poly_live/halt.flag"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()


# ── LIVE-MODE SAFETY GATES ──────────────────────────────────────────────────
# When trading_mode == "live", clamp anything that could over-risk on a
# small bankroll. Same defensive pattern as crypto-trader/config/settings.py.
if settings.trading_mode == "live":
    if settings.max_position_pct > 0.05:
        logger.warning(
            f"LIVE: clamping max_position_pct {settings.max_position_pct} → 0.05"
        )
        settings.max_position_pct = 0.05
    # Smart-money copy strategy needs more concurrent slots (5-10) since
    # sports/politics markets resolve in hours/days vs crypto's minutes.
    # Clamp ceiling raised from 3 → 10 on 2026-05-11.
    if settings.max_concurrent_trades > 10:
        logger.warning(
            f"LIVE: clamping max_concurrent_trades {settings.max_concurrent_trades} → 10"
        )
        settings.max_concurrent_trades = 10


# Pull secrets from Secret Manager (no-op if env vars already set)
if not settings.anthropic_api_key:
    s = _fetch_secret_manager("anthropic-api-key")
    if s:
        settings.anthropic_api_key = s
        settings.claude_advisor_enabled = True
        logger.info("Anthropic key loaded from Secret Manager")

if not settings.telegram_bot_token:
    s = _fetch_secret_manager("telegram-bot-token")
    if s:
        settings.telegram_bot_token = s
        logger.info("Telegram bot token loaded from Secret Manager")
if not settings.telegram_chat_id:
    s = _fetch_secret_manager("telegram-chat-id")
    if s:
        settings.telegram_chat_id = s
        logger.info("Telegram chat id loaded from Secret Manager")

# Polygon RPC (Alchemy) — needed for Phase 2 WebSocket scanner. Load always
# (not gated on live mode) since paper-mode bot also benefits from
# real-time signal detection.
if not settings.polygon_alchemy_api_key:
    v = _fetch_secret_manager("polygon-alchemy-key")
    if v:
        settings.polygon_alchemy_api_key = v
        logger.info("Polygon Alchemy key loaded from Secret Manager")

if settings.trading_mode == "live":
    for fld, secret_name in [
        ("polymarket_api_key", "polymarket-api-key"),
        ("polymarket_api_secret", "polymarket-api-secret"),
        ("polymarket_api_passphrase", "polymarket-api-passphrase"),
        ("polymarket_funder_address", "polymarket-funder"),
        ("polymarket_private_key", "polymarket-pk"),
    ]:
        if not getattr(settings, fld):
            v = _fetch_secret_manager(secret_name)
            if v:
                setattr(settings, fld, v)
                # Don't log the value — only the name
                logger.info(f"Polymarket secret loaded: {secret_name}")
