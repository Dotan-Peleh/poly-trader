"""
Polymarket wallet snapshot — pulls real USDC balance + open positions +
recent fills from Polymarket using the live ClobClient. Writes a JSON
snapshot to GCS every 5 min so the existing dashboard can read it
without needing direct API access.

Endpoints used:
  • py-clob-client.get_balance_allowance() — USDC balance + allowance
  • httpx GET /trades?market=...&user=... — recent fills
  • httpx GET /data-api/positions?user=... — open positions

Snapshot path: gs://crypto-trader-backups-494710/poly_live/wallet.json
"""
import json
import logging
import time
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Optional

import httpx

from config.settings import settings

logger = logging.getLogger(__name__)


_data_api_base = "https://data-api.polymarket.com"

# Polymarket migrated their CLOB collateral from USDC.e to pUSD (Polymarket
# USD). The CLOB API's get_balance_allowance(asset_type=COLLATERAL) still
# returns the legacy USDC.e number — for active accounts that's $0. Real
# trading capital sits as pUSD on Polygon. We read it directly so the
# dashboard / bot see the same number as the Polymarket UI.
PUSD_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
POLYGON_RPCS = (
    "https://polygon.gateway.tenderly.co",
    "https://rpc.ankr.com/polygon",
    "https://polygon-mainnet.public.blastapi.io",
    "https://polygon-rpc.com",
)


def _fetch_pusd_balance(funder: str) -> Optional[float]:
    """Read the funder's pUSD ERC-20 balance via Polygon JSON-RPC. Returns
    USD float (pUSD has 6 decimals). Tries multiple public RPCs and
    returns the first non-error result. Returns None on total failure."""
    if not funder or not funder.startswith("0x") or len(funder) != 42:
        return None
    padded = funder[2:].lower().rjust(64, "0")
    data = "0x70a08231" + padded  # balanceOf(address) selector
    payload = json.dumps({
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": PUSD_CONTRACT, "data": data}, "latest"],
        "id": 1,
    }).encode("utf-8")
    for rpc in POLYGON_RPCS:
        try:
            req = urllib.request.Request(
                rpc, data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=8) as r:
                resp = json.loads(r.read().decode("utf-8"))
            hex_result = resp.get("result")
            if hex_result and hex_result.startswith("0x") and len(hex_result) > 2:
                return int(hex_result, 16) / 1_000_000.0  # pUSD has 6 decimals
        except Exception as e:
            logger.debug(f"pUSD RPC {rpc} failed: {e}")
            continue
    return None


def _gcp_token() -> Optional[str]:
    # 1) GCE metadata server (works on a GCE VM, fails fast on laptop)
    try:
        url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
        req = urllib.request.Request(url, headers={"Metadata-Flavor": "Google"})
        with urllib.request.urlopen(req, timeout=2) as r:
            return json.loads(r.read().decode("utf-8"))["access_token"]
    except Exception:
        pass
    # 2) ADC fallback so the wallet writer works on a laptop too
    #    (gcloud auth application-default login). Without this the
    #    dashboard's wallet block goes stale when the bot runs at home.
    try:
        import google.auth
        from google.auth.transport.requests import Request as _GReq
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        creds.refresh(_GReq())
        return creds.token
    except Exception:
        return None


def _addresses_of_interest() -> dict:
    """Return both the v1 Magic Link proxy AND the v2 deposit wallet,
    so the snapshot covers funds wherever they happen to live during
    the migration period."""
    addrs: dict = {
        "legacy_proxy": getattr(settings, "polymarket_funder_address", "") or "",
        "deposit_wallet": "",
    }
    try:
        from execution.polymarket_orders import deposit_wallet_address
        addrs["deposit_wallet"] = deposit_wallet_address() or ""
    except Exception as e:
        logger.debug(f"_addresses_of_interest: derive failed: {e}")
    return addrs


def _fetch_balance() -> dict:
    """Read the trading balance Polymarket actually uses today (pUSD).

    Returns five signals so the dashboard / sizer can pick the right one:
      • usdc           = LEGACY CLOB USDC.e collateral (~always 0 today)
      • pusd_legacy    = pUSD on the OLD Magic Link proxy
      • pusd_deposit   = pUSD on the NEW v2 deposit wallet
      • pusd           = max(legacy, deposit)  — easiest "what we have"
      • effective      = pusd_deposit if > 0 else pusd_legacy (the value
                         actually usable for v2 trading; legacy funds
                         can't trade until migrated)
    """
    addrs = _addresses_of_interest()
    out: dict = {
        "raw": None, "usdc": None,
        "pusd_legacy": None, "pusd_deposit": None,
        "pusd": None, "effective": None,
        "legacy_proxy": addrs.get("legacy_proxy"),
        "deposit_wallet": addrs.get("deposit_wallet"),
    }

    # On-chain pUSD on both addresses
    try:
        if addrs.get("legacy_proxy"):
            out["pusd_legacy"] = _fetch_pusd_balance(addrs["legacy_proxy"])
    except Exception as e:
        logger.debug(f"_fetch_balance (legacy pUSD) failed: {e}")
    try:
        if addrs.get("deposit_wallet"):
            out["pusd_deposit"] = _fetch_pusd_balance(addrs["deposit_wallet"])
    except Exception as e:
        logger.debug(f"_fetch_balance (deposit pUSD) failed: {e}")

    # Highest known balance — useful header on the dashboard
    candidates = [v for v in (out["pusd_legacy"], out["pusd_deposit"]) if v is not None]
    out["pusd"] = max(candidates) if candidates else None

    # The v2 CLOB will only match orders backed by the deposit wallet.
    # Legacy funds are "stuck" until the user migrates them via the
    # Polymarket UI. effective = what the bot can actually deploy.
    if out["pusd_deposit"] is not None and out["pusd_deposit"] > 0:
        out["effective"] = out["pusd_deposit"]
    elif out["pusd_legacy"] is not None:
        out["effective"] = 0.0  # legacy funds exist but cannot trade on v2
    else:
        out["effective"] = None

    return out


def _fetch_positions() -> list:
    """Open positions for the user — query BOTH the legacy proxy and
    the v2 deposit wallet, since funds/positions can live in either
    during the migration period."""
    addrs = _addresses_of_interest()
    out: list = []
    for label, addr in addrs.items():
        if not addr:
            continue
        try:
            url = f"{_data_api_base}/positions?user={addr}&sizeThreshold=0.01"
            with httpx.Client(timeout=15.0) as c:
                r = c.get(url)
                r.raise_for_status()
                data = r.json()
                if isinstance(data, list):
                    # Tag each row so the dashboard can group v1 vs v2
                    for p in data:
                        if isinstance(p, dict):
                            p["__source"] = label
                    out.extend(data)
        except Exception as e:
            logger.debug(f"_fetch_positions[{label}] failed: {e}")
    return out


def _fetch_recent_trades(limit: int = 50) -> list:
    """Recent fills for the user. Uses the authenticated CLOB v2 endpoint."""
    try:
        from execution.polymarket_orders import _get_client
        client = _get_client()
        # v2 SDK exposes get_market_trades_events / open_orders etc;
        # the historical trades endpoint may not be there yet. Best-
        # effort: return empty list rather than raise.
        if hasattr(client, "get_trades"):
            try:
                trades = client.get_trades(params={"limit": limit})
            except TypeError:
                trades = client.get_trades()
            if isinstance(trades, list):
                return trades[:limit]
            if isinstance(trades, dict) and "data" in trades:
                return trades["data"][:limit]
        return []
    except Exception as e:
        logger.debug(f"_fetch_recent_trades failed: {e}")
        return []


def write_wallet_snapshot():
    """Build a wallet snapshot JSON and upload to GCS for the dashboard.
    Runs in BOTH paper and live mode — your real wallet balance + open
    positions exist regardless of bot mode. Skips only if creds are
    missing (so a fresh deployment without secrets doesn't error)."""
    if not (settings.polymarket_api_key and settings.polymarket_private_key):
        return  # not configured — skip silently
    _addrs = _addresses_of_interest()
    snapshot = {
        "ts": datetime.utcnow().isoformat() + "Z",
        # Keep "funder" for legacy dashboard compatibility — points at
        # the v1 Magic Link proxy. New fields below expose both
        # legacy and v2 deposit-wallet addresses explicitly.
        "funder": _addrs.get("legacy_proxy"),
        "legacy_proxy": _addrs.get("legacy_proxy"),
        "deposit_wallet": _addrs.get("deposit_wallet"),
        "balance": _fetch_balance(),
        "positions": _fetch_positions(),
        "recent_trades": _fetch_recent_trades(limit=20),
    }

    body = json.dumps(snapshot, default=str).encode("utf-8")

    token = _gcp_token()
    if not token:
        logger.warning("polymarket_wallet: no GCP token; skipping upload")
        return

    object_name = "poly_live/wallet.json"
    upload_url = (
        f"https://storage.googleapis.com/upload/storage/v1/b/"
        f"{settings.gcs_bucket}/o?uploadType=media&name="
        f"{urllib.parse.quote(object_name)}"
    )
    try:
        req = urllib.request.Request(
            upload_url,
            data=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
            },
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10).read()
        bal_block = snapshot.get("balance", {}) or {}
        n_pos = len(snapshot.get("positions") or [])
        n_trd = len(snapshot.get("recent_trades") or [])
        logger.info(
            f"wallet_snapshot: pUSD=${bal_block.get('pusd')} "
            f"usdc.e=${bal_block.get('usdc')} "
            f"effective=${bal_block.get('effective')} "
            f"positions={n_pos} trades={n_trd}"
        )
    except Exception as e:
        logger.warning(f"wallet_snapshot upload failed: {e}")
