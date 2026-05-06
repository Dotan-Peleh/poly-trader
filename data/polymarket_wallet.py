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


def _gcp_token() -> Optional[str]:
    try:
        url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
        req = urllib.request.Request(url, headers={"Metadata-Flavor": "Google"})
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read().decode("utf-8"))["access_token"]
    except Exception:
        return None


def _fetch_balance() -> dict:
    """USDC balance + allowance via the ClobClient (HMAC-authenticated)."""
    try:
        from execution.polymarket_orders import _get_client
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        client = _get_client()
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        resp = client.get_balance_allowance(params)
        # Polymarket returns balance + allowance in micro-USDC (6 decimals)
        bal = resp.get("balance") if isinstance(resp, dict) else None
        if bal is not None:
            try:
                bal = float(bal) / 1_000_000.0  # USDC has 6 decimals
            except Exception:
                pass
        return {"raw": resp, "usdc": bal}
    except Exception as e:
        logger.warning(f"_fetch_balance failed: {e}")
        return {"raw": None, "usdc": None, "error": str(e)[:200]}


def _fetch_positions() -> list:
    """Open positions for the funder. Public Data API (no auth needed)."""
    funder = settings.polymarket_funder_address
    if not funder:
        return []
    try:
        url = f"{_data_api_base}/positions?user={funder}&sizeThreshold=0.01"
        with httpx.Client(timeout=15.0) as c:
            r = c.get(url)
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []
    except Exception as e:
        logger.debug(f"_fetch_positions failed: {e}")
        return []


def _fetch_recent_trades(limit: int = 50) -> list:
    """Recent fills for the user. Uses the authenticated CLOB endpoint."""
    try:
        from execution.polymarket_orders import _get_client
        client = _get_client()
        # py-clob-client exposes get_trades — public method, takes optional params
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
    """Build a wallet snapshot JSON and upload to GCS for the dashboard."""
    if settings.trading_mode != "live":
        # In paper mode, the wallet doesn't apply
        return
    snapshot = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "funder": settings.polymarket_funder_address,
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
        bal = snapshot.get("balance", {}).get("usdc")
        n_pos = len(snapshot.get("positions") or [])
        n_trd = len(snapshot.get("recent_trades") or [])
        logger.info(
            f"wallet_snapshot: balance=${bal} positions={n_pos} trades={n_trd}"
        )
    except Exception as e:
        logger.warning(f"wallet_snapshot upload failed: {e}")
