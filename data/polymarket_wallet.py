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
    try:
        url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
        req = urllib.request.Request(url, headers={"Metadata-Flavor": "Google"})
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read().decode("utf-8"))["access_token"]
    except Exception:
        return None


def _fetch_balance() -> dict:
    """Read the trading balance Polymarket actually uses today (pUSD).

    Returns three signals so the dashboard / sizer can pick the right one:
      • usdc       = LEGACY CLOB USDC.e collateral (almost always 0 today)
      • pusd       = REAL trading capital, read on-chain from Polygon
      • effective  = pusd if non-None else usdc (the value to size against)
    """
    out: dict = {"raw": None, "usdc": None, "pusd": None, "effective": None}
    # 1) Legacy USDC.e via the CLOB (kept for compatibility / debugging).
    try:
        from execution.polymarket_orders import _get_client
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        client = _get_client()
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        resp = client.get_balance_allowance(params)
        bal = resp.get("balance") if isinstance(resp, dict) else None
        if bal is not None:
            try:
                bal = float(bal) / 1_000_000.0
            except Exception:
                pass
        out["raw"] = resp
        out["usdc"] = bal
    except Exception as e:
        logger.debug(f"_fetch_balance (CLOB USDC.e) failed: {e}")
        out["error"] = str(e)[:200]

    # 2) Real pUSD ERC-20 balance from Polygon RPC.
    try:
        from config.settings import settings as _s
        funder = getattr(_s, "polymarket_funder_address", "")
        pusd = _fetch_pusd_balance(funder) if funder else None
        out["pusd"] = pusd
    except Exception as e:
        logger.debug(f"_fetch_balance (pUSD on-chain) failed: {e}")

    out["effective"] = out["pusd"] if out["pusd"] is not None else out["usdc"]
    return out


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
    """Build a wallet snapshot JSON and upload to GCS for the dashboard.
    Runs in BOTH paper and live mode — your real wallet balance + open
    positions exist regardless of bot mode. Skips only if creds are
    missing (so a fresh deployment without secrets doesn't error)."""
    if not (settings.polymarket_api_key and settings.polymarket_private_key):
        return  # not configured — skip silently
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
