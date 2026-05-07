"""
Halt-flag check + mode override — both controlled via GCS objects so
the dashboard can flip them without restarting the bot.

  • is_halted() — pause new entries (existing)
  • effective_mode() — overrides settings.trading_mode at runtime:
      gs://.../poly_live/mode.txt content "paper" or "live"
      (absent → fall back to settings.trading_mode)
"""
import json
import logging
import time
import urllib.parse
import urllib.request
from typing import Optional

from config.settings import settings

logger = logging.getLogger(__name__)

_HALT_CACHE = {"checked_at": 0.0, "exists": False}
_MODE_CACHE = {"checked_at": 0.0, "value": None}
_CACHE_TTL = 60.0
_MODE_OBJECT = "poly_live/mode.txt"


def _gcp_token() -> Optional[str]:
    try:
        url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
        req = urllib.request.Request(url, headers={"Metadata-Flavor": "Google"})
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read().decode("utf-8"))["access_token"]
    except Exception:
        return None


def _check_flag_exists() -> bool:
    token = _gcp_token()
    if not token:
        return False
    try:
        url = (
            f"https://storage.googleapis.com/storage/v1/b/{settings.gcs_bucket}"
            f"/o/{urllib.parse.quote(settings.gcs_halt_flag_path, safe='')}"
        )
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        urllib.request.urlopen(req, timeout=5).read()
        return True
    except Exception:
        return False


def _read_mode_object() -> Optional[str]:
    """Download mode.txt content (or None if missing/error)."""
    token = _gcp_token()
    if not token:
        return None
    try:
        url = (
            f"https://storage.googleapis.com/storage/v1/b/{settings.gcs_bucket}"
            f"/o/{urllib.parse.quote(_MODE_OBJECT, safe='')}?alt=media"
        )
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=5) as r:
            content = r.read().decode("utf-8").strip().lower()
            if content in ("paper", "live"):
                return content
    except Exception:
        pass
    return None


def is_halted() -> bool:
    """Cached 60s. Returns True if the halt flag exists in GCS."""
    now = time.time()
    if now - _HALT_CACHE["checked_at"] < _CACHE_TTL:
        return _HALT_CACHE["exists"]
    _HALT_CACHE["checked_at"] = now
    _HALT_CACHE["exists"] = _check_flag_exists()
    return _HALT_CACHE["exists"]


def effective_mode() -> str:
    """Returns 'paper' or 'live'. Reads gs://.../poly_live/mode.txt
    if present; falls back to settings.trading_mode otherwise.
    Cached 60s."""
    now = time.time()
    if now - _MODE_CACHE["checked_at"] < _CACHE_TTL:
        cached = _MODE_CACHE["value"]
        if cached:
            return cached
    _MODE_CACHE["checked_at"] = now
    override = _read_mode_object()
    final = override or settings.trading_mode
    _MODE_CACHE["value"] = final
    return final
