"""
Halt-flag check — same GCS-flag pattern as crypto-trader.
Existence of gs://<bucket>/<gcs_halt_flag_path> means: pause new entries.
"""
import json
import logging
import time
import urllib.request
from typing import Optional

from config.settings import settings

logger = logging.getLogger(__name__)

_CACHE = {"checked_at": 0.0, "exists": False}
_CACHE_TTL = 60.0


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


def is_halted() -> bool:
    """Cached 60s. Returns True if the halt flag exists in GCS."""
    now = time.time()
    if now - _CACHE["checked_at"] < _CACHE_TTL:
        return _CACHE["exists"]
    _CACHE["checked_at"] = now
    _CACHE["exists"] = _check_flag_exists()
    return _CACHE["exists"]


import urllib.parse  # noqa: E402
