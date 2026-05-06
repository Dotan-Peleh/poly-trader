"""
Heartbeat to GCS — written every 5 min by main.py scheduler.
The crypto-trader watchdog Cloud Function can be reused with a second
trigger schedule pointing at a different heartbeat path.

Same direct-REST pattern as crypto-trader/main.heartbeat_tick to bypass
the urllib3/SSL conflict in google-cloud-storage on Debian Python 3.11.
"""
import json
import logging
import urllib.request
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from data.storage import engine, BtcTick, PolymarketMarket
from config.settings import settings

logger = logging.getLogger(__name__)


def _gcp_token() -> Optional[str]:
    try:
        url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
        req = urllib.request.Request(url, headers={"Metadata-Flavor": "Google"})
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read().decode("utf-8"))["access_token"]
    except Exception as e:
        logger.debug(f"_gcp_token failed: {e}")
        return None


def write_heartbeat():
    """Build a small status payload and POST to GCS."""
    try:
        with Session(engine) as session:
            tick_count = session.query(BtcTick).count()
            last_tick = (session.query(BtcTick.ts, BtcTick.price)
                         .order_by(BtcTick.ts.desc()).first())
            active_markets = (session.query(PolymarketMarket)
                              .filter(PolymarketMarket.state == "active").count())

        payload = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "tick_count": int(tick_count),
            "last_tick_ts": last_tick[0].isoformat() + "Z" if last_tick else None,
            "last_btc_price": float(last_tick[1]) if last_tick else None,
            "active_markets": int(active_markets),
            "mode": settings.trading_mode,
        }

        token = _gcp_token()
        if not token:
            logger.warning("heartbeat: no GCP token; skipping upload")
            return

        upload_url = (
            f"https://storage.googleapis.com/upload/storage/v1/b/"
            f"{settings.gcs_bucket}/o?uploadType=media&name="
            f"{urllib.parse.quote(settings.gcs_heartbeat_path)}"
        )
        body = json.dumps(payload).encode("utf-8")
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
    except Exception as e:
        logger.warning(f"heartbeat write failed: {e}")


# Late import to allow above name use
import urllib.parse  # noqa: E402
