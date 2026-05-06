"""
Binance WebSocket trade stream → SQLite.

Subscribes to btcusdt@trade (every executed trade on Binance spot).
Each tick is one trade event with:
  E: event time (ms)
  p: price
  q: quantity
  m: is_buyer_maker (True = sell-side aggression)

Backpressure strategy: batch inserts every N ticks or every M seconds,
whichever comes first. Single-row inserts at >100 ticks/s fragment SQLite
write-ahead log. We aim for sub-second write latency without pathological
fsync cost.

Failure modes handled:
  - WS disconnect → reconnect with exponential backoff capped at 60s
  - DB write failure → log + drop the batch (we'd rather miss ticks than
    crash; the model uses recent ticks only)
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from collections import deque

from data.storage import save_btc_tick
from config.settings import settings

logger = logging.getLogger(__name__)


BATCH_FLUSH_TICKS = 50      # write to DB every 50 ticks
BATCH_FLUSH_SEC = 2.0       # or every 2 seconds


async def _stream_loop():
    """One iteration of the WS loop. Caller wraps with reconnect."""
    import websockets

    backoff = 1.0
    while True:
        try:
            async with websockets.connect(
                settings.binance_ws_url, ping_interval=15, ping_timeout=20
            ) as ws:
                logger.info(f"Binance WS connected: {settings.binance_ws_url}")
                backoff = 1.0  # reset on successful connection

                buffer: deque = deque()
                last_flush = time.time()

                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    # Trade event: {e: 'trade', E: <ts ms>, s: 'BTCUSDT',
                    #               p: '<price>', q: '<qty>', m: <bool>}
                    if data.get("e") != "trade":
                        continue
                    ts = datetime.fromtimestamp(data["E"] / 1000.0, tz=timezone.utc).replace(tzinfo=None)
                    price = float(data["p"])
                    qty = float(data.get("q", 0))
                    is_buyer_maker = bool(data.get("m", False))
                    buffer.append((ts, price, qty, is_buyer_maker))

                    if (len(buffer) >= BATCH_FLUSH_TICKS
                            or (time.time() - last_flush) >= BATCH_FLUSH_SEC):
                        # Flush batch — best-effort
                        try:
                            for t in buffer:
                                save_btc_tick(*t)
                            logger.debug(f"flushed {len(buffer)} ticks to DB")
                        except Exception as e:
                            logger.error(f"DB flush failed (dropping batch): {e}")
                        buffer.clear()
                        last_flush = time.time()

        except Exception as e:
            logger.warning(f"Binance WS error ({type(e).__name__}: {e}); reconnecting in {backoff:.1f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


def run_forever():
    """Blocking entry point. Call from main.py in a background thread."""
    while True:
        try:
            asyncio.run(_stream_loop())
        except KeyboardInterrupt:
            return
        except Exception as e:
            logger.error(f"WS event loop crashed: {e}; restart in 5s")
            time.sleep(5)
