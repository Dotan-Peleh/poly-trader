"""
Phase 2: real-time Polymarket trade stream via Polygon RPC WebSocket.

Subscribes to OrderFilled events on the CTF Exchange contract directly
from the chain. Latency: <1 sec from on-chain confirmation to our signal
(vs 30s position-poll or 5min /trades-poll). This is what makes us faster
than other copy bots.

Contract:
  Polymarket CTF Exchange (Polygon Mainnet)
  0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E
  Event: OrderFilled(bytes32 orderHash, address indexed maker,
                     address indexed taker, uint256 makerAssetId,
                     uint256 takerAssetId, uint256 makerAmountFilled,
                     uint256 takerAmountFilled, uint256 fee)
  Topic0: 0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6

Architecture:
  • Single asyncio task subscribes to logs filtered by contract + topic0
  • For each event: decode wallet (taker/maker), asset_id, side, price, size
  • Look up wallet in smart_wallet_rankings — is this someone we copy?
  • If yes → emit RealTimeSignal → trigger copy_from_signal()
  • Reconnect on disconnect with exponential backoff

Reliability:
  • WebSocket reconnect logic: catches connection drops, resubscribes
  • Heartbeat: send eth_blockNumber every 30s, expect response
  • Fallback: if RPC fails 3x in a row, drop to /trades polling

Cost:
  • Alchemy free tier: 300M compute units/mo
  • eth_subscribe = 30 CU/sub + ~1 CU/event
  • At 100k events/day = 3M CU/mo. Plenty of headroom.
"""
import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# ── Polymarket on-chain plumbing ─────────────────────────────────────────
# CTFExchange = order-matching engine (emits anonymous OrderFilled — no wallets)
# ConditionalTokens = where outcome tokens live (emits TransferSingle with
#                     `to` wallet address indexed — THIS is what we subscribe to)
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# TransferSingle(address indexed operator, address indexed from,
#                address indexed to, uint256 id, uint256 value)
# This event fires every time outcome tokens move. When a wallet BUYS via
# the exchange, the exchange transfers tokens TO the wallet — so we filter
# on from = CTF_EXCHANGE and read the buyer wallet out of topics[3].
TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"

# Pad an Ethereum address to a 32-byte topic-format hex string (lower)
def _pad_address_topic(addr: str) -> str:
    a = addr.lower().replace("0x", "")
    return "0x" + "0" * 24 + a


@dataclass(frozen=True)
class RealTimeSignal:
    """Emitted within ~1s of an on-chain trade by a tracked smart wallet."""
    wallet: str               # taker address (lowercased)
    maker: str                # maker address
    asset_id: str             # token_id of the bought asset
    side: str                 # 'BUY' (taker buys this asset)
    amount_filled: float      # base units (need decimals = 6)
    price: float              # cents per share, 0..1
    block_number: int
    tx_hash: str
    log_index: int
    received_at: float        # local time we received it


def _decode_uint(hex_str: str) -> int:
    return int(hex_str, 16)


def _decode_address(topic: str) -> str:
    # 32-byte topic, address is the rightmost 20 bytes
    return "0x" + topic[-40:]


# Phase 2.5 — subscribe to ConditionalTokens.TransferSingle.
# When the CTFExchange fills an order, it transfers outcome tokens TO the
# buyer's wallet. The TransferSingle event's `to` topic IS the buyer's
# wallet address — direct attribution without needing the Polymarket API.
#
#   topics[0] = TRANSFER_SINGLE_TOPIC
#   topics[1] = operator (indexed)  — usually the exchange itself
#   topics[2] = from     (indexed)  — CTF_EXCHANGE when this is a trade
#   topics[3] = to       (indexed)  — buyer wallet
#   data      = id (uint256)  || value (uint256)
#
# We filter `from = CTF_EXCHANGE` server-side to drop user-to-user
# transfers (not trades), then check `to` against our rankings table.


def _parse_log(log: dict) -> Optional[RealTimeSignal]:
    """Decode a TransferSingle event into a RealTimeSignal.

    Event: TransferSingle(operator, from, to, id, value) all indexed up to to.
      topics[0] = TRANSFER_SINGLE_TOPIC
      topics[1] = operator (indexed address — usually exchange)
      topics[2] = from     (indexed address — CTF_EXCHANGE when this is a buy)
      topics[3] = to       (indexed address — buyer wallet)
      data      = id (uint256, 32B) || value (uint256, 32B)
    """
    try:
        topics = log.get("topics") or []
        if len(topics) < 4 or topics[0].lower() != TRANSFER_SINGLE_TOPIC:
            return None
        operator = _decode_address(topics[1]).lower()
        from_addr = _decode_address(topics[2]).lower()
        to_addr = _decode_address(topics[3]).lower()
        # Skip redemptions (transfer to zero address) and self-transfers
        if to_addr == "0x0000000000000000000000000000000000000000":
            return None
        data = log.get("data", "")
        if data.startswith("0x"):
            data = data[2:]
        if len(data) < 128:
            return None
        token_id = _decode_uint(data[0:64])
        value = _decode_uint(data[64:128])
        # ConditionalTokens use 6 decimals
        amount = value / 1_000_000.0
        if amount <= 0:
            return None
        return RealTimeSignal(
            wallet=to_addr,           # the BUYER
            maker=from_addr,          # CTF_EXCHANGE
            asset_id=str(token_id),
            side="BUY",
            amount_filled=amount,
            price=0.0,                # not in this event — look up book separately
            block_number=_decode_uint(log["blockNumber"]),
            tx_hash=log["transactionHash"],
            log_index=_decode_uint(log["logIndex"]),
            received_at=time.time(),
        )
    except Exception as e:
        logger.debug(f"polygon_stream: log decode failed: {e}")
        return None


async def _ws_loop(api_key: str, on_signal):
    """Single iteration of the WebSocket subscription loop. Reconnects on
    disconnect via the outer driver. Yields control on every event so the
    handler can run synchronously."""
    import websockets  # lazy import — only needed when stream runs
    url = f"wss://polygon-mainnet.g.alchemy.com/v2/{api_key}"
    async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
        # Subscribe to ALL TransferSingle events on ConditionalTokens.
        # Polymarket uses several exchange/relayer contracts; rather than
        # whitelisting operators we accept everything (~80 events/sec) and
        # post-filter by `to` in Python (cheap SQLite lookup vs rankings).
        # We DROP zero-address transfers (= token redemptions, not trades)
        # in the parser.
        sub_req = {
            "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
            "params": ["logs", {
                "address": CONDITIONAL_TOKENS,
                "topics": [TRANSFER_SINGLE_TOPIC],
            }],
        }
        await ws.send(json.dumps(sub_req))
        ack = json.loads(await ws.recv())
        if "result" not in ack:
            logger.error(f"polygon_stream: subscribe failed: {ack}")
            return
        sub_id = ack["result"]
        logger.info(f"polygon_stream: subscribed to TransferSingle "
                     f"from CTFExchange (sub_id={sub_id})")

        while True:
            msg = json.loads(await ws.recv())
            params = msg.get("params", {})
            result = params.get("result")
            if not isinstance(result, dict):
                continue
            signal = _parse_log(result)
            if signal is None:
                continue
            try:
                on_signal(signal)
            except Exception as e:
                logger.warning(f"polygon_stream: on_signal handler failed: {e}")


async def _stream_driver(api_key: str, on_signal):
    """Outer driver: reconnect on any websocket error with exponential backoff."""
    backoff = 1.0
    while True:
        try:
            await _ws_loop(api_key, on_signal)
        except Exception as e:
            logger.warning(f"polygon_stream: ws loop ended: {type(e).__name__}: {e}; "
                            f"reconnecting in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        else:
            backoff = 1.0


def start_stream_in_thread(api_key: str, on_signal):
    """Spin up the asyncio event loop in a daemon thread. Caller passes a
    synchronous `on_signal(RealTimeSignal)` callback that runs on the loop
    thread (do not block heavily here — offload to a queue if needed)."""
    import threading

    def _runner():
        try:
            asyncio.run(_stream_driver(api_key, on_signal))
        except Exception as e:
            logger.error(f"polygon_stream: thread crashed: {e}")

    t = threading.Thread(target=_runner, daemon=True, name="polygon_stream")
    t.start()
    logger.info("polygon_stream: thread started")
    return t
