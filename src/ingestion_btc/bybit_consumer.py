"""Bybit V5 public WebSocket consumer with subscribe + app-level ping + reconnect."""
import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator

import websockets

log = logging.getLogger(__name__)

PING_INTERVAL_S = 20  # Bybit: send {"op":"ping"} every 20s
RECONNECT_BACKOFF_MAX_S = 30.0


def transform(t: dict) -> dict:
    """Bybit publicTrade item -> our Delta schema row."""
    return {
        "trade_id": t["i"],
        "symbol": t["s"],
        "price": float(t["p"]),
        "quantity": float(t["v"]),
        "taker_side": t["S"],
        "trade_time": int(t["T"]),
        "cross_sequence": int(t["seq"]),
        "is_block_trade": bool(t.get("BT", False)),
        "ingested_at": int(time.time() * 1000),
    }


async def _ping_loop(ws: websockets.WebSocketClientProtocol) -> None:
    while True:
        await asyncio.sleep(PING_INTERVAL_S)
        await ws.send(json.dumps({"op": "ping", "req_id": "hb"}))


async def stream_trades(ws_url: str, topic: str) -> AsyncIterator[dict]:
    """Yields parsed trades from Bybit publicTrade. Reconnects with exp backoff."""
    backoff = 1.0
    while True:
        try:
            log.info("Connecting %s", ws_url)
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": [topic], "req_id": "sub"}))
                log.info("Subscribed topic=%s", topic)
                backoff = 1.0

                ping_task = asyncio.create_task(_ping_loop(ws))
                try:
                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg.get("topic") != topic:
                            continue  # sub ack, pong, other ops
                        for t in msg.get("data", []):
                            yield transform(t)
                finally:
                    ping_task.cancel()
        except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
            log.warning("WS error: %s. Reconnecting in %.1fs", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX_S)
