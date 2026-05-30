"""Pipeline: Bybit V5 WS -> ZeroBus SDK (gRPC) -> Delta.

SDK handles: auth/token refresh, batching (max_inflight_records),
retry/recovery, backpressure, durable ack.
"""
import asyncio
import logging
import signal
import time

from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import (
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
)

from ingestion_btc.bybit_consumer import stream_trades
from ingestion_btc.config import Config, parse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
log = logging.getLogger("ingestion_btc")

HEARTBEAT_S = 60.0


async def _ingest(cfg: Config) -> None:
    sdk = ZerobusSdk(cfg.zerobus_endpoint, cfg.workspace_url)
    options = StreamConfigurationOptions(
        record_type=RecordType.JSON,
        max_inflight_records=10_000,
        recovery=True,
        recovery_retries=5,
    )
    stream = await sdk.create_stream(
        cfg.client_id, cfg.client_secret, TableProperties(cfg.fqn), options
    )
    log.info("stream created table=%s", cfg.fqn)

    sent = 0
    last_hb_at = time.monotonic()
    last_hb_sent = 0
    last_lag_ms = 0
    last_trade_id = ""
    first_logged = False

    try:
        async for trade in stream_trades(cfg.ws_url, cfg.topic):
            await stream.ingest_record_offset(trade)
            sent += 1
            last_trade_id = trade["trade_id"]
            last_lag_ms = trade["ingested_at"] - trade["trade_time"]

            if not first_logged:
                log.info(
                    "first trade ingested trade_id=%s price=%s lag_ms=%d",
                    last_trade_id, trade["price"], last_lag_ms,
                )
                first_logged = True

            now = time.monotonic()
            if now - last_hb_at >= HEARTBEAT_S:
                window_s = now - last_hb_at
                window_sent = sent - last_hb_sent
                log.info(
                    "hb sent=%d window=%d/%.0fs rate=%.2f/s lag_ms=%d last_trade_id=%s",
                    sent, window_sent, window_s, window_sent / window_s,
                    last_lag_ms, last_trade_id,
                )
                last_hb_at = now
                last_hb_sent = sent
    finally:
        log.info("flushing sent=%d last_trade_id=%s", sent, last_trade_id)
        await stream.flush()
        await stream.close()
        log.info("stream closed sent=%d last_trade_id=%s", sent, last_trade_id)


async def run_async() -> None:
    cfg = parse()
    log.info("table=%s endpoint=%s ws=%s topic=%s",
             cfg.fqn, cfg.zerobus_endpoint, cfg.ws_url, cfg.topic)

    task = asyncio.create_task(_ingest(cfg), name="ingest")
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, task.cancel)
        except NotImplementedError:
            pass  # Windows

    try:
        await task
    except asyncio.CancelledError:
        log.info("ingest cancelled by signal")


def run() -> None:
    try:
        asyncio.run(run_async())
    except RuntimeError as e:
        if "running event loop" not in str(e):
            raise
        import nest_asyncio

        nest_asyncio.apply()
        asyncio.get_event_loop().run_until_complete(run_async())


if __name__ == "__main__":
    run()
