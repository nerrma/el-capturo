#!/usr/bin/env python3

import json
from datetime import datetime, timezone

from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from config_manager import load_logging_config
from loguru import logger
from writers.parquet_writer import ParquetWriter


class WebsocketOrderBookCapture:
    def __init__(self):
        self.writer = ParquetWriter(buffer_size=1e4)

    def on_close(self, _):
        logger.debug("Closing connection.")

        del (
            self.writer
        )  # manually flush contents (avoids weird python edge case where program dies before writer)

        exit(0)

    def on_book_ticker(self, _, message: str):
        if "result" not in message:
            logger.debug("Got message: {}", message)
            message = json.loads(message)
            self.writer.write(
                data_type="orderbook",
                data={
                    "timestamp": datetime.now(timezone.utc),
                    "asset_name": message["s"],
                    "bid_price": float(message["b"]),
                    "bid_size": float(message["B"]),
                    "ask_price": float(message["a"]),
                    "ask_size": float(message["A"]),
                },
            )


@logger.catch
def run_capture() -> SpotWebsocketStreamClient:
    client = WebsocketOrderBookCapture()
    binance_connection = SpotWebsocketStreamClient(
        on_message=client.on_book_ticker, on_close=client.on_close
    )

    binance_connection.book_ticker(symbol="btcusdt")

    return binance_connection


if __name__ == "__main__":
    load_logging_config()

    binance_connection = None
    try:
        binance_connection = run_capture()
        while True:
            pass
    except KeyboardInterrupt:
        pass
    except EOFError:
        pass

    if binance_connection is not None:
        binance_connection.stop()
