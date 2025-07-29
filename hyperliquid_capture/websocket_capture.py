#!/usr/bin/env python3

import json
import os
import threading
from collections import defaultdict
from datetime import datetime, timezone
from enum import Enum
from functools import reduce

from config_manager import load_logging_config
from constants import HYPERLIQUID_WSS_URL, TIMER_INTERVAL_SECONDS
from loguru import logger
from utils import convert_timestamp
from websocket import WebSocketApp, WebSocketConnectionClosedException
from writers.parquet_writer import ParquetWriter


class Channel(Enum):
    MARKET_CHANNEL = "l2Book"


class WebsocketOrderBookCapture:
    def __init__(self, channel_type, url):
        self.channel_type = channel_type
        self.url = url
        self.markets = None
        self.wsapp = WebSocketApp(
            self.url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
        self.orderbooks = defaultdict(dict)  # orderbooks per coin
        self.exit_code = 0
        self.writer = ParquetWriter(buffer_size=1e3)

    def on_message(self, ws: WebSocketApp, message: str):
        if message == "PONG":
            logger.debug("Got PONG")
            return

        data = json.loads(message)
        logger.debug("Received data: {}", data)

        if data["channel"] == "subscriptionResponse":
            logger.debug("Sucessfully subscribed to hyperliquid feed")
            return

        if data["channel"] != self.channel_type.value:
            logger.warning("Unknown channel: {}", data["channel"])
            return

        coin = data["data"]["coin"]
        levels = data["data"]["levels"]

        if coin not in self.orderbooks:
            self.orderbooks[coin] = {"bids": [], "asks": []}

        self.orderbooks[coin]["bids"] = [
            {"price": float(b["px"]), "size": float(b["sz"])}
            for i, b in enumerate(levels[0], start=1)
        ]
        self.orderbooks[coin]["asks"] = [
            {"price": float(a["px"]), "size": float(a["sz"])}
            for i, a in enumerate(levels[1], start=1)
        ]

        serialized_book = self.serialize(coin)
        self.writer.write(
            data_type="orderbook",
            data={
                "timestamp": datetime.now(timezone.utc),
                "exchange_timestamp": convert_timestamp(data["data"]["time"]),
                "asset_name": coin,
            }
            | reduce(lambda x, y: x | y, serialized_book, {}),
        )

    def on_error(self, ws: WebSocketApp, error: str):
        if error:
            logger.error("Error: {}", error)
            self.exit_code = 1

    def on_close(self, ws, close_status_code, close_msg):
        logger.debug("Closing connection.")

        del self.writer

    def on_open(self, ws):
        logger.debug("Connected to websocket server.")

        if self.channel_type == Channel.MARKET_CHANNEL:
            req = json.dumps(
                {
                    "method": "subscribe",
                    "subscription": {
                        "type": self.channel_type.value,
                        "coin": "BTC",
                        "nSigFigs": 5,
                    },
                }
            )
        else:
            exit(1)

        logger.debug("Sending websocket request: {}", req, serialize=True)
        ws.send(req)

    def serialize(self, coin, levels=10):
        return [
            *[
                {f"bid_{i + 1}_price": b["price"], f"bid_{i + 1}_size": b["size"]}
                for i, b in enumerate(self.orderbooks[coin]["bids"][:levels])
            ],
            *[
                {f"ask_{i + 1}_price": a["price"], f"ask_{i + 1}_size": a["size"]}
                for i, a in enumerate(self.orderbooks[coin]["asks"][:levels])
            ],
        ]

    def run(self):
        self.wsapp_thread = threading.Thread(target=self.wsapp.run_forever)
        self.wsapp_thread.start()

    def stop(self):
        self.wsapp.close()
        self.wsapp_thread.join()


@logger.catch
def run_capture() -> WebsocketOrderBookCapture:
    # TODO: get candle

    market_connection = WebsocketOrderBookCapture(
        Channel.MARKET_CHANNEL, HYPERLIQUID_WSS_URL
    )

    market_connection.run()

    return market_connection


if __name__ == "__main__":
    load_logging_config()
    run_capture()
