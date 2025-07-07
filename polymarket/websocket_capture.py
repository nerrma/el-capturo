#!/usr/bin/env python3

import json
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from enum import Enum
from functools import reduce

from config_manager import load_logging_config
from constants import POLYMARKET_WSS_URL, TIMER_INTERVAL_SECONDS
from loguru import logger
from websocket import WebSocketApp, WebSocketConnectionClosedException
from writers.parquet_writer import ParquetWriter

from polymarket.events.parsers import (
    parse_book_event,
    parse_last_trade_price,
    parse_price_change_event,
)
from polymarket.events.types import BookEvent, LastTradePrice, PriceChangeEvent
from polymarket.market_info import get_hourly_market_info_for, MarketInfo
from polymarket.orderbook.orderbook import Orderbook


class Channel(Enum):
    MARKET_CHANNEL = "market"
    USER_CHANNEL = "user"


class WebsocketOrderBookCapture:
    def __init__(self, channel_type, url, tokens, auth):
        self.channel_type = channel_type
        self.url = url
        self.tokens = {t.token_id: t for t in tokens}
        self.auth = auth
        self.markets = None
        furl = url + "/ws/" + channel_type.value
        self.wsapp = WebSocketApp(
            furl,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
        self.orderbooks = defaultdict(Orderbook)  # orderbooks per asset_id
        self.exit_code = 0
        self.writer = ParquetWriter(buffer_size=1e3)
        self.ping_thread = None

    def on_message(self, ws: WebSocketApp, message: str):
        logger.debug("Got message: {}", message)

        if message == "PONG":
            logger.debug("Got PONG")
            return

        messages = json.loads(message)
        logger.debug("evaluated to {}", messages)

        for message in messages:
            match message["event_type"]:
                case "book":
                    event = parse_book_event(message)
                case "price_change":
                    event = parse_price_change_event(message)
                case "tick_size_change":
                    event = None
                    pass
                case "last_trade_price":
                    event = parse_last_trade_price(message)
                    pass
                case _:
                    logger.warning("Unknown message type: {}", message["event_type"])
                    return

            logger.debug("Parsed event {}", event, serialize=True)
            match event:
                case BookEvent() | PriceChangeEvent():
                    self.orderbooks[message["asset_id"]].apply_event(event)
                    serialized_book = self.orderbooks[message["asset_id"]].serialize()
                    logger.debug(
                        "Orderbook for {} is {}",
                        message["asset_id"],
                        serialized_book,
                        serialize=True,
                    )

                    self.writer.write(
                        data_type="orderbook",
                        data={
                            "timestamp": datetime.now(timezone.utc),
                            "exchange_timestamp": event.timestamp,
                            "asset_id": message["asset_id"],
                            "asset_name": self.tokens[message["asset_id"]].token_name,
                            "event_type": message["event_type"],
                        }
                        | reduce(lambda x, y: x | y, serialized_book, {}),
                    )
                case LastTradePrice():
                    self.writer.write(
                        data_type="trade",
                        data={
                            "timestamp": datetime.now(timezone.utc),
                            "exchange_timestamp": event.timestamp,
                            "asset_id": message["asset_id"],
                            "asset_name": self.tokens[message["asset_id"]].token_name,
                            "side": event.side.value,
                            "price": event.price,
                            "size": event.size,
                        },
                    )

    def on_error(self, ws: WebSocketApp, error: str):
        if error:
            logger.error("Error: {}", error)
            self.exit_code = 1

    def on_close(self, ws: WebSocketApp, close_status_code: int, close_msg: str):
        logger.debug("Closing connection.")

        if self.ping_thread:
            self.ping_thread.cancel()

        del self.writer

    def on_open(self, ws: WebSocketApp):
        match self.channel_type:
            case Channel.MARKET_CHANNEL:
                req = json.dumps(
                    {
                        "assets_ids": list(self.tokens.keys()),
                        "type": self.channel_type.value,
                    }
                )
            case Channel.USER_CHANNEL:
                assert self.auth
                assert self.markets
                req = json.dumps(
                    {
                        "markets": self.markets,
                        "type": self.channel_type.value,
                        "auth": self.auth,
                    }
                )
            case _:
                exit(1)

        logger.debug("Sending websocket request: {}", req, serialize=True)
        ws.send(req)

        self._run_next_ping(ws)

    def _run_next_ping(self, ws: WebSocketApp):
        self.ping_thread = threading.Timer(
            TIMER_INTERVAL_SECONDS, self.ping, args=(ws,)
        )
        self.ping_thread.start()

    def ping(self, ws: WebSocketApp):
        if not ws.has_errored:
            try:
                ws.send("PING")
                self._run_next_ping(ws)
            except WebSocketConnectionClosedException as e:
                logger.warning("Caught exception {}", e)

    def run(self):
        self.wsapp_thread = threading.Thread(target=self.wsapp.run_forever)
        self.wsapp_thread.start()

    def stop(self):
        self.wsapp.close()
        self.ping_thread.cancel()
        self.wsapp_thread.join()


@logger.catch
def run_capture() -> (WebsocketOrderBookCapture, MarketInfo):
    api_key = os.getenv("API_KEY")
    api_secret = os.getenv("API_SECRET")
    api_passphrase = os.getenv("PASSPHRASE")

    market_info = get_hourly_market_info_for(market="bitcoin-up-or-down")

    assert len(market_info) >= 1, "No market info retrieved!"

    if len(market_info) > 1:
        logger.warning("More than 1 market read, got {}", len(market_info))

    tokens = market_info[0].tokens
    auth = {"apiKey": api_key, "secret": api_secret, "passphrase": api_passphrase}

    market_connection = WebsocketOrderBookCapture(
        Channel.MARKET_CHANNEL, POLYMARKET_WSS_URL, tokens, auth
    )

    market_connection.run()

    return market_connection, market_info[0]


if __name__ == "__main__":
    load_logging_config()
    run_capture()
