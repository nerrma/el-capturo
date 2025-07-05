#!/usr/bin/env python3

import json
import os
import threading
import time
from enum import Enum

from config_manager import load_logging_config
from constants import POLYMARKET_WSS_URL
from dotenv import load_dotenv
from loguru import logger
from websocket import WebSocket, WebSocketApp

from polymarket.market_info import get_hourly_market_info_for
from polymarket.events.parsers import parse_book_event, parse_price_change_event


class Channel(Enum):
    MARKET_CHANNEL = "market"
    USER_CHANNEL = "user"


class WebSocketOrderBook:
    def __init__(self, channel_type, url, asset_ids, auth):
        self.channel_type = channel_type
        self.url = url
        self.asset_ids = asset_ids
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
        self.orderbook = {}  # TODO: track internal orderbook state and apply `Changes`
        self.exit_code = 0
        self.ping_thread = None

    def on_message(self, ws: WebSocketApp, message: str):
        logger.spam("Got message: {}", message)

        if message == "PONG":
            logger.debug("Got PONG")
            return

        message = json.loads(message)[0]

        match message["event_type"]:
            case "book":
                event = parse_book_event(message)
            case "price_change":
                event = parse_price_change_event(message)
            case "tick_size_change":
                event = None
                pass
            case _:
                logger.warn("Unknown message type: {}", message["event_type"])

        if event:
            logger.debug("Parsed event {}", event, serialize=True)

    def on_error(self, ws: WebSocketApp, error: str):
        if error:
            logger.error("Error: {}", error)
            self.exit_code = 1

    def on_close(self, ws: WebSocketApp, close_status_code: int, close_msg: str):
        logger.info("Closing connection.")

        if self.ping_thread:
            self.ping_thread.join()

        exit(self.exit_code)

    def on_open(self, ws: WebSocketApp):
        match self.channel_type:
            case Channel.MARKET_CHANNEL:
                req = json.dumps(
                    {"assets_ids": self.asset_ids, "type": self.channel_type.value}
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

        self.ping_thread = threading.Thread(target=self.ping, args=(ws,), daemon=True)
        self.ping_thread.start()

    def ping(self, ws: WebSocketApp):
        while True and not ws.has_errored:
            ws.send("PING")
            time.sleep(10)

    def run(self):
        self.wsapp.run_forever()


@logger.catch
def main():
    load_logging_config()

    api_key = os.getenv("API_KEY")
    api_secret = os.getenv("API_SECRET")
    api_passphrase = os.getenv("PASSPHRASE")

    market_info = get_hourly_market_info_for(market="bitcoin-up-or-down")

    assert len(market_info) >= 1, "No market info retrieved!"

    if len(market_info) > 1:
        logger.warning("More than 1 market read, got {}", len(market_info))

    asset_ids = [t.token_id for t in market_info[0].tokens]
    logger.debug("Opening websocket connection for asset_ids={}", asset_ids)

    auth = {"apiKey": api_key, "secret": api_secret, "passphrase": api_passphrase}

    market_connection = WebSocketOrderBook(
        Channel.MARKET_CHANNEL, POLYMARKET_WSS_URL, asset_ids, auth
    )

    market_connection.run()


if __name__ == "__main__":
    main()
