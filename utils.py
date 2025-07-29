#!/usr/bin/env python3

import datetime
import json
import re
import time

from loguru import logger

import pytz
import requests

from constants import BINANCE_API_URL, HYPERLIQUID_API_URL

MAX_RETRIES = 5


def convert_timestamp(timestamp: str) -> datetime:
    return datetime.datetime.fromtimestamp(int(timestamp) / 1000, datetime.UTC)


def get_candle_times() -> (datetime.datetime, datetime.datetime):
    # Get current time in UTC
    utc_now = datetime.datetime.now(pytz.utc).replace(minute=0, second=0, microsecond=0)

    # Define the time range for the candle
    candle_start = utc_now
    candle_end = candle_start + datetime.timedelta(minutes=1)
    return candle_start, candle_end


def get_binance_target_price(symbol: str) -> float:
    candle_start, candle_end = get_candle_times()

    # Define the API endpoint and parameters
    url = f"{BINANCE_API_URL}/v3/klines"
    params = {
        "symbol": symbol,
        "interval": "1m",
        "startTime": int(candle_start.timestamp() * 1000),  # Convert to milliseconds
        "endTime": int(candle_end.timestamp() * 1000),  # Convert to milliseconds
    }

    # Make the GET request
    response = requests.get(url, params=params)

    # Handle the response
    if response.status_code == 200:
        klines = response.json()

        # Ensure there is at least one kline
        if klines:
            return float(klines[0][1])  # The "open" value is at index 1
        else:
            raise ValueError("No klines received")
    else:
        raise Exception(f"Error fetching data: {response.status_code}")


def get_hyperliquid_target_price(symbol: str) -> float:
    candle_start, candle_end = get_candle_times()

    # Define the API endpoint and parameters
    url = HYPERLIQUID_API_URL
    req = {
        "type": "candleSnapshot",
        "req": {
            "coin": symbol,
            "interval": "1m",
            "startTime": int(
                candle_start.timestamp() * 1000
            ),  # Convert to milliseconds
            "endTime": int(candle_end.timestamp() * 1000),  # Convert to milliseconds
        },
    }

    headers = {"Content-Type": "application/json; charset=utf-8"}

    # Make the GET request
    response = requests.post(url, json=req, headers=headers)
    open_price = None
    retries = 0

    while open_price is None and retries < MAX_RETRIES:
        if response.status_code == 200:
            candles = response.json()

            # Ensure we have at least one candle
            if candles:
                # Get the open price from the first candle
                open_price = float(
                    candles[0]["o"]
                )  # The "open" price is in the "o" field
                return open_price
            else:
                logger.warning("No candle received from hyperliquid, retrying...")
                retries += 1
                time.sleep(3)
        else:
            raise Exception(f"Error fetching data: {response.status_code}")

    if open_price is None:
        logger.warning("No open price for hyperliquid!")

    return open_price
