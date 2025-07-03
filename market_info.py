#!/usr/bin/env python3

from datetime import datetime, timedelta
import requests
import pytz
from dataclasses import dataclass
from loguru import logger
import json
from typing import List
from constants import POLYMARKET_GAMMA_URL


@dataclass
class MarketInfo:
    condition_id: str
    token_ids: List[str]


def get_hourly_market_info_for(market="bitcoin-up-or-down") -> List[MarketInfo]:
    # Get current time in Eastern Time
    now = datetime.now(pytz.timezone("US/Eastern"))

    # Round up to the next hour
    now = now.replace(minute=0, second=0, microsecond=0)

    # Format components
    month_str = now.strftime("%B").lower()
    day = now.day
    hour_12 = now.strftime("%I").lstrip("0")
    am_pm = now.strftime("%p").lower()

    # Compose slug
    slug = f"{market}-{month_str}-{day}-{hour_12}{am_pm}-et"
    logger.debug("Generated market slug {}", slug)

    response = requests.get(f"{POLYMARKET_GAMMA_URL}/markets?slug={slug}")
    markets = response.json()

    logger.debug("Got API response {}", markets)

    return [
        MarketInfo(
            condition_id=market.get("conditionId"),
            token_ids=json.loads(market.get("clobTokenIds")),
        )
        for market in markets
    ]
