#!/usr/bin/env python3

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import pytz
import requests
from constants import POLYMARKET_GAMMA_URL
from loguru import logger


@dataclass
class Token:
    token_name: str
    token_id: str


@dataclass
class MarketInfo:
    slug: str
    condition_id: str
    tokens: List[Token]


def get_hourly_market_info_for(market="bitcoin-up-or-down") -> List[MarketInfo]:
    # make slug in the form "market-{month_str}-{day}-{hour}-et"
    now = datetime.now(pytz.timezone("US/Eastern"))

    now = now.replace(minute=0, second=0, microsecond=0)

    month_str = now.strftime("%B").lower()
    day = now.day
    hour_12 = now.strftime("%I").lstrip("0")
    am_pm = now.strftime("%p").lower()

    slug = f"{market}-{month_str}-{day}-{hour_12}{am_pm}-et"
    logger.debug("Generated market slug {}", slug)

    response = requests.get(f"{POLYMARKET_GAMMA_URL}/markets?slug={slug}")
    markets = response.json()

    logger.debug("Got API response {}", markets)

    return [
        MarketInfo(
            slug=slug,
            condition_id=market.get("conditionId"),
            tokens=[
                Token(token_name=o, token_id=i)
                for o, i in zip(
                    json.loads(market.get("outcomes")),
                    json.loads(market.get("clobTokenIds")),
                )
            ],
        )
        for market in markets
    ]
