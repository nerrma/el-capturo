#!/usr/bin/env python3

from enum import Enum
from typing import List

from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel, ValidationError, field_validator

from polymarket.market_info import Token


class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"


class Order(BaseModel):
    price: float
    size: float

    @field_validator("price")
    @classmethod
    def validate_price(cls, p: float) -> float:
        if p < 0 or p > 1:
            raise ValueError()
        return p

    @field_validator("size")
    @classmethod
    def validate_size(cls, s: float) -> int:
        if s < 0:
            raise ValueError()
        return s


@dataclass
class Change:
    order: Order
    side: Side


@dataclass
class BookEvent:
    asset: Token
    bids: List[Order]
    asks: List[Order]
    timestamp: datetime


@dataclass
class PriceChangeEvent:
    asset: Token
    changes: List[Change]
    timestamp: datetime


Event = BookEvent | PriceChangeEvent
