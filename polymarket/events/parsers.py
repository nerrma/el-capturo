#!/usr/bin/env python3

from polymarket.events.types import (
    Side,
    Order,
    Change,
    BookEvent,
    PriceChangeEvent,
    LastTradePrice,
)
import datetime


def parse_order(order: dict) -> Order:
    return Order(price=float(order["price"]), size=float(order["size"]))


def parse_change(change: dict) -> Change:
    return Change(
        order=Order(price=float(change["price"]), size=float(change["size"])),
        side=Side(change["side"]),
    )


def convert_timestamp(timestamp: str) -> datetime:
    return datetime.datetime.utcfromtimestamp(int(timestamp) / 1000)


def parse_book_event(event: dict) -> BookEvent:
    return BookEvent(
        asset=event["asset_id"],
        bids=[parse_order(b) for b in event["bids"]],
        asks=[parse_order(a) for a in event["asks"]],
        timestamp=convert_timestamp(event["timestamp"]),
    )


def parse_price_change_event(event: dict) -> PriceChangeEvent:
    return PriceChangeEvent(
        asset=event["asset_id"],
        changes=[parse_change(c) for c in event["changes"]],
        timestamp=convert_timestamp(event["timestamp"]),
    )


def parse_last_trade_price(data: dict) -> LastTradePrice:
    return LastTradePrice(
        asset=data["asset_id"],
        side=Side(data["side"]),
        price=float(data["price"]),
        size=float(data["size"]),
        timestamp=convert_timestamp(data["timestamp"]),
    )
