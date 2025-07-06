#!/usr/bin/env python3

from collections import OrderedDict
from polymarket.events.types import (
    Event,
    BookEvent,
    PriceChangeEvent,
    Change,
    Order,
    Side,
)


class Orderbook:
    bid: dict
    ask: dict

    def __init__(self):
        self.bids = OrderedDict()
        self.asks = OrderedDict()

    def apply_event(self, event: Event):
        match event:
            case BookEvent(asset=_, bids=bids, asks=asks, timestamp=_):
                self.bids.clear()
                self.asks.clear()
                self.bids.update((b.price, b) for b in bids)
                self.asks.update((a.price, a) for a in asks)
                self.bids = OrderedDict(sorted(self.bids.items(), reverse=True))
                self.asks = OrderedDict(sorted(self.asks.items()))
            case PriceChangeEvent(asset=_, changes=changes, timestamp=_):
                for c in changes:
                    if c.side == Side.BUY:
                        self.bids[c.order.price] = c.order
                    elif c.side == Side.SELL:
                        self.asks[c.order.price] = c.order
                self.bids = OrderedDict(sorted(self.bids.items(), reverse=True))
                self.asks = OrderedDict(sorted(self.asks.items()))

    def serialize(self, levels=5):
        return [
            *[
                {f"bid_{i + 1}_price": b.price, f"bid_{i + 1}_size": b.size}
                for i, b in enumerate(list(self.bids.values())[:levels])
            ],
            *[
                {f"ask_{i + 1}_price": a.price, f"ask_{i + 1}_size": a.size}
                for i, a in enumerate(list(self.asks.values())[:levels])
            ],
        ]

    def __repr__(self):
        bids_str = ", ".join(f"{price}: {order}" for price, order in self.bids.items())
        asks_str = ", ".join(f"{price}: {order}" for price, order in self.asks.items())
        return f"Orderbook(bids={{{bids_str}}}, asks={{{asks_str}}})"
