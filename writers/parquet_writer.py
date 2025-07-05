#!/usr/bin/env python3

from collections import defaultdict

import polars as pl
from loguru import logger
from polymarket.orderbook.orderbook import Orderbook


class ParquetWriter:
    def __init__(self, buffer_size=1000):
        self.data = pl.LazyFrame()
        self.buffer_size = buffer_size
        self.asset_name_to_data = defaultdict(list)
        self.iterations = defaultdict(int)

    def __del__(self):
        self._flush_data()

    def _flush_data(self):
        logger.info("Flushing Parquet data")
        for asset_name, entry_list in self.asset_name_to_data.items():
            self.iterations[asset_name] += 1
            asset_data = pl.LazyFrame(entry_list)
            asset_data = asset_data.collect().write_parquet(
                f"cap-{self.iterations[asset_name]}-{asset_name.lower()}.parquet"
            )
        self.asset_name_to_data = defaultdict(list)

    def write(self, entry: dict):
        asset_name = entry["asset_name"]
        self.asset_name_to_data[asset_name].append(entry)

        if len(self.asset_name_to_data[asset_name]) >= self.buffer_size:
            self._flush_data()
