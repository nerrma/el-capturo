#!/usr/bin/env python3

from collections import defaultdict

import polars as pl
from loguru import logger

from tqdm import tqdm


class ParquetWriter:
    def __init__(self, buffer_size=1000):
        self.data = pl.LazyFrame()
        self.buffer_size = buffer_size
        self.asset_name_to_data = defaultdict(lambda: defaultdict(list))
        self.progress_bars = {}  # store tqdm objects per asset_id
        self.iterations = defaultdict(lambda: defaultdict(int))

    def __del__(self):
        for k in self.asset_name_to_data:
            for dt in self.asset_name_to_data[k]:
                self._flush_data(k, dt)

    def _flush_data(self, asset_name: str, data_type: str):
        logger.debug("Flushing {} Parquet data for {}", data_type, asset_name)
        self.iterations[asset_name][data_type] += 1
        asset_data = pl.LazyFrame(self.asset_name_to_data[asset_name][data_type])
        asset_data = asset_data.collect().write_parquet(
            f"{data_type.lower()}-{self.iterations[asset_name][data_type]}-{asset_name.lower()}.parquet",
            compression="zstd",
        )

        self.progress_bars[asset_name][data_type].close()
        self.progress_bars[asset_name][data_type] = tqdm(
            desc=f"{asset_name.lower()}-{data_type.lower()}", total=self.buffer_size
        )

        self.asset_name_to_data[asset_name][data_type].clear()

    def write(self, data_type: str, data: dict):
        asset_name = data["asset_name"]
        self.asset_name_to_data[asset_name][data_type].append(data)

        if asset_name not in self.progress_bars:
            self.progress_bars[asset_name] = {
                data_type: tqdm(
                    desc=f"{asset_name.lower()}-{data_type.lower()}",
                    total=self.buffer_size,
                )
            }
        else:
            if data_type not in self.progress_bars[asset_name]:
                self.progress_bars[asset_name][data_type] = tqdm(
                    desc=f"{asset_name.lower()}-{data_type.lower()}",
                    total=self.buffer_size,
                )

            self.progress_bars[asset_name][data_type].update(1)

        if len(self.asset_name_to_data[asset_name][data_type]) >= self.buffer_size:
            self._flush_data(asset_name, data_type)
