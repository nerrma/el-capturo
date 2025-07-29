#!/usr/bin/env python3

import json
import os
import shutil
import signal
import threading
from datetime import datetime, timedelta

from loguru import logger

from binance_capture.websocket_capture import run_capture as run_binance_capture
from config_manager import load_logging_config
from hyperliquid_capture.websocket_capture import run_capture as run_hyperliquid_capture
from polymarket.websocket_capture import run_capture as run_polymarket_capture
from utils import get_binance_target_price, get_hyperliquid_target_price

binance_connection = None
polymarket_connection = None
hyperliquid_connection = None
market_info = None
terminate = False
timing_thread = None
target_thread = None


def signal_handler(sig_num, _):
    assert timing_thread is not None
    timing_thread.cancel()

    assert target_thread is not None
    target_thread.cancel()

    global binance_connection
    if binance_connection is not None:
        logger.debug("Closing binance connection.")
        binance_connection.stop()
    else:
        logger.warning("Binance connection not found!")

    global hyperliquid_connection
    if hyperliquid_connection is not None:
        logger.debug("Closing hyperliquid connection.")
        hyperliquid_connection.stop()
    else:
        logger.warning("Hyperliquid connection not found!")

    global polymarket_connection
    if polymarket_connection is not None:
        logger.debug("Closing polymarket connection.")
        polymarket_connection.stop()
    else:
        logger.warning("Polymarket connection not found!")

    global terminate
    if sig_num != signal.SIGUSR1 or terminate:
        terminate = True
        logger.info("Signal received to terminate")
    else:
        assert not terminate
        logger.info("Signal received to re-run")

    # move any parquets to their own slug directory based on market_info
    parquet_files = [e for e in os.listdir("./") if e.endswith(".parquet")]
    if market_info and parquet_files:
        slug_directory = f"data/{market_info.slug}/"
        os.makedirs(slug_directory, exist_ok=True)

        for parquet_file in parquet_files:
            shutil.move(parquet_file, slug_directory)

        logger.info("Parquet files moved to '{}'.", slug_directory)

        # move targets file
        shutil.move("targets.json", slug_directory)

    # continue running
    if not terminate:
        logger.debug("Rerunning")
        main()


def run_binance_capture_thread():
    global binance_connection
    binance_connection = run_binance_capture()
    logger.debug("Got binance_connection {}", binance_connection)


def run_hyperliquid_capture_thread():
    global hyperliquid_connection
    hyperliquid_connection = run_hyperliquid_capture()
    logger.debug("Got hyperliquid_connection {}", hyperliquid_connection)


def run_polymarket_capture_thread():
    global polymarket_connection, market_info
    polymarket_connection, market_info = run_polymarket_capture()
    logger.debug("Got polymarket_connection {}", polymarket_connection)


def fire_interrupt():
    logger.info("Firing signal to interrupt.")
    pid = os.getpid()
    os.kill(pid, signal.SIGUSR1)


def output_targets():
    binance_target = get_binance_target_price("BTCUSDT")
    hyperliquid_target = get_hyperliquid_target_price("BTC")

    with open("targets.json", "w") as f:
        json.dump(
            {
                "binance_target": binance_target,
                "hyperliquid_target": hyperliquid_target,
            },
            f,
            indent=4,
        )


@logger.catch
def main():
    load_logging_config()

    now = datetime.now()
    next_hour = now.replace(minute=59, second=59, microsecond=0)
    delay = (next_hour - now).total_seconds()  # calculate delay till next hour

    if delay <= 1:
        next_hour = (now + timedelta(hours=1)).replace(
            minute=59, second=59, microsecond=0
        )
        delay = (next_hour - now).total_seconds()

    global target_thread
    target_thread = threading.Timer(
        60, output_targets
    )  # 1 minute delay for initial candles

    t1 = threading.Thread(target=run_binance_capture_thread)
    t2 = threading.Thread(target=run_polymarket_capture_thread)
    t3 = threading.Thread(target=run_hyperliquid_capture_thread)

    global timing_thread
    timing_thread = threading.Timer(delay, fire_interrupt)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGUSR1, signal_handler)

    t1.start()
    t2.start()
    t3.start()
    timing_thread.start()
    target_thread.start()

    t1.join()
    t2.join()
    t3.join()
    timing_thread.join()
    target_thread.join()


if __name__ == "__main__":
    logger.info("Starting capture...")
    main()
