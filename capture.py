#!/usr/bin/env python3

import signal
import threading

from loguru import logger

from binance_capture.websocket_capture import run_capture as run_binance_capture
from config_manager import load_logging_config
from polymarket.websocket_capture import run_capture as run_polymarket_capture


binance_connection = None
polymarket_connection = None


def signal_handler(signal, frame):
    global binance_connection
    if binance_connection is not None:
        logger.info("Closing binance connection.")
        binance_connection.stop()
    else:
        logger.warning("Binance connection not found!")

    global polymarket_connection
    if polymarket_connection is not None:
        logger.info("Closing polymarket connection.")
        polymarket_connection.stop()
    else:
        logger.warning("Polymarket connection not found!")
    exit(0)


def run_binance_capture_thread():
    global binance_connection
    binance_connection = run_binance_capture()
    logger.info("Got binance_connection {}", binance_connection)


def run_polymarket_capture_thread():
    global polymarket_connection
    polymarket_connection = run_polymarket_capture()
    logger.info("Got polymarket_connection {}", polymarket_connection)


@logger.catch
def main():
    logger.info("Starting capture...")
    load_logging_config()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    t1 = threading.Thread(target=run_binance_capture_thread)
    t2 = threading.Thread(target=run_polymarket_capture_thread)
    t1.start()
    t2.start()

    t1.join()
    t2.join()


if __name__ == "__main__":
    main()
