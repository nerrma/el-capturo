#!/usr/bin/env python3

import sys

import yaml
from loguru import logger

from constants import LOG_CONFIG_FILE


def load_logging_config():
    with open(LOG_CONFIG_FILE, "r") as f:
        config = yaml.safe_load(f)

    for i, h in enumerate(config["handlers"]):
        if "sink" in h and "sys" in h["sink"]:
            match h["sink"]:
                case "sys.stderr":
                    config["handlers"][i]["sink"] = sys.stderr
                case "sys.stdout":
                    config["handlers"][i]["sink"] = sys.stdout
                case _:
                    pass

    logger.configure(**config)
