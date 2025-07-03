#!/usr/bin/env python3

import os

from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

load_dotenv("keys.env")


def main():
    host = "https://clob.polymarket.com"
    key = os.getenv("PK")
    chain_id = POLYGON

    if not key:
        raise ValueError(
            "Private key not found. Please set PK in the environment variables."
        )

    client = ClobClient(host, key=key, chain_id=chain_id)

    try:
        api_creds = client.create_or_derive_api_creds()
        print(f"received api creds: ")
        print(f"{api_creds=}")

        print("\nadd the following to your .env file:")
        print(f'API_KEY = "{api_creds.api_key}"')
        print(f'API_SECRET = "{api_creds.api_secret}"')
        print(f'PASSPHRASE = "{api_creds.api_passphrase}"')

    except Exception as e:
        print("Error creating or deriving API credentials:", e)


if __name__ == "__main__":
    main()
