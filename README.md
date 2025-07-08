# el-capturo

Scripts for capturing Polymarket and Binance data.

## How To Use

### Key setup

First, set up a `keys.env` file in the root directory.

For polymarket, you will need to export your private key from [https://reveal.magic.link/polymarket](https://reveal.magic.link/polymarket). Insert this key into your `keys.env` file named as `PK`.

``` dotenv
PK = "{YOUR_KEY_HERE}"
```

Once this is set up, you will need to get (or generate) your unique Polymarket API keys. Do this using the [polymarket/derive_creds.py](polymarket/derive_creds.py) file as follows:

```shell
$ uv run python -m polymarket.derive_creds
```

Add the bottom of the output to the `keys.env` file as it is shown in the script. Your file should look like this:
``` dotenv
PK = "{YOUR_KEY_HERE}"

API_KEY = "{API_KEY}"
API_SECRET = "{API_SECRET}"
PASSPHRASE = "{PASSPHRASE}"
```

### Running a capture

You can now run a Polymarket and Binance capture by doing:

```shell
$ uv run python capture.py
```

This will capture market data for the hourly Bitcoin market by default and output them to Parquet files named `{asset_name}-{capture_type}-{seq_no}.parquet` where:
- `asset_name` can be a polymarket token name or cryptocurrency name
- `capture_type` is an 'orderbook' or 'trade' capture
- `seq_no` the sequence number in the capture series

These will be stored in the `data/{market_slug}` directory for the relevant market.

Refer to [polymarket/market_info.py](polymarket/market_info.py) for info on how the information for the market is generated. The important thing here are the `token_ids` which we listen to for information on the relevant market.
