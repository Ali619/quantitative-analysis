import asyncio
import datetime
import os
import time
from pathlib import Path

import ccxt.async_support as ccxt
import pandas as pd
from dotenv import load_dotenv

# import ccxt


NOW = datetime.datetime.now()
YEAR = datetime.datetime.now().year
TIME = NOW.strftime("%Y-%m-%d_%H-%M")
PATH = Path(__file__).parent.parent / "exchange-fetched-data"
PATH.mkdir(parents=True, exist_ok=True)

load_dotenv()
ID = os.getenv("id")
SECRET = os.getenv("secret")


async def exchange_fetch_price(
    timeframe: str, symbol: str, limit: int = 1000
) -> pd.DataFrame:
    """Fetch data from exchange from `2023-01-01T00:00:00Z`. Default exchange is CoinEx and it's not changeble at this time.
    params:
        `timeframe`: timeframe of data (for e.x. `"4h"`)
        `limit`: number of data to fetch
        `symbol`: symbol of data (for e.x. `"BTC/USDT"`)
    """
    exchange = ccxt.coinex(
        {
            "apiKey": ID,
            "secret": SECRET,
            "enableRateLimit": True,
        }
    )
    since = f"{YEAR}-01-01T00:00:00Z"
    since = exchange.parse8601(since)
    for retries in range(5):
        try:
            ohlcv = await exchange.fetch_ohlcv(
                symbol=symbol, timeframe=timeframe, since=since, limit=limit
            )
            await exchange.close()
            break
        except (
            ccxt.ExchangeError,
            ccxt.AuthenticationError,
            ccxt.NetworkError,
            ccxt.RequestTimeout,
            ccxt.ExchangeNotAvailable,
            Exception,
        ) as e:
            print(type(e).__name__, e, f"waiting for {retries + 1} secs")

            await asyncio.sleep(retries + 1)
    else:
        print("Maximum retries reached, fetching data is closed.")
        exit()
    file_name = f'{PATH}/{symbol.replace("/", "_")}-{timeframe}-{TIME}'
    print(
        f"ohlvc for {symbol} is fetched, dataframe created and stored in {file_name}.csv"
    )
    df = pd.DataFrame(ohlcv, columns=["date", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["date"], unit="ms")
    df.to_csv(
        f"{file_name}.csv",
        index=False,
    )
    return df


async def run_asynco_fetch(symbols, timeframes):
    task = [
        exchange_fetch_price(timeframe=timeframe, symbol=symbol)
        for symbol in symbols
        for timeframe in timeframes
    ]
    result = await asyncio.gather(*task)
    return result
