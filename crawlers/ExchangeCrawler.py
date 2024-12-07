import ccxt
import pandas as pd
from dotenv import load_dotenv
import os
import time
import datetime
import os
from pathlib import Path

TIME = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
PATH = Path(__file__).parent.parent / 'exchange-fetched-data'
PATH.mkdir(parents=True, exist_ok=True)

# os.makedirs(PATH, exist_ok=True)
load_dotenv()
ID = os.getenv('id')
SECRET = os.getenv('secret')

def exchange_fetch_price(timeframe: str, symbol: str, limit: int=1000) -> pd.DataFrame:
    """Fetch data from exchange from `2023-01-01T00:00:00Z`. Default exchange is CoinEx and it's not changeble at this time.
    params: 
        `timeframe`: timeframe of data
        `limit`: number of data to fetch
        `symbol`: symbol of data (for e.x. `"BTC/USDT"`)
    """
    exchange = ccxt.coinex({
        'apiKey': ID,
        'secret': SECRET,
        'enableRateLimit': True,
    })
    since = '2024-01-01T00:00:00Z'
    since = exchange.parse8601(since)
    for retries in range(5):
        try:
            ohlcv = exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since, limit=limit)
            break
        except(ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.NetworkError, ccxt.RequestTimeout, ccxt.ExchangeNotAvailable, Exception) as e:
            print(type(e).__name__, e)
            time.sleep(retries + 1)
    else:
        print('Maximum retries reached, fetching data is closed.')
        exit()
    print(f'ohlvc data is fetched. Creating dataframe and storing in {PATH}/{TIME}-{timeframe.upper()}-data.csv')
    df = pd.DataFrame(ohlcv, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
    df['date'] = pd.to_datetime(df['date'], unit='ms')
    df.to_csv(f'{PATH}/{TIME}-{timeframe.upper()}-data.csv', index=False)
    return df
