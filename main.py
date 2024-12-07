import pandas as pd

from crawlers.exchangeCrawler import exchange_fetch_price
from sqlite.sqlite import add_to_db

if __name__ == "__main__":
    df = exchange_fetch_price(timeframe="4h", symbol="BTC/USDT")
    # df = pd.read_csv('./exchange-fetched-data/2024-12-07_22-24-4H-data.csv')
    add_to_db(df=df, database_name="coinex_4h", table_name="BTC_USDT")
