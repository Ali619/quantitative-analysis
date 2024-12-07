from crawlers.ExchangeCrawler import exchange_fetch_price
from sqlite.Sqlite import add_to_db
import pandas as pd

if __name__ == "__main__":
    df = exchange_fetch_price(timeframe='4h', symbol='BTC/USDT')
    # df = pd.read_csv('./exchange-fetched-data/2024-12-07_20-17-4H-data.csv')
    # add_to_db(df=df, database_name='coinex_4h', table_name='BTC_USDT')

