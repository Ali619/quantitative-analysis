import asyncio
import os

from dotenv import load_dotenv

from crawlers.exchange import run_asynco_fetch
from sqlite.sqlite import PATH, async_ad_to_db, split_path_to_create_db

load_dotenv()
symbols = os.getenv("symbols").split(",")
timeframes = os.getenv("timeframes").split(",")


async def main():
    await run_asynco_fetch(symbols, timeframes)
    split_dfs = await split_path_to_create_db(PATH)
    await async_ad_to_db(split_dfs)


if __name__ == "__main__":
    asyncio.run(main())
