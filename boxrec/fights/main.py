import asyncio
import time
from typing import Awaitable, Any

from config import read_config
from log import log_msg
from db import get_fight_urls_not_in_db, make_new_fights_table, insert_fights_df_to_db
from exporters import get_fight_urls_not_in_archive, save_all_fights_to_csv
from fightscraper import FightsScraper
from fightparser import FightParser
from log import log_msg
from wrangle import fights_to_df


async def run_sequence(*functions: Awaitable[Any]) -> None:
    for function in functions:
        await function


async def run_parallel(*functions: Awaitable[Any]) -> None:
    await asyncio.gather(*functions)


async def main(): 

    log_msg(f"\n[START]: The '__main__' function was called >>> Fights Scrape beginning...\n")

    config = read_config('./fights-config.json')
    """
    # fight_urls_rem = get_fight_urls_not_in_db(config)
    fight_urls_rem = get_fight_urls_not_in_archive(config)
    print("# of urls remaining: ", len(fight_urls_rem))
    
    scraper = FightsScraper(config)
    await scraper.scrape(fight_urls_rem)
    """
    p = FightParser(config)
    all_fights = await p.parse()

    df = await fights_to_df(all_fights)
    # print(df.columns[df.isna().any()].tolist())
    print(df.dtypes) 
    print(df.shape)
    
    await save_all_fights_to_csv(config, all_fights)
    # await save_df_to_csv(config, df)
    
    await make_new_fights_table()
    await insert_fights_df_to_db(df)
     
if __name__ == '__main__':
    start = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - start
    log_msg(f"\n[EXIT]: The entire program was completed in a total of: {elapsed} seconds!\n")
