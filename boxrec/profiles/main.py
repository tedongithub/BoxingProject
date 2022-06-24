import asyncio
import time
from typing import Awaitable, Any

from config import read_config
from profilesscraper import ProfilesScraper
from profilesparser import ProfilesParser
from fightsnipsparser import FightSnippetsParser
from log import log_msg
from urls import get_profile_urls_rem, get_files, top_hundred_profile_urls
from transform import profiles_to_df, fightsnips_to_df
from export import save_df_to_csv, save_all_profiles_to_csv, save_all_fightsnips_to_csv
from db import (
    new_profiles_table,
    new_fight_snips_table,
    insert_profiles_to_db,
    insert_fightsnips_to_db,
    get_profile_urls_from_ratings,
)


async def run_sequence(*functions: Awaitable[Any]) -> None:

    for function in functions:
        await function


async def run_parallel(*functions: Awaitable[Any]) -> None:

    await asyncio.gather(*functions)


async def main():

    log_msg(
        f"\n[START]: The '__main__' function was called >>> Profiles Scrape beginning...\n"
    )

    config = read_config("./profile-config.json")

    urls_100 = await top_hundred_profile_urls(config)

    profile_urls_rem = get_profile_urls_rem(config)

    scraper = ProfilesScraper(config)
    await scraper.scrape(urls_100)

    pp = ProfilesParser(config)
    all_profiles = await pp.parse()

    cp = FightSnippetsParser(config)
    all_fightsnips = await cp.parse()

    df_profiles = await profiles_to_df(all_profiles)
    df_fightsnips = await fightsnips_to_df(all_fightsnips)

    await save_all_profiles_to_csv(config, all_profiles)
    await save_all_fightsnips_to_csv(config, all_fightsnips)

    await new_profiles_table()
    await new_fight_snips_table()
    await insert_profiles_to_db(df_profiles)
    await insert_fightsnips_to_db(df_fightsnips)


if __name__ == "__main__":
    start = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - start
    log_msg(
        f"\n[EXIT]: The entire program was completed in a total of: {elapsed} seconds!\n"
    )
