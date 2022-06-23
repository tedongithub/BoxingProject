import asyncio
import time
from typing import Any, Awaitable

from config import read_config
from ratingsscraper import RatingsScraper 
from ratingsparser import RatingsParser
from transform import rows_to_df
from export import save_df_to_csv
from db import make_new_ratings_table, insert_to_db_table
from log import log_msg
from urls import get_urls, get_urls_rem, get_files


async def run_sequence(*functions: Awaitable[Any]) -> None:
    for function in functions:
        await function

async def run_parallel(*functions: Awaitable[Any]) -> None:
    await asyncio.gather(*functions)


async def main():

	log_msg(f"\n[START]: The '__main__' function was called >>> Ratings Scrape beginning...\n")

#1. CONFIGURATION: create urls to scrape
	config = read_config('./config.json')

	urls = get_urls(config, get_all=False, how_many=5)
	
	# urls = get_urls(config, get_all=True)
	# path = get_files(config.collected_snapshots_dir)
	# urls_rem = get_urls_rem(urls, path)

	
#2. SCRAPER: scrape given URLs and save profile page html files
	scraper = RatingsScraper(config) 
	await scraper.scrape(urls) 

	
#3. PARSER: parse each result (html) for its rows of data.
	p = RatingsParser(config)
	rows = await p.parse()


#4. TRANSFORMS: create, transform, and clean dataframe w/ Pandas.
	df = await rows_to_df(rows)


#5. EXPORTS: export data to a .csv file and insert dataframe into SQLServer database.
	await run_parallel(
		save_df_to_csv(config, df),
		run_sequence(
			make_new_ratings_table(),
			insert_to_db_table(df)
		),
	)
	

if __name__ == '__main__':
	start = time.perf_counter()
	asyncio.run(main())
	elapsed = time.perf_counter() - start
	print(f"\n[EXIT]: The entire program was completed in a total of: {elapsed} seconds!\n")


