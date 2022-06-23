from datetime import datetime
from pathlib import Path
import time
import csv

import pandas as pd

from config import ScrapeConfig
from log import log_msg



def save_rows_to_csv(config: ScrapeConfig, rows: list[tuple[int, str, float]]) -> None: 
	"""Saves rows (tuples) to a .csv file.""" 

	filename=f"{config.csv_folder}{config.rows_csv_prefix}-{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.csv" 
	with open(filename, 'w') as csvfile: 
		write = csv.writer(csvfile) 
		write.writerow(
					[
						'br_rating',
						'br_boxer_id',
						'boxer_name', 
						'br_points', 
						'weightclass',
						'boxer_wld', 
						'bout_tot',
						'win_tot',
						'loss_tot',
						'draw_tot',
						'career_date_span',
						'career_start_year',
						'career_end_year'
				]
			) 
		for lst in rows:
			for row in lst:
				write.writerow(row)

async def save_df_to_csv(config: ScrapeConfig, df: pd.DataFrame) -> None:
	"""Saves given pd.DataFrame to a .csv file."""
	
	start = time.perf_counter()

	filename=f"{config.csv_folder}/boxrec-alltime-ratings-{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.csv"
	df.to_csv(filename, encoding='utf-8', index=False)

	elapsed = time.perf_counter() - start
	log_msg(f"\n[DataStorage]: The ratings dataframe was exported to a .csv file: {elapsed} seconds!\n")


async def export_scraped_content(pages_content: list[str]) -> None:
	"""Saves html scraped from ratings pages to a locally accessible directory."""

	start = time.perf_counter()

	scrape_output_dir = Path().resolve() / 'snapshots'
	scrape_output_dir.mkdir(parents=True, exist_ok=True)

	for count, result in enumerate(pages_content):
		page_num = result.get("page_num")
		response_text = result.get('response_text')
		output_file = scrape_output_dir / f"pg-{page_num}.html"
		try:
			output_file.write_text(response_text)
			log_msg(f"Saved html file: {count+1}/{len(pages_content)}... ratings page#: {page_num}")
		except:
			log_msg(f"Ratings Page: {page_num}, did NOT have the proper encoding and was skipped.")
			continue 

	elapsed = time.perf_counter() - start
	log_msg(f"\n[RatingsExport]: Ratings Scraper has saved the available ratings page html bodies: {elapsed} seconds!\n")
	