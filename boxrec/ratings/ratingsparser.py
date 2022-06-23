import time
import unicodedata
from pathlib import Path  
from typing import Any

from bs4 import BeautifulSoup as bs

from config import ScrapeConfig
from log import log_msg


def clean_str(text: str) -> str: 
	"""Cleans given String of unicodedata, newlines, special chars, and whitespace."""

	return unicodedata.normalize('NFKC', text).replace('\r','').replace('\n','').replace('*','').strip()

async def create_soup(response_text: str) -> bs:
	"""Uses BeatifulSoup to convert response.text (html) into a BeautifulSoup object."""

	return bs(response_text, 'html.parser')


class PageRatingsHTMLNotRenderedError(Exception):
	"""The ratings page did not laod the html needed for/during scrape extraction."""
	pass


class RatingsParser:

	def __init__(self, config: ScrapeConfig):
		self.config = config
		self.files = self.mount_files()
		self.archive = self.mount_archive()

	def mount_files(self) -> Path: 
		"""Function called to initialize access to saved html files from scraper as class instance variable."""
		directory = 'snapshots'
		files = list(Path(directory).glob('*'))
		return files 

	def mount_archive(self) -> Path: 
		"""Function called to initialize access to archived html files as class instance variable."""
		directory = self.config.collected_snapshots_dir
		files = list(Path(directory).glob('*'))
		return files 

	async def extract_rows(self, response_text: str) -> tuple[list[tuple[int, str, float]], list[Any]]:
		"""Extracts given response.text data into rows (tuples) and appends all to list."""

		rows = []
		error_rows = []

		soup = await create_soup(response_text)
		tbodies = [i for item in soup.find_all('tbody') for i in item]

		page_num = soup.find_all('span', 'pagerCurrent')[-1].text.strip()

		if len(tbodies) < 1 or soup.find('title').text.strip() != 'BoxRec: Ratings':
			log_msg(f"***** The html was not avialable for parsing. page: {page_num} ***** ...")

		for tr in tbodies:
			lst = list(tr)
			try:
				br_rating = int(clean_str(lst[0].text))
				br_boxer_id = int(clean_str(lst[1].find('a').get('href')).split('r/')[1])
				boxer_name = clean_str(lst[1].text)
				br_points = float(clean_str(lst[2].text))
				weightclass = clean_str(lst[4].text)
				boxer_wld = clean_str(lst[3].text).replace(' ','-')
				win_tot = int(boxer_wld.split('-')[0])
				loss_tot = int(boxer_wld.split('-')[1])
				draw_tot = int(boxer_wld.split('-')[2])
				bout_tot = int(win_tot + loss_tot + draw_tot)
				career_date_span = clean_str(lst[5].text)
				career_start_year = int(career_date_span.split('-')[0])
				career_end_year = int(career_date_span.split('-')[1])

			except:
				error_rows.append(lst)
				pass

			row = (
					br_rating,
					br_boxer_id,
					boxer_name, 
					br_points, 
					weightclass,
					boxer_wld, 
					bout_tot,
					win_tot,
					loss_tot,
					draw_tot,
					career_date_span,
					career_start_year,
					career_end_year
				)
			rows.append(row)

		return rows, error_rows

	async def parse(self) -> list[tuple[int, str, float]]:
		"""Accesses saved html directory and extracts rows from all files found within."""
		
		start = time.perf_counter()

		all_rows = []

		files = self.archive

		for count, file in enumerate(files):
			with open(file, 'r', encoding='utf-8', errors='ignore') as f:
				response_text = f.read()

				page_num = str(f).split('pg-')[1].split('.html')[0]
				
				try:
					rows, error_rows = await self.extract_rows(response_text)
					all_rows.append(rows)
					log_msg(f"Ratings Parser extracted data from html file: {count+1}/{len(list(files))}... ratings page#: {page_num}")
				except:
					log_msg(f"Ratings Parsing did NOT complete for file: {count+1}/{len(list(files))}... ratings page#: {page_num}")
					continue

		elapsed = time.perf_counter() - start
		log_msg(f"\n[RatingsParser]: Ratings Parser extracted data from available html files: {elapsed} seconds!\n")
		return all_rows  

	


