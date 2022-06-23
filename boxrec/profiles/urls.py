# import asyncio
from pathlib import Path
import random

from db import get_profile_urls_from_ratings, get_profiles_wins_100
from config import ScrapeConfig


def get_files(path: Path) -> Path: 
	"""Accepts a file path and returns a generator of all file within.""" 

	directory = path
	files = list(Path(directory).glob('*'))
	return files 

def get_profile_urls_rem(config: ScrapeConfig) -> list[str]:
	"""Returns an ordered list of URLs by comparing all URLs possible and URLs saved in the specified config directory."""

	all_profile_urls = get_profile_urls_from_ratings(config)

	files = get_files(config.collected_profiles_dir)
	collected_profile_urls = [(config.base_url+config.base_href+str(i).split('id-')[1].split('.')[0]) for i in files]
	# collected_profile_urls = [x for x in all_profile_urls if (config.base_url+config.base_href+str(i).split('id-')[1].split('.')[0]) not in all_profile_urls]
	profile_urls_rem = [x for x in all_profile_urls if x not in collected_profile_urls]
	# random.shuffle(profile_urls_rem)
	return profile_urls_rem

def build_next_url(config: ScrapeConfig, boxrec_id: int) -> str:
	url = config.base_url + config.base_href + str(boxrec_id)
	return url

async def get_br_id_from_url(url: str) -> str:
	if '?' in url: return url.split('r/')[1].split('?')[0].strip()
	return url.split('r/')[1].strip()

async def get_br_id_from_file(file: str) -> str:
	if '-1.' or '-2.' in file: return str(file).split('id-')[1].split('.')[0]
	return str(file).split('id-')[1].split('.')[0].strip()

async def filename_to_url(file: str) -> str:
	if '-1.' in str(file): return str(file).split('id-')[1].split('-1.')[0] + '?&offset=100'
	if '-2.' in str(file): return str(file).split('id-')[1].split('-2.')[0] + '?&offset=200'

	return str(file).split('id-')[1].split('.')[0].strip()


async def top_hundred_profile_urls(config: ScrapeConfig) -> list[str]:
	snapshots = get_files('snapshots')
	print(f"Files already saved in 'snapshots': {len(snapshots)}\n")

	url_list = []

	html_urls = [build_next_url(config, await filename_to_url(x)) for x in snapshots]
	# print(z, len(z), "\n")

	boxers = get_profiles_wins_100()

	for item in boxers.items():
		if 100 <= item[1] < 300:
			url_list.append(
				build_next_url(config, boxrec_id=item[0]) + '?&offset=100')
		if 200 <= item[1] < 300:
			url_list.append(
				build_next_url(config, boxrec_id=item[0]) + '?&offset=200')
		url_list.append(
			build_next_url(config, boxrec_id=item[0]) )

	return [x for x in url_list if x not in html_urls]