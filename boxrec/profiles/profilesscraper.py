import time
import aiohttp
import asyncio

from config import ScrapeConfig
from log import log_msg
from urls import get_br_id_from_url
from export import export_scraped_content
from request import create_agents_list, random_header_agent, fetch_with_sem


class ProfilesScraper:
    def __init__(self, config: ScrapeConfig) -> None:
        self.config = config

    async def scrape(self, urls: list[str]):

        start = time.perf_counter()
        log_msg(
            f"\n[ProfilesScraper]: Profiles Scraper has begun scraping profile URLs...\n"
        )

        collected_profiles = []

        lst = await create_agents_list()

        sem = asyncio.Semaphore(4)
        conn = aiohttp.TCPConnector(limit=20)
        async with aiohttp.ClientSession(connector=conn) as session:
            for count, url in enumerate(urls):

                headers = await random_header_agent(lst)
                br_boxer_id = await get_br_id_from_url(url)

                try:
                    profile = await fetch_with_sem(
                        sem, url, session, headers, br_boxer_id
                    )

                    if profile.get("multipage_code") == 2:
                        urls.append(url + "?&offset=100")
                        urls.append(url + "?&offset=200")
                    if profile.get("multipage_code") == 1:
                        urls.append(url + "?&offset=100")

                    if profile.get("content_flag") == True:
                        collected_profiles.append(profile)
                        log_msg(
                            f"Successfully scraped br_boxer_id#: {br_boxer_id}... count: {count+1}/{len(urls)}"
                        )
                    else:
                        log_msg(
                            f"\nFailed to scraped br_boxer_id#: {br_boxer_id}... count: {count+1}/{len(urls)}"
                        )
                        break
                except:
                    log_msg(
                        f"\nbr_boxer_id: {br_boxer_id}, {count+1}/{len(urls)} failed to create profile >>> broken in the scrape() except: block."
                    )
                    break

        elapsed = time.perf_counter() - start
        log_msg(
            f"\n[ProfilesScraper]: Finished scraping {len(collected_profiles)} profile pages: {elapsed} seconds!\n"
        )

        await export_scraped_content(collected_profiles)
