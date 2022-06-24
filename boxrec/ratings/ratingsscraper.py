import asyncio
import aiohttp
import time

from config import ScrapeConfig
from export import export_scraped_content
from log import log_msg
from request import (
    create_agents_list,
    random_header_agent,
    fetch_with_sem,
    silence_event_loop_closed,
)
from urls import curr_page_num


class RatingsScraper:
    def __init__(self, config: ScrapeConfig) -> None:
        self.config = config

    async def scrape(self, urls) -> list[str]:
        """Scrapes list of given URLs and saves them to a locally accessible directory for parsing."""

        start = time.perf_counter()
        log_msg(
            f"\n[RatingsScraper]: Ratings Scraper has begun scraping ratings URLs...\n"
        )

        pages_content = []

        agents = await create_agents_list()
        sem = asyncio.Semaphore(10)
        conn = aiohttp.TCPConnector(limit=50)

        async with aiohttp.ClientSession(connector=conn) as session:
            for count, url in enumerate(urls):
                page_num = await curr_page_num(
                    self.config.base_url, self.config.base_href, url
                )
                headers = await random_header_agent(agents)

                try:
                    page_content = await fetch_with_sem(
                        sem, url, session, headers, page_num
                    )
                    if page_content.get("content_flag") == True:
                        pages_content.append(page_content)
                        log_msg(
                            f"Successfully scraped URL: {count+1}/{len(urls)}... ratings page#: {page_num}"
                        )
                    else:
                        log_msg(
                            f"\nFailed to scrape URL: {count+1}/{len(urls)}... ratings page#: {page_num}"
                        )
                        break
                except:
                    log_msg(
                        f"\nPage: {page_num}, {count+1}/{len(urls)} failed to create page_content >>> broken in the scrape() except: block."
                    )
                    break

        elapsed = time.perf_counter() - start
        log_msg(
            f"\n[RatingsScraper]: Finished scraping {len(pages_content)} ratings pages: {elapsed} seconds!\n"
        )

        await export_scraped_content(pages_content)
