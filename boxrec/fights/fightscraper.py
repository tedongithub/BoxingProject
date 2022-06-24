import aiohttp
import asyncio
import time

from config import ScrapeConfig
from exporters import export_scraped_content
from log import log_msg
from utils import curr_event_fight_id
from fightrequest import create_agents_list, random_header_agent, fetch_with_sem


class FightsScraper:
    def __init__(self, config: ScrapeConfig) -> None:
        self.config = config

    async def scrape(self, urls: list[str]) -> str:

        start = time.perf_counter()
        log_msg(f"\n[FightsScraper]: Fights Scraper has begun scraping fight URLs...\n")

        collected_fights = []

        lst = await create_agents_list()

        sem = asyncio.Semaphore(15)
        conn = aiohttp.TCPConnector(limit=25)
        async with aiohttp.ClientSession(connector=conn) as session:
            for count, url in enumerate(urls):

                headers = await random_header_agent(lst)
                event_fight_id = await curr_event_fight_id(url)

                try:
                    fight = await fetch_with_sem(
                        sem, url, session, headers, event_fight_id
                    )
                    if fight.get("content_flag") == True:
                        collected_fights.append(fight)
                        log_msg(
                            f"Successfully scraped event_fight_id#: {event_fight_id}... count: {count+1}/{len(urls)}"
                        )
                    else:
                        log_msg(
                            f"\nFailed to scraped event_fight_id#: {event_fight_id}... count: {count+1}/{len(urls)}"
                        )
                        break
                except:
                    log_msg(
                        f"\nevent_fight_id: {event_fight_id}, {count+1}/{len(urls)} failed to create fight >>> broken in the scrape() except: block."
                    )
                    break

        elapsed = time.perf_counter() - start
        log_msg(
            f"\n[FightsScraper]: Finished scraping {len(collected_fights)} fights pages: {elapsed} seconds!\n"
        )

        await export_scraped_content(collected_fights)
