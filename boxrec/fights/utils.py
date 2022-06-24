from config import ScrapeConfig


def build_next_url(config: ScrapeConfig, event_fight_id: str) -> str:

    url = config.base_url + config.base_href + event_fight_id

    return url


async def curr_event_fight_id(url: str) -> str:

    return url.split("t/")[1].strip()
