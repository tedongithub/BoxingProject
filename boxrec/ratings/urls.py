from pathlib import Path
import random
from typing import Generator

from config import ScrapeConfig


def get_files(path: Path) -> Generator[Path, None, None]:
    """Accepts a file path and returns a generator of all file within."""

    directory = path
    files = Path(directory).glob("*")

    return files


def get_folder_offsets(path: Generator[Path, None, None]) -> list[str]:
    """Accepts a generator of path and iterates over all files to extract the page offset number (URL parameter related to page number)."""

    li = []
    for file in path:
        with open(file, "r") as f:
            li.append(str(file).split("pg-")[1].split(".html")[0])

    return li


def offset_to_page_num(offest: str) -> str:
    """Accepts and converts a page file's offset to a legible page number as String."""

    page_num = str(int(int(offest) / 50))

    return page_num


def page_num_to_offset(page_num: str) -> str:
    """Accepts and converts a page file's page number to an offest (URL parameter) as String."""

    offest = str(int(int(page_num) * 50))

    return offest


def get_urls_rem(urls: list[str], path: Generator[Path, None, None]) -> list[str]:
    """Returns a random ordered list of URLs by comparing given URL list and URLs saved in the given path generator."""

    all_page_offsets = [url.split("offset=")[1] for url in urls]

    folder_page_nums = get_folder_offsets(path)
    folder_page_offsets = [page_num_to_offset(x) for x in folder_page_nums]

    offsets_rem = [x for x in all_page_offsets if x not in folder_page_offsets]
    urls_rem = [
        build_next_url(config.base_url, config.base_href, o) for o in offsets_rem
    ]
    random.shuffle(urls_rem)

    return urls_rem


def build_next_url(base_url: str, base_href: str, page_offset: int) -> str:
    """Creates a URL String from given baseURL, extension, and given page offset number."""

    url = base_url + base_href + str(page_offset)

    return url


async def curr_page_num(base_url: str, base_href: str, url: str) -> int:
    """Converts URL to a legible page number."""

    page_num = int(int(url.replace(base_url, "").replace(base_href, "")) / 50)

    return page_num


def get_urls(config: ScrapeConfig, get_all=False, how_many: int = 0) -> list:
    """Returns all or a specified number of URLs in a list, randomized."""

    r = [*range(0, 65450, 50)]
    urls = [build_next_url(config.base_url, config.base_href, o) for o in r]

    if get_all == True and how_many == 0:
        random.shuffle(urls)
        return urls

    elif get_all == False:
        return random.choices(urls, k=how_many)
