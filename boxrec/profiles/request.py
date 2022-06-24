import json
import random
import csv
import asyncio
from asyncio.proactor_events import _ProactorBasePipeTransport

import pandas as pd
from functools import wraps


async def create_agents_list() -> list:
    """Returns a list of user-agents from a locally accessible .csv file."""

    filename = "./agents/user-agents-list-2022_05_24-02_46_53_PM.csv"
    with open(filename, newline="") as f:
        reader = csv.reader(f)
        data = list(reader)
        colnames = ["user_agent"]
        df = pd.read_csv(filename)
        lst = df.user_agent.tolist()

        return lst


async def random_header_agent(lst) -> dict:
    """Returns a request header with random user-agent from a locally accessible .json file."""

    with open("./profile-config.json") as file:
        data = json.load(file)
        headers = dict(data["config"]["headers"])
        headers["User-Agent"] = str(random.choices(lst, k=1)[0])

        return headers


async def fetch(url, session, headers, boxrec_id=None) -> dict:
    """Http request with given parameters that returns dictionary of response data and metadata."""

    async with session.get(url, headers=headers) as response:
        await asyncio.sleep(random.randrange(1, 3))  # was (1,5) last working

        try:
            response_text = await response.text()
            status_code = response.status
            content_flag = bool("id#" in response_text.lower())
            if (
                url + "?&offset=100" in response_text
                and url + "?&offset=200" in response_text
            ):
                multipage_code = 2
            elif (
                url + "?&offset=100" in response_text
                and url + "?&offset=200" not in response_text
            ):
                multipage_code = 1
            else:
                multipage_code = 0

            return {
                "boxrec_id": boxrec_id,
                "status_code": status_code,
                "content_flag": content_flag,
                "multipage_code": multipage_code,
                "response_text": response_text,
                "url_string": url,
            }
        except:
            raise Exception("The request was not instantiated successfully.")


async def fetch_with_sem(sem, url, session, headers, boxrec_id=None) -> dict:
    """A wrapper function for URL requests to employ use of semaphore."""

    async with sem:

        return await fetch(url, session, headers, boxrec_id)


def silence_event_loop_closed(func):
    """Silences warning from potential closed event loop."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except RuntimeError as e:
            if str(e) != "Event loop is closed":
                raise

    return wrapper


_ProactorBasePipeTransport.__del__ = silence_event_loop_closed(
    _ProactorBasePipeTransport.__del__
)
