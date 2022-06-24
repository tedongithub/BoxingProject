import unicodedata
import asyncio
from pathlib import Path
from datetime import date, datetime
import re
import time

from bs4 import BeautifulSoup as bs
from typing import Any

from config import ScrapeConfig
from log import log_msg
from fightsnippet import FightSnippet, from_dict_to_dataclass
from transform import create_soup, clean_str, rekey, convert_to_float
from urls import get_br_id_from_file


class FightSnippetsParser:
    def __init__(self, config: ScrapeConfig) -> None:
        self.config = config
        self.files = self.mount_files()
        self.archived = self.mount_archived()

    def mount_files(self) -> list[Path]:

        directory = "snapshots"
        files = list(Path(directory).glob("*"))

        return files

    def mount_archived(self) -> Path:

        directory = self.config.collected_profiles_dir
        files = list(Path(directory).glob("*"))

        return files

    def add_boxer_name(self, h1_tags: bs, snip_dict: dict) -> None:

        for tag in h1_tags:
            if (
                tag.get("style")
                == "display:inline-block;margin-right:10px;line-height:30px;"
            ):
                snip_dict["boxer_name"] = clean_str(tag.text)

    def add_boxer_boxrec_id(self, h2_tags: bs, snip_dict: dict) -> None:

        for tag in h2_tags:
            if (
                tag.get("style")
                == "display:inline-block;line-height:100%;padding:0;margin:0;"
            ):
                snip_dict["br_boxer_id"] = int(clean_str(tag.text).split("#")[1])

    def add_snip_fight_id(self, snippet: bs, snip_dict: dict) -> None:

        if len(snippet.get("id")) > 0:
            snip_dict["fsnip_fight_id"] = int(clean_str(snippet.get("id")))

    def add_snip_fight_date(self, snippet: bs, snip_dict: dict) -> None:

        if len(snippet.find_all("a", href=re.compile("date"))) > 0:
            snip_dict["fsnip_fight_date"] = datetime.strptime(
                clean_str(snippet.find_all("a", href=re.compile("date"))[-1].text),
                "%Y-%m-%d",
            ).date()

    def add_snip_boxer_weighin_weight(self, snippet: bs, snip_dict: dict) -> None:

        if len(snippet.select("td:nth-child(3)")) == 1:
            try:
                snip_dict["fsnip_boxer_weighin_weight"] = convert_to_float(
                    clean_str(snippet.select_one("td:nth-child(3)").text)
                )
            except:
                pass

    def add_snip_opponent_name(self, snippet: bs, snip_dict: dict) -> None:

        if len(snippet.find_all("a", href=re.compile("proboxer"))) > 0:
            snip_dict["fsnip_opp_name"] = clean_str(
                snippet.find_all("a", href=re.compile("proboxer"))[-1].text
            )

    def add_snip_opponent_id(self, snippet: bs, snip_dict: dict) -> None:

        if len(snippet.find_all("a", href=re.compile("proboxer"))) > 0:
            snip_dict["fsnip_br_opp_id"] = int(
                clean_str(
                    snippet.find_all("a", href=re.compile("proboxer"))[-1]
                    .get("href")
                    .split("r/")[1]
                )
            )

    def add_snip_opponent_weighin_weight(self, snippet: bs, snip_dict: dict) -> None:

        if bool(snippet.find("a", href=re.compile("proboxer"))):
            try:
                snip_dict["fsnip_opp_weighin_weight"] = convert_to_float(
                    clean_str(
                        snippet.find(
                            "a", href=re.compile("proboxer")
                        ).parent.next_sibling.text
                    )
                )
            except:
                pass

    def add_snip_fight_result(self, snippet: bs, snip_dict: dict) -> None:

        if len(snippet.find_all("div", {"class": "boutResult"})) > 0:
            snip_dict["fsnip_fight_result"] = clean_str(
                snippet.find_all("div", {"class": "boutResult"})[0].text
            )

    def add_snip_fight_result_type(self, snippet: bs, snip_dict: dict) -> None:

        if len(snippet.find_all("div", {"class": "boutResult"})) > 0:
            snip_dict["fsnip_fight_result_type"] = clean_str(
                snippet.find_all("div", {"class": "boutResult"})[
                    0
                ].parent.next_sibling.text
            )

    def add_snip_fight_rounds_completed(self, snippet: bs, snip_dict: dict) -> None:

        if len(snippet.find_all("div", {"class": "boutResult"})) > 0:
            try:
                snip_dict["fsnip_fight_rounds_completed"] = int(
                    clean_str(
                        snippet.find_all("div", {"class": "boutResult"})[
                            0
                        ].parent.next_sibling.next_sibling.text.split("/")[0]
                    )
                )
            except:
                pass

    def add_snip_fight_rounds_scheduled(self, snippet: bs, snip_dict: dict) -> None:

        if len(snippet.find_all("div", {"class": "boutResult"})) > 0:
            try:
                snip_dict["fsnip_fight_rounds_scheduled"] = int(
                    clean_str(
                        snippet.find_all("div", {"class": "boutResult"})[
                            0
                        ].parent.next_sibling.next_sibling.text.split("/")[1]
                    )
                )
            except:
                pass

    def add_snip_event_fight_id(self, snippet: bs, snip_dict: dict) -> None:

        if len(snippet.find_all("a", href=re.compile("event"))) > 0:
            snip_dict["fsnip_event_fight_id"] = clean_str(
                snippet.find_all("a", href=re.compile("event"))[-1]
                .get("href")
                .split("t/")[1]
            )

    def add_boxer_name2(self, h1_tags: bs) -> str:

        for tag in h1_tags:
            if (
                tag.get("style")
                == "display:inline-block;margin-right:10px;line-height:30px;"
            ):
                return clean_str(tag.text)

    def add_boxer_boxrec_id2(self, h2_tags: bs) -> str:

        for tag in h2_tags:
            if (
                tag.get("style")
                == "display:inline-block;line-height:100%;padding:0;margin:0;"
            ):
                return int(clean_str(tag.text).split("#")[1])

    async def extract_snip(self, snip_top: bs) -> FightSnippet:

        snip_dict = {}

        snip_bot = snip_top.next_sibling

        self.add_snip_fight_id(snip_top, snip_dict)
        self.add_snip_fight_date(snip_top, snip_dict)
        self.add_snip_boxer_weighin_weight(snip_top, snip_dict)
        self.add_snip_opponent_name(snip_top, snip_dict)
        self.add_snip_opponent_id(snip_top, snip_dict)
        self.add_snip_opponent_weighin_weight(snip_top, snip_dict)
        self.add_snip_fight_result(snip_top, snip_dict)
        self.add_snip_fight_result_type(snip_top, snip_dict)
        self.add_snip_fight_rounds_completed(snip_top, snip_dict)
        self.add_snip_fight_rounds_scheduled(snip_top, snip_dict)
        self.add_snip_event_fight_id(snip_top, snip_dict)

        return from_dict_to_dataclass(FightSnippet, snip_dict)

    async def extract_snippets(self, response_text: str) -> list[FightSnippet]:

        soup = await create_soup(response_text)

        h1_tags = soup.find_all("h1")
        h2_tags = soup.find_all("h2")
        snip_tops = [x for x in soup.find_all("tr", {"class": "drawRowBorder"})]

        all_snips = []

        for count, snip_top in enumerate(snip_tops):
            snip = await self.extract_snip(snip_top)
            snip.boxer_name = self.add_boxer_name2(h1_tags)
            snip.br_boxer_id = self.add_boxer_boxrec_id2(h2_tags)
            all_snips.append(snip)

        return all_snips

    async def parse(self) -> list[FightSnippet]:

        start = time.perf_counter()

        all_fightsnips = []

        files = self.files

        for count, file in enumerate(files):
            with open(file, "r", encoding="utf-8") as f:
                response_text = f.read()

                boxrec_id = await get_br_id_from_file(file)

                try:
                    career = await self.extract_snippets(response_text)
                    all_fightsnips.append(career)
                    log_msg(
                        f"Fight Snips Parser extracted data from html file: {count+1}/{len(list(files))}... boxer-ID#: {boxrec_id}"
                    )
                except:
                    log_msg(
                        f"Parsing did NOT complete for fightsnip file: {count+1}/{len(list(files))}... boxer-ID#: {boxrec_id}"
                    )
                    continue

        elapsed = time.perf_counter() - start
        log_msg(
            f"\n[FightSnippetsParser]: Fight Snips Parser extracted data from available html files: {elapsed} seconds!\n"
        )

        return all_fightsnips
