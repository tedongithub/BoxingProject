import unicodedata
import asyncio
from pathlib import Path
from datetime import date, datetime
import time

from bs4 import BeautifulSoup as bs
from typing import Any

from config import ScrapeConfig, read_config
from log import log_msg
from profile import BoxerProfile, from_dict_to_dataclass
from urls import get_br_id_from_file
from transform import (
    create_soup,
    clean_str,
    rekey,
    convert_to_float,
    convert_to_feet_float,
)


class ProfilesParser:
    def __init__(self, config: ScrapeConfig):
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

    def add_boxer_name(self, h1_tags: bs, profile_dict: dict) -> None:

        for tag in h1_tags:
            if (
                tag.get("style")
                == "display:inline-block;margin-right:10px;line-height:30px;"
            ):
                profile_dict["boxer_name"] = tag.text.strip()

    def add_br_boxer_id(self, h2_tags: bs, profile_dict: dict) -> None:

        for tag in h2_tags:
            if (
                tag.get("style")
                == "display:inline-block;line-height:100%;padding:0;margin:0;"
            ):
                profile_dict["br_boxer_id"] = int(clean_str(tag.text).split("#")[1])

    def add_boxer_win_tot(self, boxer_profile_bgw: bs, profile_dict: dict) -> None:

        if len(boxer_profile_bgw) == 1:
            profile_dict["boxer_win_tot"] = int(clean_str(boxer_profile_bgw[0].text))

    def add_boxer_loss_tot(self, boxer_profile_bgl: bs, profile_dict: dict) -> None:

        if len(boxer_profile_bgl) == 1:
            profile_dict["boxer_loss_tot"] = int(clean_str(boxer_profile_bgl[0].text))

    def add_boxer_draw_tot(self, boxer_profile_bgd: bs, profile_dict: dict) -> None:

        if len(boxer_profile_bgd) == 1:
            profile_dict["boxer_draw_tot"] = int(clean_str(boxer_profile_bgd[0].text))

    def add_boxer_ko_win_tot(self, profile_wld_table: bs, profile_dict: dict) -> None:

        if bool(profile_wld_table[0].find("th", {"class": "textWon"}).text):
            profile_dict["boxer_ko_win_tot"] = int(
                clean_str(
                    profile_wld_table[0]
                    .find("th", {"class": "textWon"})
                    .text.split("K")[0]
                )
            )

    def add_boxer_ko_loss_tot(self, profile_wld_table: bs, profile_dict: dict) -> None:

        if bool(profile_wld_table[0].find("th", {"class": "textLost"}).text):
            profile_dict["boxer_ko_loss_tot"] = int(
                clean_str(
                    profile_wld_table[0]
                    .find("th", {"class": "textLost"})
                    .text.split("K")[0]
                )
            )

    def add_boxer_profile_attr(
        self, tags: list[bs], profile_dict: dict, attr: str
    ) -> None:

        for tag in tags:

            if attr in tag.text:
                profile_dict[attr] = clean_str(tag.next_sibling.next_sibling.text)

    def add_boxer_titles_held(
        self, tags: list[bs], profile_dict: dict, attr: str = "titles held"
    ) -> None:

        for tag in tags:
            if attr in tag.text and tag.next_sibling.next_sibling.find_all("a"):
                profile_dict["boxer_titles_held"] = [
                    clean_str(t.text)
                    for t in tag.next_sibling.next_sibling.find_all("a")
                ]

    def add_boxer_height_ft(self, profile_dict: dict) -> None:

        try:
            profile_dict["boxer_height_ft"] = convert_to_feet_float(
                clean_str(profile_dict.get("boxer_height").split("/")[0])
            )
        except:
            pass

    def add_boxer_height_cm(self, profile_dict: dict) -> None:

        try:
            profile_dict["boxer_height_cm"] = int(
                clean_str(profile_dict.get("boxer_height").split("/")[1].split("cm")[0])
            )
        except:
            pass

    def add_boxer_reach_in(self, profile_dict: dict) -> None:

        try:
            profile_dict["boxer_reach_in"] = convert_to_feet_float(
                clean_str(profile_dict.get("boxer_reach").split("/")[0])
            )
        except:
            pass

    def add_boxer_reach_cm(self, profile_dict: dict) -> None:

        try:
            profile_dict["boxer_reach_cm"] = int(
                clean_str(profile_dict.get("boxer_reach").split("/")[1].split("cm")[0])
            )
        except:
            pass

    def add_boxer_wins_pct(self, profile_dict: dict) -> None:

        try:
            profile_dict["boxer_wins_pct"] = float(
                int(profile_dict.get("boxer_win_tot"))
                / int(profile_dict.get("boxer_bouts_tot"))
            )
        except:
            pass

    def add_career_start_yr(self, profile_dict: dict) -> None:

        try:
            profile_dict["career_start_year"] = int(
                profile_dict.get("boxer_career_span").split("-")[0]
            )
        except:
            pass

    def add_career_end_yr(self, profile_dict: dict) -> None:

        try:
            profile_dict["career_end_year"] = int(
                profile_dict.get("boxer_career_span").split("-")[1]
            )
        except:
            pass

    async def extract_profile(self, response_text: str) -> BoxerProfile:

        soup = await create_soup(response_text)

        h1_tags = soup.find_all("h1")
        h2_tags = soup.find_all("h2")
        profile_wld_table = soup.find_all("table", {"class": "profileWLD"})
        boxer_profile_bgw = soup.find_all("td", {"class": "bgW"})
        boxer_profile_bgl = soup.find_all("td", {"class": "bgL"})
        boxer_profile_bgd = soup.find_all("td", {"class": "bgD"})
        row_label_class = soup.find_all("td", {"class": "rowLabel"})

        profile_dict = {}

        self.add_boxer_name(h1_tags, profile_dict)
        self.add_br_boxer_id(h2_tags, profile_dict)
        self.add_boxer_win_tot(boxer_profile_bgw, profile_dict)
        self.add_boxer_loss_tot(boxer_profile_bgl, profile_dict)
        self.add_boxer_draw_tot(boxer_profile_bgd, profile_dict)
        self.add_boxer_ko_win_tot(profile_wld_table, profile_dict)
        self.add_boxer_ko_loss_tot(profile_wld_table, profile_dict)
        self.add_boxer_profile_attr(row_label_class, profile_dict, "division")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "bouts")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "rounds")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "KOs")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "career")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "debut")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "birth name")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "alias")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "age")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "nationality")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "stance")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "height")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "reach")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "residence")
        self.add_boxer_profile_attr(row_label_class, profile_dict, "birth place")

        self.add_boxer_titles_held(row_label_class, profile_dict)

        d3 = {
            "division": "weightclass",
            "bouts": "boxer_bouts_tot",
            "rounds": "boxer_rounds_tot",
            "KOs": "boxer_kos_pct",
            "career": "boxer_career_span",
            "debut": "boxer_debut_date",
            "birth name": "boxer_birth_name",
            "alias": "boxer_alias_name",
            "age": "boxer_age",
            "nationality": "boxer_nation",
            "stance": "boxer_stance",
            "height": "boxer_height",
            "reach": "boxer_reach",
            "residence": "boxer_residence",
            "birth place": "boxer_birthplace",
        }

        profile_dict = rekey(profile_dict, d3)

        if "boxer_bouts_tot" in profile_dict:
            profile_dict["boxer_bouts_tot"] = int(profile_dict.get("boxer_bouts_tot"))
        if "boxer_rounds_tot" in profile_dict:
            profile_dict["boxer_rounds_tot"] = int(profile_dict.get("boxer_rounds_tot"))
        if "boxer_kos_pct" in profile_dict:
            profile_dict["boxer_kos_pct"] = float(
                profile_dict.get("boxer_kos_pct").replace("%", "")
            )
        if "boxer_debut_date" in profile_dict:
            profile_dict["boxer_debut_date"] = datetime.strptime(
                profile_dict.get("boxer_debut_date"), "%Y-%m-%d"
            ).date()
        if "boxer_age" in profile_dict:
            profile_dict["boxer_age"] = int(profile_dict.get("boxer_age"))

        self.add_boxer_wins_pct(profile_dict)
        self.add_career_start_yr(profile_dict)
        self.add_career_end_yr(profile_dict)
        self.add_boxer_height_ft(profile_dict)
        self.add_boxer_height_cm(profile_dict)
        self.add_boxer_reach_in(profile_dict)
        self.add_boxer_reach_cm(profile_dict)

        return from_dict_to_dataclass(BoxerProfile, profile_dict)

    async def parse(self) -> list[BoxerProfile]:

        start = time.perf_counter()

        all_profiles = []

        files = self.files

        for count, file in enumerate(files):

            boxrec_id = await get_br_id_from_file(file)

            if "-2" not in boxrec_id and "-1" not in boxrec_id:
                with open(file, "r", encoding="utf-8") as f:
                    response_text = f.read()

                    try:
                        profile = await self.extract_profile(response_text)
                        all_profiles.append(profile)
                        log_msg(
                            f"Profile Parser extracted data from html file: {count+1}/{len(list(files))}... boxer-ID#: {boxrec_id}"
                        )
                    except:
                        log_msg(
                            f"Profile Parsing did NOT complete for file: {count+1}/{len(list(files))}... boxer-ID#: {boxrec_id}"
                        )
                        continue

        elapsed = time.perf_counter() - start
        log_msg(
            f"\n[ProfilesParser]: Profile Parser extracted data from available html files: {elapsed} seconds!\n"
        )

        return all_profiles
