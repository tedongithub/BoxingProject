from pathlib import Path
from datetime import date, datetime
import re
import time

from bs4 import BeautifulSoup as bs

from config import ScrapeConfig
from fight import Fight, from_dict_to_dataclass
from log import log_msg
from wrangle import (
    create_soup,
    clean_str,
    curr_event_fight_id,
    convert_to_float,
    convert_to_feet_float,
)
from exporters import save_df_to_csv, save_all_fights_to_csv
from db import make_new_fights_table, insert_fights_df_to_db


class FightParser:
    def __init__(self, config: ScrapeConfig):
        self.config = config
        self.files = self.mount_files()
        self.archived = self.mount_archived()

    def mount_files(self) -> list[Path]:
        """Used in class initializer to make available the snapshots folder as a class instance variable."""

        directory = "snapshots"
        files = list(Path(directory).glob("*"))

        return files

    def mount_archived(self) -> Path:
        """Used in class initializer to make available the snapshots folder as a class instance variable."""

        directory = self.config.fights_store
        files = list(Path(directory).glob("*"))

        return files

    def add_fight_date(self, h2_tags: list[bs], fight_dict: dict) -> None:

        for tag in h2_tags:
            if tag.a:
                fight_date = clean_str(tag.text)
        fight_dict["fight_date"] = datetime.strptime(fight_date, "%A %d, %B %Y").date()

    def add_fight_weightclass_and_rounds(
        self, tags: list[str], fight_dict: dict
    ) -> None:

        for tag in tags:
            if "contest" in tag.text.lower():
                fight_contest_type, fight_rounds_scheduled = clean_str(tag.text).split(
                    ","
                )
                fight_dict["fight_weightclass"] = clean_str(
                    fight_contest_type.split("C")[0]
                )
                fight_dict["fight_rounds_scheduled"] = int(
                    fight_rounds_scheduled.lower().split("r")[0]
                )

    def add_fight_venue(self, div: bs, fight_dict: dict) -> None:

        fight_venue = [clean_str(tag.text) for tag in div.find_all("a")[1:]]
        fight_dict["fight_venue"] = ", ".join(fight_venue)

    def add_fight_referee(self, table_top: bs, fight_dict: dict) -> None:

        if bool(table_top.find("a", href=re.compile("referee"))):
            fight_dict["fight_referee_name"] = clean_str(
                table_top.find("a", href=re.compile("referee")).text
            )

    def add_referee_scores(self, table_top: bs, fight_dict: dict) -> None:

        if bool(table_top.find("a", href=re.compile("referee"))):
            referee_tag = table_top.find("a", href=re.compile("referee")).parent
            try:
                fight_dict["fight_judge3_score_l"] = int(
                    clean_str(referee_tag.previous_sibling.previous_sibling.text)
                )
                fight_dict["fight_judge3_score_r"] = int(
                    clean_str(referee_tag.next_sibling.next_sibling.text)
                )
                fight_dict["fight_judge3_name"] = clean_str(
                    table_top.find("a", href=re.compile("referee")).text
                )
            except:
                pass

    def add_fight_judges_names(self, judge_tags: list[bs], fight_dict: dict) -> None:

        if len(judge_tags) == 3:
            fight_dict["fight_judge1_name"] = clean_str(judge_tags[0].text)
            fight_dict["fight_judge2_name"] = clean_str(judge_tags[1].text)
            fight_dict["fight_judge3_name"] = clean_str(judge_tags[2].text)
        elif len(judge_tags) == 2:
            fight_dict["fight_judge1_name"] = clean_str(judge_tags[0].text)
            fight_dict["fight_judge2_name"] = clean_str(judge_tags[1].text)

    def add_fight_judges_scores_l(
        self, fight_judges_tags: list[bs], fight_dict: dict
    ) -> None:

        if len(fight_judges_tags) == 3:
            try:
                fight_dict["fight_judge1_score_l"] = int(
                    clean_str(
                        fight_judges_tags[0].previous_sibling.previous_sibling.text
                    )
                )
                fight_dict["fight_judge2_score_l"] = int(
                    clean_str(
                        fight_judges_tags[1].previous_sibling.previous_sibling.text
                    )
                )
                fight_dict["fight_judge3_score_l"] = int(
                    clean_str(
                        fight_judges_tags[2].previous_sibling.previous_sibling.text
                    )
                )
            except:
                pass
        elif len(fight_judges_tags) == 2:
            try:
                fight_dict["fight_judge1_score_l"] = int(
                    clean_str(
                        fight_judges_tags[0].previous_sibling.previous_sibling.text
                    )
                )
                fight_dict["fight_judge2_score_l"] = int(
                    clean_str(
                        fight_judges_tags[1].previous_sibling.previous_sibling.text
                    )
                )
            except:
                pass

    def add_fight_judges_scores_r(
        self, fight_judges_tags: list[bs], fight_dict: dict
    ) -> None:

        if len(fight_judges_tags) == 3:
            try:
                fight_dict["fight_judge1_score_r"] = int(
                    clean_str(fight_judges_tags[0].next_sibling.next_sibling.text)
                )
                fight_dict["fight_judge2_score_r"] = int(
                    clean_str(fight_judges_tags[1].next_sibling.next_sibling.text)
                )
                fight_dict["fight_judge3_score_r"] = int(
                    clean_str(fight_judges_tags[2].next_sibling.next_sibling.text)
                )
            except:
                fight_dict["fight_judge1_score_r"] = 0
                fight_dict["fight_judge2_score_r"] = 0
                fight_dict["fight_judge3_score_r"] = 0
        elif len(fight_judges_tags) == 2:
            try:
                fight_dict["fight_judge1_score_r"] = int(
                    clean_str(fight_judges_tags[0].next_sibling.next_sibling.text)
                )
                fight_dict["fight_judge2_score_r"] = int(
                    clean_str(fight_judges_tags[1].next_sibling.next_sibling.text)
                )
            except:
                pass

    def add_boxer_name_l(self, boxer_name_tag_l: bs, fight_dict: dict) -> None:

        fight_dict["boxer_name_l"] = clean_str(boxer_name_tag_l.a.text)

    def add_boxer_name_r(self, boxer_name_tag_r: bs, fight_dict: dict) -> None:

        fight_dict["boxer_name_r"] = clean_str(boxer_name_tag_r.a.text)

    def add_boxers_boxrec_ids(
        self, boxer_name_tag_l: bs, boxer_name_tag_r: bs, fight_dict: dict
    ) -> None:

        fight_dict["boxer_boxrec_id_l"] = int(
            clean_str(boxer_name_tag_l.a.get("href").split("r/")[1])
        )
        fight_dict["boxer_boxrec_id_r"] = int(
            clean_str(boxer_name_tag_r.a.get("href").split("r/")[1])
        )

    def add_winner_loser_labels(
        self, boxer_name_tag_l: bs, boxer_name_tag_r: bs, fight_dict
    ) -> None:

        winner_flag = bool(boxer_name_tag_l.find("span", "textWon"))

        if winner_flag != True:
            fight_dict["fight_winner_id"] = int(
                clean_str(boxer_name_tag_r.a.get("href").split("r/")[1])
            )
            fight_dict["fight_loser_id"] = int(
                clean_str(boxer_name_tag_l.a.get("href").split("r/")[1])
            )
            fight_dict["fight_winner_name"] = clean_str(boxer_name_tag_r.a.text)
            fight_dict["fight_loser_name"] = clean_str(boxer_name_tag_l.a.text)
        fight_dict["fight_winner_id"] = int(
            clean_str(boxer_name_tag_l.a.get("href").split("r/")[1])
        )
        fight_dict["fight_loser_id"] = int(
            clean_str(boxer_name_tag_r.a.get("href").split("r/")[1])
        )
        fight_dict["fight_winner_name"] = clean_str(boxer_name_tag_l.a.text)
        fight_dict["fight_loser_name"] = clean_str(boxer_name_tag_r.a.text)

    def add_boxers_points_after(self, after_fight_tag: bs, fight_dict: dict) -> None:

        if bool(after_fight_tag.previous_sibling.previous_sibling.text):
            try:
                fight_dict["boxer_points_after_l"] = float(
                    clean_str(
                        after_fight_tag.previous_sibling.previous_sibling.text.split(
                            "p"
                        )[0]
                    )
                )
            except:
                pass
            try:
                fight_dict["boxer_points_after_r"] = float(
                    clean_str(
                        after_fight_tag.next_sibling.next_sibling.text.split("p")[0]
                    )
                )
            except:
                pass

    def add_boxers_ages(self, boxer_age_tag: bs, fight_dict: dict) -> None:

        try:
            fight_dict["boxer_age_l"] = int(
                clean_str(boxer_age_tag.previous_sibling.previous_sibling.text)
            )
            fight_dict["boxer_age_r"] = int(
                clean_str(boxer_age_tag.next_sibling.next_sibling.text)
            )
        except:
            pass

    def add_boxers_stances(self, boxer_stance_tag: bs, fight_dict: dict) -> None:

        if clean_str(boxer_stance_tag.previous_sibling.previous_sibling.text) != "":
            fight_dict["boxer_stance_l"] = clean_str(
                boxer_stance_tag.previous_sibling.previous_sibling.text
            )
        if clean_str(boxer_stance_tag.next_sibling.next_sibling.text) != "":
            fight_dict["boxer_stance_r"] = clean_str(
                boxer_stance_tag.next_sibling.next_sibling.text
            )

    def add_boxers_heights(self, boxer_height_tag: bs, fight_dict: dict) -> None:

        if clean_str(boxer_height_tag.previous_sibling.previous_sibling.text) != "":
            fight_dict["boxer_height_l"] = clean_str(
                boxer_height_tag.previous_sibling.previous_sibling.text
            )
        if clean_str(boxer_height_tag.next_sibling.next_sibling.text) != "":
            fight_dict["boxer_height_r"] = clean_str(
                boxer_height_tag.next_sibling.next_sibling.text
            )

    def add_boxers_reach(self, boxer_reach_tag: bs, fight_dict: dict) -> None:

        if clean_str(boxer_reach_tag.previous_sibling.previous_sibling.text) != "":
            fight_dict["boxer_reach_l"] = clean_str(
                boxer_reach_tag.previous_sibling.previous_sibling.text
            )
        if clean_str(boxer_reach_tag.next_sibling.next_sibling.text) != "":
            fight_dict["boxer_reach_r"] = clean_str(
                boxer_reach_tag.next_sibling.next_sibling.text
            )

    def add_boxers_wins_before(
        self, boxer_wins_before_tag: bs, fight_dict: dict
    ) -> None:

        fight_dict["boxer_wins_before_l"] = int(
            clean_str(boxer_wins_before_tag.previous_sibling.previous_sibling.text)
        )
        fight_dict["boxer_wins_before_r"] = int(
            clean_str(boxer_wins_before_tag.next_sibling.next_sibling.text)
        )

    def add_boxers_losses_before(
        self, boxers_losses_before_tag: bs, fight_dict: dict
    ) -> None:

        fight_dict["boxer_losses_before_l"] = int(
            clean_str(boxers_losses_before_tag.previous_sibling.previous_sibling.text)
        )
        fight_dict["boxer_losses_before_r"] = int(
            clean_str(boxers_losses_before_tag.next_sibling.next_sibling.text)
        )

    def add_boxers_draws_before(
        self, boxers_draws_before_tag: bs, fight_dict: dict
    ) -> None:

        fight_dict["boxer_draws_before_l"] = int(
            clean_str(boxers_draws_before_tag.previous_sibling.previous_sibling.text)
        )
        fight_dict["boxer_draws_before_r"] = int(
            clean_str(boxers_draws_before_tag.next_sibling.next_sibling.text)
        )

    def add_boxers_kos_before(
        self, boxers_kos_before_tag: bs, fight_dict: dict
    ) -> None:

        fight_dict["boxer_kos_before_l"] = int(
            clean_str(boxers_kos_before_tag.previous_sibling.previous_sibling.text)
        )
        fight_dict["boxer_kos_before_r"] = int(
            clean_str(boxers_kos_before_tag.next_sibling.next_sibling.text)
        )

    def add_title_fight_flag(self, title_fight_flag_tag: bs, fight_dict: dict) -> None:

        if bool(title_fight_flag_tag) == True:
            fight_dict["title_fight_flag"] = "Title Fight"

    def add_fight_titles_avail(
        self, title_fight_flag_tag: bs, fight_dict: dict
    ) -> None:

        if bool(title_fight_flag_tag) == True:
            fight_titles_avail_tags = title_fight_flag_tag.find_all("a")

            fight_dict["fight_titles_avail"] = [
                clean_str(x.text) for x in fight_titles_avail_tags
            ]

    def add_fight_rounds_completed(
        self, boxer_name_tag_l: bs, h2_tags: list[bs], fight_dict: dict
    ) -> None:

        if "round" in boxer_name_tag_l.parent.text.lower():
            fight_dict["fight_rounds_completed"] = int(
                clean_str(boxer_name_tag_l.parent.text.lower().split("round")[1])[:2]
            )
        elif "round" not in boxer_name_tag_l.parent.text.lower():
            for tag in h2_tags:
                if "contest" in tag.text.lower():
                    fight_rounds_scheduled = clean_str(tag.text).split(",")[1]
                    fight_dict["fight_rounds_completed"] = int(
                        fight_rounds_scheduled.lower().split("r")[0]
                    )

    def add_boxers_reach_cm(self, boxer_reach_tag: bs, fight_dict: dict) -> None:

        try:
            fight_dict["boxer_reach_cm_l"] = int(
                clean_str(
                    boxer_reach_tag.previous_sibling.previous_sibling.text.split("/")[
                        1
                    ].split("cm")[0]
                )
            )
            fight_dict["boxer_reach_cm_r"] = int(
                clean_str(
                    boxer_reach_tag.next_sibling.next_sibling.text.split("/")[1].split(
                        "cm"
                    )[0]
                )
            )
        except:
            pass

    def add_boxers_height_cm(self, boxer_height_tag: bs, fight_dict: dict) -> None:

        try:
            fight_dict["boxer_height_cm_l"] = int(
                clean_str(
                    boxer_height_tag.previous_sibling.previous_sibling.text.split("/")[
                        1
                    ].split("cm")[0]
                )
            )
            fight_dict["boxer_height_cm_r"] = int(
                clean_str(
                    boxer_height_tag.next_sibling.next_sibling.text.split("/")[1].split(
                        "cm"
                    )[0]
                )
            )
        except:
            pass

    def add_boxers_reach_in(self, boxer_reach_tag: bs, fight_dict: dict) -> None:

        try:
            fight_dict["boxers_reach_in_l"] = convert_to_feet_float(
                clean_str(
                    boxer_reach_tag.previous_sibling.previous_sibling.text.split("/")[0]
                )
            )
        except:
            pass
        try:
            fight_dict["boxers_reach_in_r"] = convert_to_feet_float(
                clean_str(boxer_reach_tag.next_sibling.next_sibling.text.split("/")[0])
            )
        except:
            pass

    def add_boxers_height_ft(self, boxer_height_tag: bs, fight_dict: dict) -> None:

        try:
            fight_dict["boxers_height_ft_l"] = convert_to_feet_float(
                clean_str(
                    boxer_height_tag.previous_sibling.previous_sibling.text.split("/")[
                        0
                    ]
                )
            )
        except:
            pass
        try:
            fight_dict["boxers_height_ft_r"] = convert_to_feet_float(
                clean_str(boxer_height_tag.next_sibling.next_sibling.text.split("/")[0])
            )
        except:
            pass

    async def extract_fight(self, response_text: str) -> Fight:

        fight_dict = {}

        soup = await create_soup(response_text)

        h2_tags = soup.find_all("h2")
        div_venue_chk = soup.find("div", style="text-align:left;display:inline-block;")
        table_top = soup.select_one(
            "div.singleColumn > div.overflowScroll > table > tr:nth-child(1)"
        )
        judge_tags = table_top.find_all("a", href=re.compile("judge"))
        fight_judges_tags = [
            x.parent for x in table_top.find_all("a", href=re.compile("judge"))
        ]
        boxer_name_tag_l = soup.find("td", style="text-align:right;")
        boxer_name_tag_r = soup.find("td", style="text-align:left;")
        after_fight_tag = soup.find("b", text="after fight").parent
        boxer_age_tag = soup.find("b", text="age").parent
        boxer_stance_tag = soup.find("b", text="stance").parent
        boxer_height_tag = soup.find("b", text="height").parent
        boxer_reach_tag = soup.find("b", text="reach").parent
        boxer_wins_before_tag = soup.find("b", text="won").parent
        boxers_losses_before_tag = soup.find("b", text="lost").parent
        boxers_draws_before_tag = soup.find("b", text="drawn").parent
        boxers_kos_before_tag = soup.find("b", text="KOs").parent
        title_fight_flag_tag = soup.find("div", "titleColor")

        self.add_fight_date(h2_tags, fight_dict)
        self.add_fight_weightclass_and_rounds(h2_tags, fight_dict)
        self.add_fight_venue(div_venue_chk, fight_dict)
        self.add_fight_referee(table_top, fight_dict)
        self.add_fight_judges_names(judge_tags, fight_dict)
        self.add_fight_judges_scores_l(fight_judges_tags, fight_dict)
        self.add_fight_judges_scores_r(fight_judges_tags, fight_dict)
        self.add_boxer_name_l(boxer_name_tag_l, fight_dict)
        self.add_boxer_name_r(boxer_name_tag_r, fight_dict)
        self.add_winner_loser_labels(boxer_name_tag_l, boxer_name_tag_r, fight_dict)
        self.add_boxers_points_after(after_fight_tag, fight_dict)
        self.add_boxers_boxrec_ids(boxer_name_tag_l, boxer_name_tag_r, fight_dict)
        self.add_boxers_ages(boxer_age_tag, fight_dict)
        self.add_boxers_stances(boxer_stance_tag, fight_dict)
        self.add_boxers_heights(boxer_height_tag, fight_dict)
        self.add_boxers_reach(boxer_reach_tag, fight_dict)
        self.add_boxers_wins_before(boxer_wins_before_tag, fight_dict)
        self.add_boxers_losses_before(boxers_losses_before_tag, fight_dict)
        self.add_boxers_draws_before(boxers_draws_before_tag, fight_dict)
        self.add_boxers_kos_before(boxers_kos_before_tag, fight_dict)
        self.add_title_fight_flag(title_fight_flag_tag, fight_dict)
        self.add_fight_titles_avail(title_fight_flag_tag, fight_dict)
        self.add_referee_scores(table_top, fight_dict)
        self.add_fight_rounds_completed(boxer_name_tag_l, h2_tags, fight_dict)
        self.add_boxers_reach_cm(boxer_reach_tag, fight_dict)
        self.add_boxers_height_cm(boxer_height_tag, fight_dict)
        self.add_boxers_height_ft(boxer_height_tag, fight_dict)
        self.add_boxers_reach_in(boxer_reach_tag, fight_dict)

        return from_dict_to_dataclass(Fight, fight_dict)

    async def parse(self) -> list[Fight]:

        start = time.perf_counter()

        all_fights = []

        files = self.archived

        for count, file in enumerate(files):
            with open(file, "r") as f:
                response_text = f.read()

                event_fight_id = await curr_event_fight_id(file)

                fight = await self.extract_fight(response_text)
                fight.event_fight_id = event_fight_id
                fight.fight_id = int(event_fight_id.split("/")[1])
                fight.event_id = int(event_fight_id.split("/")[0])
                all_fights.append(fight)
                log_msg(
                    f"Fight Parser extracted data from html file: {count+1}/{len(list(files))}... event_fight_id - ID#: {event_fight_id}"
                )

        elapsed = time.perf_counter() - start
        log_msg(
            f"\n[FightsParser]: Fights Parser extracted data from available html files: {elapsed} seconds!\n"
        )

        return all_fights
