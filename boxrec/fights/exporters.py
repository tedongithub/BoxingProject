from datetime import datetime
import csv
import pathlib
import time

import pandas as pd

from config import ScrapeConfig
from fight import Fight
from utils import build_next_url
from db import get_fight_ids_in_fightsnips_table
from log import log_msg


async def export_scraped_content(collected_fights: list[str]) -> None:
    """Saves html scraped from collected_fights pages to a locally accessible directory."""

    start = time.perf_counter()

    scrape_output_dir = pathlib.Path().resolve() / 'snapshots'
    scrape_output_dir.mkdir(parents=True, exist_ok=True)

    for count, fight in enumerate(collected_fights):
        event_fight_id = fight.get("event_fight_id")
        response_text = fight.get('response_text')
        output_file = scrape_output_dir / f"id-{event_fight_id.replace('/','-')}.html"
        try:
            output_file.write_text(response_text)
            log_msg(f"Saved {count+1}/{len(collected_fights)} html files... event_fight_id#: {event_fight_id}")
        except:
            log_msg(f"event_fight_id: {event_fight_id}, did not have the proper encoding and was skipped!")
            continue

    elapsed = time.perf_counter() - start
    log_msg(f"\n[FightsExport]: Fights Scraper has saved the available fights page html bodies: {elapsed} seconds!\n")


async def save_df_to_csv(config: ScrapeConfig, df: pd.DataFrame) -> None:
    """Saves given pd.DataFrame to a .csv file."""

    start = time.perf_counter()

    filename = f"{config.csv_folder}{config.fights_csv_prefix}-{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.csv"
    df.to_csv(filename, encoding='utf-8', index=False)

    elapsed = time.perf_counter() - start
    log_msg(f"\n[DataStorage]: The fights dataframe was exported to a .csv file: {elapsed} seconds!\n")


async def save_all_fights_to_csv(config: ScrapeConfig, all_fights: list[Fight]) -> None:
    """Saves a given list of Fight instances to a .csv file."""

    start = time.perf_counter()

    filename = f"{config.csv_folder}{config.fights_csv_prefix}-{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.csv"

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for fight in all_fights:
                writer.writerow(
                    [

                        fight.event_fight_id,
                        fight.event_id,
                        fight.fight_id,
                        fight.fight_date,
                        fight.fight_winner_id,
                        fight.fight_winner_name,
                        fight.fight_loser_id,
                        fight.fight_loser_name,
                        fight.fight_weightclass,
                        fight.title_fight_flag,
                        fight.fight_venue,
                        fight.fight_rounds_completed,
                        fight.fight_rounds_scheduled,
                        fight.boxer_name_l,
                        fight.boxer_name_r,
                        fight.boxer_boxrec_id_l,
                        fight.boxer_boxrec_id_r,
                        fight.boxer_wins_before_l,
                        fight.boxer_losses_before_l,
                        fight.boxer_draws_before_l,
                        fight.boxer_kos_before_l,
                        fight.boxer_wins_before_r,
                        fight.boxer_losses_before_r,
                        fight.boxer_draws_before_r,
                        fight.boxer_kos_before_r,
                        fight.boxer_age_l,
                        fight.boxer_age_r,
                        fight.boxer_stance_l,
                        fight.boxer_stance_r,
                        fight.boxer_height_l,
                        fight.boxer_height_r,
                        fight.boxers_height_ft_l,
                        fight.boxers_height_ft_r,
                        fight.boxer_height_cm_l,
                        fight.boxer_height_cm_r,
                        fight.boxer_reach_l,
                        fight.boxer_reach_r,
                        fight.boxers_reach_in_l,
                        fight.boxers_reach_in_r,
                        fight.boxer_reach_cm_l,
                        fight.boxer_reach_cm_r,
                        fight.boxer_points_after_l,
                        fight.boxer_points_after_r,
                        fight.fight_referee_name,
                        fight.fight_judge1_name,
                        fight.fight_judge1_score_l,
                        fight.fight_judge1_score_r,
                        fight.fight_judge2_name,
                        fight.fight_judge2_score_l,
                        fight.fight_judge2_score_r,
                        fight.fight_judge3_name,
                        fight.fight_judge3_score_l,
                        fight.fight_judge3_score_r,

                        fight.fight_titles_avail
                    ]
                )
    except BaseException as e:
        print('BaseException:', filename)
    else:
        elapsed = time.perf_counter() - start
        log_msg(f"\n[DataStorage]: List of Fight(s) was exported to a .csv file: {elapsed} seconds!\n")


def get_fight_urls_not_in_archive(config: ScrapeConfig): 
    """Compares careers(fightsnip) table from database to saved files and returns a list of URLs not yet saved."""

    curr_fsnips_fight_ids = get_fight_ids_in_fightsnips_table()

    directory = config.fights_store
    files = [str(file).split('id-')[1].split('.html')[0].replace('-', '/')
             for file in list(pathlib.Path(directory).glob('*'))]
    return [build_next_url(config, x) for x in curr_fsnips_fight_ids if x not in files]
 