import time
import asyncio
from datetime import datetime
import csv
import pathlib

import pandas as pd

from config import ScrapeConfig
from profile import BoxerProfile
from fightsnippet import FightSnippet
from log import log_msg


async def export_scraped_content(collected_profiles: list[str]) -> None:
    """Saves html scraped from collected_profiles pages to a locally accessible directory."""

    start = time.perf_counter()

    scrape_output_dir = pathlib.Path().resolve() / "snapshots"
    scrape_output_dir.mkdir(parents=True, exist_ok=True)

    for count, profile in enumerate(collected_profiles):
        boxrec_id = profile.get("boxrec_id")
        response_text = profile.get("response_text")
        url_string = profile.get("url_string")

        if "?" and "100" in url_string:
            output_file = scrape_output_dir / f"id-{boxrec_id}-1.html"
        elif "?" and "200" in url_string:
            output_file = scrape_output_dir / f"id-{boxrec_id}-2.html"
        else:
            output_file = scrape_output_dir / f"id-{boxrec_id}.html"

        try:
            output_file.write_text(response_text, encoding="utf-8")
            log_msg(
                f"Saved {count+1}/{len(collected_profiles)} html files... page#: {boxrec_id} // output file: {output_file}"
            )
        except:
            log_msg(
                f"id: {boxrec_id}, did not have the proper encoding and was skipped"
            )
            continue

    elapsed = time.perf_counter() - start
    log_msg(
        f"\n[ProfilesExport]: Profiles Scraper has saved the available ratings page html bodies: {elapsed} seconds!\n"
    )


async def save_df_to_csv(config: ScrapeConfig, df: pd.DataFrame) -> None:
    """Saves given pd.DataFrame to a .csv file."""

    start = time.perf_counter()

    filename = f"{config.csv_folder}{config.profiles_csv_prefix}-{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.csv"
    df.to_csv(filename, encoding="utf-8", index=False)

    elapsed = time.perf_counter() - start
    log_msg(
        f"\n[DataStorage]: A dataframe was exported to a .csv file: {elapsed} seconds!\n"
    )


async def save_profiles_to_csv(config: ScrapeConfig, profiles: list[dict]) -> None:
    """Saves given list of profiles dictionaries to a .csv file."""

    filename = f"{config.csv_folder}{config.profiles_csv_prefix}-{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.csv"

    keys = [i for s in [d.keys() for d in profiles] for i in s]

    with open(filename, "a", encoding="utf-8") as output_file:
        dict_writer = csv.DictWriter(
            output_file, restval="-", fieldnames=keys, delimiter=","
        )
        dict_writer.writeheader()
        dict_writer.writerows(profiles)


async def save_all_profiles_to_csv(
    config: ScrapeConfig, all_profiles: list[BoxerProfile]
) -> None:
    """Saves a given list of BoxerProfile instances to a .csv file."""

    start = time.perf_counter()

    filename = f"{config.csv_folder}{config.profiles_csv_prefix}-{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.csv"
    try:
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for profile in all_profiles:
                writer.writerow(
                    [
                        profile.br_boxer_id,
                        profile.boxer_name,
                        profile.weightclass,
                        profile.boxer_win_tot,
                        profile.boxer_loss_tot,
                        profile.boxer_draw_tot,
                        profile.boxer_bouts_tot,
                        profile.boxer_rounds_tot,
                        profile.boxer_ko_win_tot,
                        profile.boxer_ko_loss_tot,
                        profile.boxer_kos_pct,
                        profile.boxer_wins_pct,
                        profile.boxer_alias_name,
                        profile.boxer_birth_name,
                        profile.boxer_career_span,
                        profile.boxer_debut_date,
                        profile.career_start_year,
                        profile.career_end_year,
                        profile.boxer_age,
                        profile.boxer_nation,
                        profile.boxer_stance,
                        profile.boxer_height,
                        profile.boxer_height_ft,
                        profile.boxer_height_cm,
                        profile.boxer_reach,
                        profile.boxer_reach_in,
                        profile.boxer_reach_cm,
                        profile.boxer_residence,
                        profile.boxer_birthplace,
                        profile.boxer_titles_held,
                    ]
                )
    except BaseException as e:
        print("BaseException:", filename)
    else:
        elapsed = time.perf_counter() - start
        log_msg(
            f"\n[DataStorage]: List of BoxerProfile(s) was exported to a .csv file: {elapsed} seconds!\n"
        )


async def save_all_fightsnips_to_csv(
    config: ScrapeConfig, all_fightsnips: list[list[FightSnippet]]
) -> None:
    """Saves a given list of boxers profiles' FightSnippet instances to a .csv file."""

    start = time.perf_counter()

    filename = f"{config.csv_folder}{config.fightsnips_csv_prefix}-{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.csv"
    try:
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for fightsnips in all_fightsnips:
                for snip in fightsnips:
                    writer.writerow(
                        [
                            snip.fsnip_event_fight_id,
                            snip.fsnip_fight_id,
                            snip.fsnip_fight_date,
                            snip.br_boxer_id,
                            snip.boxer_name,
                            snip.fsnip_boxer_weighin_weight,
                            snip.fsnip_fight_result,
                            snip.fsnip_fight_result_type,
                            snip.fsnip_br_opp_id,
                            snip.fsnip_opp_name,
                            snip.fsnip_opp_weighin_weight,
                            snip.fsnip_fight_rounds_completed,
                            snip.fsnip_fight_rounds_scheduled
                            # snip.fsnip_fight_stoppage_round_time
                        ]
                    )
    except BaseException as e:
        print("BaseException:", filename)
    else:
        elapsed = time.perf_counter() - start
        log_msg(
            f"\n[DataStorage]: List of FightSnippet(s) was exported to a .csv file: {elapsed} seconds!\n"
        )
