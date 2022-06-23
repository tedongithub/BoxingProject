import unicodedata
import time

import pandas as pd
import warnings
from bs4 import BeautifulSoup as bs

from fight import Fight
from log import log_msg


async def create_soup(response_text: str) -> bs:
    """Returns response.text as BeautifulSoup object."""

    return bs(response_text, 'html.parser')


def clean_str(text: str) -> str:
    """Cleans a given string of special chars, unicodedata, new lines."""

    return unicodedata.normalize('NFKC', text).replace('\r', '').replace('\n', '').replace('*', '').strip()


async def curr_event_fight_id(file: str) -> str:
    """Returns an 'event_fight_id' from a file's name."""

    return clean_str(str(file).split('id-')[1].split('.')[0].replace('-', '/'))


def convert_to_float(frac) -> float:
    """Converts given fraction into type float."""

    if bool(frac.isnumeric()) == False and '⁄' in frac:
        try:
            num1 = frac.split('⁄')[0]
            denom = frac.split('⁄')[-1]
            num = num1[-1]
            leading = num1[:-1]
            whole = float(leading)
            frac = float(num) / float(denom)
            return whole + frac
        except:
            pass
        return float(frac)
    elif bool(frac.isnumeric()) == True:
        return float(frac)
    else:
        return frac


def convert_to_feet_float(feet_str: str) -> float:
    """Converts given height in feet, to type float."""

    try:
        feet = float(clean_str(feet_str.split('′')[0]))*12
        inches = convert_to_float(
            clean_str(feet_str.split('′')[1].replace("′′", "")))
        inches_float = (feet + inches) / 12
        return float('%.2f' % inches_float)
    except:
        try:
            inches = convert_to_float(clean_str(feet_str.replace("′′", "")))
            return float('%.2f' % inches)
        except:
            return None


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Performs transformations on given fights dataframe to clean the data."""

    # df = df.fillna(0)
    # df.dropna(subset = ["boxrec_rating"], inplace=True)
    # df.drop_duplicates(subset=['boxer_name', 'boxer_href'], keep='first', inplace=True)
    # df['boxrec_rating'] = df['boxrec_rating'].astype('int64')
    # df.sort_values(by=['boxrec_rating'], inplace=True, ignore_index=True)

    df = df.drop(['fight_titles_avail'], axis=1)
    df = df.astype(object).where(pd.notnull(df), None)
    df['boxers_height_ft_l'] = df['boxers_height_ft_l'].fillna(
        0)  # Convert nan/null to 0
    df['boxers_height_ft_r'] = df['boxers_height_ft_r'].fillna(
        0)  # Convert nan/null to 0
    df['boxers_reach_in_l'] = df['boxers_reach_in_l'].fillna(
        0)  # Convert nan/null to 0
    df['boxers_reach_in_r'] = df['boxers_reach_in_r'].fillna(
        0)  # Convert nan/null to 0
    df['boxer_points_after_l'] = df['boxer_points_after_l'].fillna(
        0)  # Convert nan/null to 0
    df['boxer_points_after_r'] = df['boxer_points_after_r'].fillna(
        0)  # Convert nan/null to 0

    return df.astype(
        {
            "event_id": "int64",
            "fight_id": "int64",
            "fight_date": "datetime64",
            "fight_winner_id": "int64",
            "fight_loser_id": "int64",
            "fight_weightclass": "category",
            "fight_rounds_completed": "int64",
            "fight_rounds_scheduled": "int64",

            "boxer_boxrec_id_l": "int64",
            "boxer_boxrec_id_r": "int64",
            "boxer_wins_before_l": "int64",
            "boxer_losses_before_l": "int64",
            "boxer_draws_before_l": "int64",
            "boxer_kos_before_l": "int64",
            "boxer_wins_before_r": "int64",
            "boxer_losses_before_r": "int64",
            "boxer_draws_before_r": "int64",
            "boxer_kos_before_r": "int64",
            "boxer_age_l": "int64",
            "boxer_age_r": "int64",

            "boxers_height_ft_l": "float64",
            "boxers_height_ft_r": "float64",
            "boxer_height_cm_l": "int64",
            "boxer_height_cm_r": "int64",
            "boxers_reach_in_l": "float64",
            "boxers_reach_in_r": "float64",
            "boxer_reach_cm_l": "int64",
            "boxer_reach_cm_r": "int64",
            "boxer_points_after_l": "float64",
            "boxer_points_after_r": "float64",

            "fight_judge1_score_l": "int64",
            "fight_judge1_score_r": "int64",
            "fight_judge2_score_l": "int64",
            "fight_judge2_score_r": "int64",
            "fight_judge3_score_l": "int64",
            "fight_judge3_score_r": "int64"
        }
    )


async def fights_to_df(all_fights: list[Fight]) -> pd.DataFrame:
    """Returns a clean fights dataframe from given list of Fight instances."""

    start = time.perf_counter()

    # Ignore FutureWarning
    warnings.filterwarnings('ignore')
    # Avoid returning a truncated DataFrame
    pd.set_option('display.max_colwidth', -1)
    pd.set_option('display.max_columns', None)
    pd.set_option('expand_frame_repr', False)
    pd.set_option('display.max_rows', None)

    elapsed = time.perf_counter() - start
    log_msg(f"\n[DataTransforms]: The fights pages data was transformed with pd.DataFrame: {elapsed} seconds!\n")
    return clean_df(
        pd.DataFrame(
            all_fights
        )
    )
