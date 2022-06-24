import pyodbc
import time

import pandas as pd
import sqlalchemy
from contextlib import asynccontextmanager
import logging

from config import read_config, ScrapeConfig
from utils import build_next_url
from log import log_msg


config = read_config("./fights-config.json")

DRIVER = (
    "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
    + config.server
    + ";DATABASE="
    + config.database
    + ";TRUSTED_CONNECTION=yes"
)


@asynccontextmanager
async def open_db(driver: str) -> None:
    """Asynchronous context manager that ensures database connection is closed after executing a SQL command."""

    cnxn = pyodbc.connect(driver)
    try:
        cursor = cnxn.cursor()
        yield cursor
    except pyodbc.OperationalError as error:
        logging.error(error)
    finally:
        logging.info("Closing Database Connection!")
        cnxn.commit()
        cnxn.close()


def query_entire_fights_table():
    """A SQL query that selects all from the ratings table."""

    logging.basicConfig(level=logging.INFO)
    with open_db(DRIVER) as cursor:
        cursor.execute("SELECT * FROM [BoxingTestDB].[dbo].[fights]")
        logging.info(cursor.fetchall())


async def make_new_fights_table() -> None:
    """Executes SQL statement that creates a new fights table if not exists."""

    logging.basicConfig(level=logging.INFO)
    async with open_db(DRIVER) as cursor:

        create_table_command = """  
			IF OBJECT_ID(N'[dbo].[fights]', N'U') IS NULL 
			BEGIN   
				CREATE TABLE [dbo].[fights] (
					[FightsID]                  INT IDENTITY (1,1)  NOT NULL,
					[event_fight_id]            VARCHAR(255)        NOT NULL,
					[event_id]                  INT                 NOT NULL,
					[fight_id]                  INT                 NOT NULL,       
					[fight_date]                DATE                NOT NULL,
					[fight_winner_id]           INT                 NOT NULL,
					[fight_winner_name]         VARCHAR(255)        NOT NULL,
					[fight_loser_id]            INT                 NOT NULL, 
					[fight_loser_name]          VARCHAR(255)        NOT NULL,   
					[fight_weightclass]         VARCHAR(255)        NULL,
					[title_fight_flag]          VARCHAR(255)        NULL,    
					[fight_venue]               VARCHAR(255)        NULL,
					[fight_rounds_completed]    INT                 NULL,    
					[fight_rounds_scheduled]    INT                 NULL,  
					[boxer_name_l]              VARCHAR(255)        NOT NULL,
					[boxer_name_r]              VARCHAR(255)        NOT NULL,    
					[boxer_boxrec_id_l]         INT                 NOT NULL,    
					[boxer_boxrec_id_r]         INT                 NOT NULL,   
					[boxer_wins_before_l]       INT                 NULL,  
					[boxer_losses_before_l]     INT                 NULL,   
					[boxer_draws_before_l]      INT                 NULL,   
					[boxer_kos_before_l]        INT                 NULL,  
					[boxer_wins_before_r]       INT                 NULL,   
					[boxer_losses_before_r]     INT                 NULL,    
					[boxer_draws_before_r]      INT                 NULL,    
					[boxer_kos_before_r]        INT                 NULL,   
					[boxer_age_l]               INT                 NULL,    
					[boxer_age_r]               INT                 NULL,  
					[boxer_stance_l]            VARCHAR(255)        NULL,
					[boxer_stance_r]            VARCHAR(255)        NULL,
					[boxer_height_l]            VARCHAR(255)        NULL, 
					[boxer_height_r]            VARCHAR(255)        NULL, 
					[boxers_height_ft_l]        FLOAT               NULL, 
					[boxers_height_ft_r]        FLOAT               NULL, 
					[boxer_height_cm_l]         INT                 NULL,
					[boxer_height_cm_r]         INT                 NULL,
					[boxer_reach_l]             VARCHAR(255)        NULL, 
					[boxer_reach_r]             VARCHAR(255)        NULL,
					[boxers_reach_in_l]         FLOAT               NULL, 
					[boxers_reach_in_r]         FLOAT               NULL, 
					[boxer_reach_cm_l]          INT                 NULL, 
					[boxer_reach_cm_r]          INT                 NULL, 
					[boxer_points_after_l]      FLOAT               NULL, 
					[boxer_points_after_r]      FLOAT               NULL,   
					[fight_referee_name]        VARCHAR(255)        NULL, 
					[fight_judge1_name]         VARCHAR(255)        NULL,    
					[fight_judge1_score_l]      INT                 NULL,    
					[fight_judge1_score_r]      INT                 NULL,  
					[fight_judge2_name]         VARCHAR(255)        NULL,  
					[fight_judge2_score_l]      INT                 NULL,    
					[fight_judge2_score_r]      INT                 NULL, 
					[fight_judge3_name]         VARCHAR(255)        NULL, 
					[fight_judge3_score_l]      INT                 NULL,
					[fight_judge3_score_r]      INT                 NULL, 
					
					[upload_date]               DATETIME            NOT NULL DEFAULT GETDATE()
				);
			END;
		"""

        cursor.execute(create_table_command)


async def insert_fights_df_to_db(df: pd.DataFrame) -> None:
    """SQL insert statement that appends given dataframe to the fights table."""

    start = time.perf_counter()

    logging.basicConfig(level=logging.INFO)
    async with open_db(DRIVER) as cursor:
        insert_df_command = """ 
			INSERT INTO dbo.fights (
				event_fight_id, event_id, fight_id, fight_date, fight_winner_id, fight_winner_name, fight_loser_id, 
				fight_loser_name, fight_weightclass, title_fight_flag, fight_venue, fight_rounds_completed, 
				fight_rounds_scheduled, boxer_name_l, boxer_name_r, boxer_boxrec_id_l, boxer_boxrec_id_r, boxer_wins_before_l, 
				boxer_losses_before_l, boxer_draws_before_l, boxer_kos_before_l, boxer_wins_before_r, boxer_losses_before_r,
				boxer_draws_before_r, boxer_kos_before_r, boxer_age_l, boxer_age_r, boxer_stance_l, boxer_stance_r, boxer_height_l,
				boxer_height_r, boxers_height_ft_l, boxers_height_ft_r, boxer_height_cm_l, boxer_height_cm_r, boxer_reach_l, 
				boxer_reach_r, boxers_reach_in_l, boxers_reach_in_r, boxer_reach_cm_l, boxer_reach_cm_r, boxer_points_after_l, 
				boxer_points_after_r, fight_referee_name, fight_judge1_name, fight_judge1_score_l, fight_judge1_score_r, 
				fight_judge2_name, fight_judge2_score_l, fight_judge2_score_r, fight_judge3_name, fight_judge3_score_l, 
				fight_judge3_score_r
						) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		"""
        for index, fight in df.iterrows():
            cursor.execute(
                insert_df_command,
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
            )

    elapsed = time.perf_counter() - start
    log_msg(
        f"\n[DataStorage]: The fights dataframe was exported to SQLServer Database: {elapsed} seconds!\n"
    )


class SQLServer:
    def __init__(self, driver):
        self.driver = driver

    def __enter__(self):
        self.cnxn = pyodbc.connect(self.driver)
        return self.cnxn.cursor()

    def __exit__(self, type, value, traceback):
        self.cnxn.commit()
        self.cnxn.close()

    def query(self):
        logging.basicConfig(level=logging.INFO)
        # with open_db(self.driver) as cursor:
        self.cnxn.cursor.execute("SELECT * FROM [BoxingTestDB].[dbo].[profiles]")
        return logging.info(self.cnxn.cursor.fetchall())


def get_fight_ids_in_fights_table():
    """SQL query that returns a list of all 'event_fight_id' in the fights table."""

    logging.basicConfig(level=logging.INFO)
    with SQLServer(DRIVER) as cursor:
        cursor.execute("SELECT [event_fight_id] FROM [BoxingTestDB].[dbo].[fights]")
        return [x[0] for x in cursor.fetchall()]


def get_fight_ids_in_fightsnips_table() -> list:
    """SQL query that returns a list of all 'event_fight_id' from the fightsnips table."""

    logging.basicConfig(level=logging.INFO)
    with SQLServer(DRIVER) as cursor:
        cursor.execute(
            "SELECT DISTINCT([fsnip_event_fight_id]) FROM [BoxingTestDB].[dbo].[fightsnips]"
        )
        return [x[0] for x in cursor.fetchall()]


def get_fight_urls_not_in_db(config: ScrapeConfig) -> list:
    """SQL query that returns a list of 'fight URLs' found in the careers(fightsnips) table, not yet in the fights table."""

    curr_careers_fights_urls = get_fight_ids_in_fightsnips_table()
    curr_fights_fights_urls = get_fight_ids_in_fights_table()
    return [
        build_next_url(config, x)
        for x in curr_careers_fights_urls
        if x not in curr_fights_fights_urls
    ]
