import time
import pyodbc
import pandas as pd
import sqlalchemy
from contextlib import asynccontextmanager
import logging

from log import log_msg
from config import read_config, ScrapeConfig


config = read_config("./config.json")
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


def query_entire_ratings_table():
    """A query that selects all from the ratings table."""

    logging.basicConfig(level=logging.INFO)
    with open_db(DRIVER) as cursor:
        cursor.execute("SELECT * FROM [BoxingTestDB].[dbo].[ratings]")
        logging.info(cursor.fetchall())


async def make_new_ratings_table() -> None:
    """SQL statement that creates a new ratings table if not exists."""

    logging.basicConfig(level=logging.INFO)
    async with open_db(DRIVER) as cursor:

        create_table_command = """ 	
			IF OBJECT_ID(N'[dbo].[ratings]', N'U') IS NULL 
			BEGIN   
				CREATE TABLE [dbo].[ratings] (
					[br_rating] 			INT 			NOT NULL,
					[br_boxer_id]			INT 			NOT NULL,
					[boxer_name] 			VARCHAR(255) 	NOT NULL,
					[br_points] 			FLOAT 			NOT NULL,
					[weightclass]			VARCHAR(255) 	NOT NULL,
					[boxer_wld]				VARCHAR(255) 	NOT NULL,
					[bout_tot]				INT 			NOT NULL,
					[win_tot]				INT 			NOT NULL,
					[loss_tot]				INT 			NOT NULL,
					[draw_tot]				INT 			NOT NULL,
					[career_date_span]		VARCHAR(255) 	NOT NULL,
					[career_start_year]		INT 			NOT NULL,
					[career_end_year]		INT 			NOT NULL,
					[upload_date] 			DATETIME 		NOT NULL 	DEFAULT 	GETDATE()
				);
			END;
		"""

        cursor.execute(create_table_command)


async def insert_to_db_table(df: pd.DataFrame) -> None:
    """SQL insert statement that appends given dataframe to the ratings table."""

    start = time.perf_counter()

    logging.basicConfig(level=logging.INFO)
    async with open_db(DRIVER) as cursor:
        insert_df_command = """ 
			INSERT INTO dbo.ratings (
				br_rating,
				br_boxer_id,
				boxer_name, 
				br_points, 
				weightclass,
				boxer_wld, 
				bout_tot,
				win_tot,
				loss_tot,
				draw_tot,
				career_date_span,
				career_start_year,
				career_end_year
			) values(?,?,?,?,?,?,?,?,?,?,?,?,?)
		"""
        for index, row in df.iterrows():
            cursor.execute(
                insert_df_command,
                row.br_rating,
                row.br_boxer_id,
                row.boxer_name,
                row.br_points,
                row.weightclass,
                row.boxer_wld,
                row.bout_tot,
                row.win_tot,
                row.loss_tot,
                row.draw_tot,
                row.career_date_span,
                row.career_start_year,
                row.career_end_year,
            )

    elapsed = time.perf_counter() - start
    log_msg(
        f"\n[DataStorage]: The ratings dataframe was exported to SQLServer Database: {elapsed} seconds!\n"
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
        self.cnxn.cursor.execute("SELECT * FROM [BoxingTestDB].[dbo].[ratings]")
        return logging.info(self.cnxn.cursor.fetchall())
