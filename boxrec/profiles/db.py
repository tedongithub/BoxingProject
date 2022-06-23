import pyodbc
import time 

import pandas as pd  
import sqlalchemy
from contextlib import asynccontextmanager
import logging

from config import read_config, ScrapeConfig
from log import log_msg


config = read_config('./profile-config.json')
DRIVER = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+config.server+';DATABASE='+config.database+';TRUSTED_CONNECTION=yes'


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

def query_entire_profiles_table():
	"""A SQL query that selects all from the ratings table."""

	logging.basicConfig(level=logging.INFO)
	with open_db(DRIVER) as cursor:
		cursor.execute("SELECT * FROM [BoxingTestDB].[dbo].[profiles]")
		logging.info(cursor.fetchall())

async def new_profiles_table() -> None:
	"""Executes SQL statement that creates a new profiles table if not exists."""

	logging.basicConfig(level=logging.INFO)
	async with open_db(DRIVER) as cursor:

		create_table_command = """ 	IF OBJECT_ID(N'[dbo].[profiles]', N'U') IS NULL 
									BEGIN   
										CREATE TABLE [dbo].[profiles] (
											[br_boxer_id]			VARCHAR(255) 			NOT NULL,
											[boxer_name]			VARCHAR(255) 			NOT NULL,
											[weightclass]			VARCHAR(255) 			NOT NULL,
											[boxer_win_tot]			INT 					NOT NULL,
											[boxer_loss_tot]		INT 					NOT NULL,	
											[boxer_draw_tot]		INT 					NOT NULL,
											[boxer_bouts_tot] 		INT 					NOT NULL,
											[boxer_rounds_tot]  	INT 					NOT NULL,
											[boxer_ko_win_tot]  	INT 					NOT NULL,	
											[boxer_ko_loss_tot] 	INT 					NOT NULL,		
											[boxer_kos_pct] 		FLOAT 					NOT NULL,
											[boxer_wins_pct] 		FLOAT 					NOT NULL,
											[boxer_alias_name] 		VARCHAR(255) 			NOT NULL,
											[boxer_birth_name] 		VARCHAR(255) 			NOT NULL,
											[boxer_career_span] 	VARCHAR(255) 			NOT NULL,
											[boxer_debut_date] 		DATE 					NOT NULL,
											[career_start_year] 	INT 					NOT NULL,	
											[career_end_year] 		INT 					NOT NULL,	
											[boxer_age] 			INT 					NOT NULL,	
											[boxer_nation] 			VARCHAR(255) 			NOT NULL,
											[boxer_stance] 			VARCHAR(255) 			NOT NULL,
											[boxer_height] 			VARCHAR(255) 			NOT NULL,
											[boxer_height_ft] 		FLOAT 					NOT NULL,
											[boxer_height_cm] 		INT 					NOT NULL,	
											[boxer_reach] 			VARCHAR(255) 			NOT NULL,
											[boxer_reach_in] 		FLOAT 					NOT NULL,
											[boxer_reach_cm] 		INT 					NOT NULL,	
											[boxer_residence] 		VARCHAR(255) 			NOT NULL,
											[boxer_birthplace] 		VARCHAR(255) 			NOT NULL,

					                        [upload_date] 			DATETIME 				NOT NULL 	DEFAULT GETDATE()
										);
									END;
								"""

		cursor.execute(create_table_command) 

async def insert_profiles_to_db(df: pd.DataFrame) -> None:
	"""SQL insert statement that appends given dataframe to the profiles table."""

	start = time.perf_counter()

	logging.basicConfig(level=logging.INFO)
	async with open_db(DRIVER) as cursor:
		insert_df_command = """ 
			INSERT INTO dbo.profiles (
							br_boxer_id, boxer_name, weightclass, boxer_win_tot, boxer_loss_tot, boxer_draw_tot, boxer_bouts_tot, 
							boxer_rounds_tot, boxer_ko_win_tot,	boxer_ko_loss_tot,boxer_kos_pct, boxer_wins_pct,
							boxer_alias_name, boxer_birth_name, boxer_career_span, boxer_debut_date, career_start_year,
							career_end_year, boxer_age, boxer_nation, boxer_stance, boxer_height, boxer_height_ft,
							boxer_height_cm, boxer_reach, boxer_reach_in, boxer_reach_cm, boxer_residence, boxer_birthplace
						) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) 
		"""
		for index, row in df.iterrows():
			cursor.execute(insert_df_command, 
								row.br_boxer_id,
								row.boxer_name,
								row.weightclass,
								row.boxer_win_tot,
								row.boxer_loss_tot,
								row.boxer_draw_tot,
								row.boxer_bouts_tot,
								row.boxer_rounds_tot,
								row.boxer_ko_win_tot,	
								row.boxer_ko_loss_tot,	
								row.boxer_kos_pct,
								row.boxer_wins_pct,
								row.boxer_alias_name,
								row.boxer_birth_name,
								row.boxer_career_span,
								row.boxer_debut_date,
								row.career_start_year,
								row.career_end_year,
								row.boxer_age,
								row.boxer_nation,
								row.boxer_stance,
								row.boxer_height,
								row.boxer_height_ft,
								row.boxer_height_cm,
								row.boxer_reach,
								row.boxer_reach_in,
								row.boxer_reach_cm,
								row.boxer_residence,
								row.boxer_birthplace
			) 

	elapsed = time.perf_counter() - start
	log_msg(f"\n[DataStorage]: The profiles dataframe was exported to SQLServer Database: {elapsed} seconds!\n")


async def new_fight_snips_table() -> None:
	"""Executes SQL statement that creates a new fightsnips table if not exists."""

	logging.basicConfig(level=logging.INFO)
	async with open_db(DRIVER) as cursor:

		create_table_command = """ 	IF OBJECT_ID(N'[dbo].[fightsnips]', N'U') IS NULL 
									BEGIN   
										CREATE TABLE [dbo].[fightsnips] (

											[fsnip_event_fight_id]				VARCHAR(255) 		NOT NULL,
											[fsnip_fight_id]					INT 				NOT NULL,
											[fsnip_fight_date]					DATE 				NOT NULL,
											[br_boxer_id]						INT 				NOT NULL,
											[boxer_name]						VARCHAR(255) 		NOT NULL,
											[fsnip_boxer_weighin_weight]		FLOAT				NOT NULL,	
											[fsnip_fight_result]				VARCHAR(255) 		NOT NULL,
											[fsnip_fight_result_type]			VARCHAR(255) 		NOT NULL,
											[fsnip_br_opp_id]					INT 				NOT NULL,
											[fsnip_opp_name]					VARCHAR(255) 		NOT NULL,	
											[fsnip_opp_weighin_weight]			FLOAT				NOT NULL,	
											[fsnip_fight_rounds_completed]		INT 				NOT NULL,
											[fsnip_fight_rounds_scheduled]		INT 				NOT NULL,

					                        [upload_date] 						DATETIME 			NOT NULL 	DEFAULT GETDATE()
										);
									END;
								"""

		cursor.execute(create_table_command)


async def insert_fightsnips_to_db(df: pd.DataFrame) -> None:
	"""SQL insert statement that appends given dataframe to the profiles table."""

	start = time.perf_counter()

	logging.basicConfig(level=logging.INFO)
	async with open_db(DRIVER) as cursor:
		insert_df_command = """ 
			INSERT INTO dbo.fightsnips (
							fsnip_event_fight_id, fsnip_fight_id, fsnip_fight_date, br_boxer_id, boxer_name,
							fsnip_boxer_weighin_weight, fsnip_fight_result, fsnip_fight_result_type, fsnip_br_opp_id,
							fsnip_opp_name, fsnip_opp_weighin_weight, fsnip_fight_rounds_completed, fsnip_fight_rounds_scheduled
						) values(?,?,?,?,?,?,?,?,?,?,?,?,?)
		"""
		for index, row in df.iterrows():
			cursor.execute(insert_df_command, 
								row.fsnip_event_fight_id,
								row.fsnip_fight_id,
								row.fsnip_fight_date,
								row.br_boxer_id,
								row.boxer_name,
								row.fsnip_boxer_weighin_weight,
								row.fsnip_fight_result,
								row.fsnip_fight_result_type,
								row.fsnip_br_opp_id,
								row.fsnip_opp_name,
								row.fsnip_opp_weighin_weight,
								row.fsnip_fight_rounds_completed,
								row.fsnip_fight_rounds_scheduled
								# row.fsnip_fight_stoppage_round_time
						) 

	elapsed = time.perf_counter() - start
	log_msg(f"\n[DataStorage]: The fightsnips dataframe was exported to SQLServer Database: {elapsed} seconds!\n")

###########################################################################################################################

class SQLServer():

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
		self.cnxn.cursor.execute("SELECT * FROM [BoxingTestDB].[dbo].[profiles]")
		return logging.info(self.cnxn.cursor.fetchall())

	# def sql_query(self, sql_statement: str) -> list:
	# 	logging.basicConfig(level=logging.INFO)
	# 	self.cnxn.cursor.execute(sql_statement)
	# 	return logging.info(self.cnxn.cursor.fetchall())


def get_profile_urls_from_ratings(config: ScrapeConfig):
	"""A query that selects [br_boxer_id] from the [ratings] table."""

	with SQLServer(DRIVER) as cursor:
		cursor.execute("SELECT [br_boxer_id] FROM [BoxingTestDB].[dbo].[ratings]")
		a = cursor.fetchall()

	return [config.root_url+str(i[0]) for i in a]


def query_saved_boxer_ratings_100(config: ScrapeConfig):
	with SQLServer(DRIVER) as cursor:
		cursor.execute("SELECT TOP (100) [br_boxer_id] FROM [BoxingTestDB].[dbo].[ratings]")
		a = cursor.fetchall()

	return [i[0] for i in a]

def query_saved_boxer_careers(config: ScrapeConfig):
	with SQLServer(DRIVER) as cursor:
		cursor.execute("SELECT DISTINCT([boxer_boxrec_id]) FROM [BoxingTestDB].[dbo].[fightsnips]")
		a = cursor.fetchall()

	return [i[0] for i in a]

def get_profiles_wins_100() -> list[str]:
	"""Sql query that returns CareerFights urls Not yet saved in the archive directory."""

	sql_statement = """	select 		br_boxer_id, boxer_bouts_tot
	    				from 		profiles 
	    				where 		boxer_bouts_tot >= 100
	    				order by 	boxer_bouts_tot desc
					"""

	with SQLServer(DRIVER) as cursor:
		cursor.execute(sql_statement)
		a = cursor.fetchall()

		return dict([(i[0], i[1]) for i in a])

	 
