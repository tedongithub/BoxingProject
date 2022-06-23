import unicodedata
import time 

import pandas as pd
import warnings 
from bs4 import BeautifulSoup as bs

from profile import BoxerProfile
from fightsnippet import FightSnippet
from log import log_msg


async def create_soup(response_text: str) -> bs:
	return bs(response_text, 'html.parser')

def clean_str(text: str) -> str:
	return unicodedata.normalize('NFKC', text).replace('\r','').replace('\n','').replace('*','').strip()

def rekey(inp_dict, keys_replace):
	return {keys_replace.get(k, k): v for k, v in inp_dict.items()}

def convert_to_float(frac) -> float:
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
	try:
		feet = float(clean_str(feet_str.split('′')[0]))*12
		inches = convert_to_float(clean_str(feet_str.split('′')[1].replace("′′","")))
		inches_float = (feet + inches) / 12
		return  float('%.2f' %inches_float)
	except:
		try:
			inches = convert_to_float(clean_str(feet_str.replace("′′","")))
			return  float('%.2f' %inches)
		except:
			return None
		

def clean_profiles_df(df: pd.DataFrame) -> pd.DataFrame:

	# df.dropna(subset = ["boxrec_rating"], inplace=True)
	# df.drop_duplicates(subset=['boxer_name', 'boxer_href'], keep='first', inplace=True)
	# df['boxrec_rating'] = df['boxrec_rating'].astype('int')
	# df.sort_values(by=['boxrec_rating'], inplace=True, ignore_index=True)

	return df.astype(
						{
							"weightclass": "category",
							"boxer_debut_date": "datetime64",
					}
				)


async def profiles_to_df(all_profiles: list[BoxerProfile]) -> pd.DataFrame:

	start = time.perf_counter()

	# Ignore FutureWarning
	warnings.filterwarnings('ignore')
	# Avoid returning a truncated DataFrame
	pd.set_option('display.max_colwidth', -1)
	pd.set_option('display.max_columns', None)
	pd.set_option('expand_frame_repr', False)
	pd.set_option('display.max_rows', None)

	elapsed = time.perf_counter() - start
	log_msg(f"\n[DataTransforms]: The profiles pages data was transformed with pd.DataFrame: {elapsed} seconds!\n")	
	return clean_profiles_df(
				pd.DataFrame(
					all_profiles
				)
			)


def clean_fightsnips_df(df: pd.DataFrame) -> pd.DataFrame:

	# df.dropna(subset = ["boxrec_rating"], inplace=True)
	# df.drop_duplicates(subset=['boxer_name', 'boxer_href'], keep='first', inplace=True)
	# df['boxrec_rating'] = df['boxrec_rating'].astype('int')
	# df.sort_values(by=['boxrec_rating'], inplace=True, ignore_index=True)

	return df.astype(
						{
							"fsnip_fight_date": "datetime64",
							# "fsnip_boxer_weighin_weight": "float64",
							# "fsnip_opp_weighin_weight": "float64"
					}
				)


async def fightsnips_to_df(all_careers: list[list[FightSnippet]]) -> pd.DataFrame:

	start = time.perf_counter()

	# Ignore FutureWarning
	warnings.filterwarnings('ignore')
	# Avoid returning a truncated DataFrame
	pd.set_option('display.max_colwidth', -1)
	pd.set_option('display.max_columns', None)
	pd.set_option('expand_frame_repr', False)
	pd.set_option('display.max_rows', None)

	elapsed = time.perf_counter() - start
	log_msg(f"\n[DataTransforms]: The profiles' fightsnips data was transformed with pd.DataFrame: {elapsed} seconds!\n")	
	return clean_fightsnips_df(
				pd.DataFrame(
					[x for y in all_careers for x in y]
				)
			)

