import pandas as pd
import warnings 
import time

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


def clean_df(df: pd.DataFrame) -> None:
	"""Transforms given dataframe to clean, format, and prepare for saving."""

	# Ignore FutureWarning
	warnings.filterwarnings('ignore')
	# Avoid returning a truncated DataFrame
	pd.set_option('display.max_colwidth', -1)
	pd.set_option('display.max_columns', None)
	pd.set_option('expand_frame_repr', False)
	pd.set_option('display.max_rows', None)
	df.dropna(subset = ["br_rating"], inplace=True)
	df.drop_duplicates(subset=['boxer_name', 'br_boxer_id'], keep='first', inplace=True)
	# df['br_rating'] = df['br_rating'].astype('int')
	df.sort_values(by=['br_rating'], inplace=True, ignore_index=True)
	return df.astype(
				{
				'br_rating': 'int64',
				'br_boxer_id': 'int64',
				'boxer_name': 'object',
				'br_points': 'float64',
				'weightclass': 'category',
				'boxer_wld': 'object',
				'career_date_span': 'object',
				}
			)

async def rows_to_df(rows: list[tuple[int, str, float]]) -> pd.DataFrame:
	"""Converts list of tuples into a dataframe and uses the cleaning function to return accurate data."""
	
	start = time.perf_counter()

	df = pd.DataFrame(
		[t for lst in rows for t in lst], 
		columns=[
					'br_rating',
					'br_boxer_id',
					'boxer_name', 
					'br_points', 
					'weightclass',
					'boxer_wld', 
					'bout_tot',
					'win_tot',
					'loss_tot',
					'draw_tot',
					'career_date_span',
					'career_start_year',
					'career_end_year'
			]
		)

	elapsed = time.perf_counter() - start
	log_msg(f"\n[DataTransforms]: The ratings pages data was transformed with pd.DataFrame: {elapsed} seconds!\n")	
	return clean_df(df) 


