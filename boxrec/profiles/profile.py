from datetime import date
import inspect

from dataclasses import dataclass, field


@dataclass
class BoxerProfile:
	"""Dataclass that represents all data extracted from a boxer's profile."""

	br_boxer_id: int = field(default=0)				
	boxer_name: str = field(default="")					
	weightclass: str = field(default="")				
	boxer_win_tot: int = field(default=0)
	boxer_loss_tot: int = field(default=0) 		
	boxer_draw_tot: int = field(default=0)
	boxer_bouts_tot: int = field(default=0)		
	boxer_rounds_tot: int = field(default=0)	
	boxer_ko_win_tot: int = field(default=0)		
	boxer_ko_loss_tot: int = field(default=0)		
	boxer_kos_pct: float = field(default=0.0)	

	boxer_wins_pct: float = field(default=0.0)		
	
	boxer_alias_name: str = field(default="")
	boxer_birth_name: str = field(default="")
	boxer_career_span: str = field(default="")
	boxer_debut_date: date = field(default=None)
	
	career_start_year: int = field(default=0)		
	career_end_year: int = field(default=0)			
	
	boxer_age: int = field(default=0)
	boxer_nation: str = field(default="")
	boxer_stance: str = field(default="")

	boxer_height: str = field(default="")
	boxer_height_ft: float = field(default=0.0)		
	boxer_height_cm: int = field(default=0)			

	boxer_reach: str = field(default="")
	boxer_reach_in: float = field(default=0.0)		
	boxer_reach_cm: int = field(default=0)			

	boxer_residence: str = field(default="")
	boxer_birthplace: str = field(default="")

	boxer_titles_held: list[str] = field(default_factory=list)



def from_dict_to_dataclass(cls, data):
	"""Helper function that converts a dictionary into an instance of BoxerProfile.""" 

	return cls(
		**{
			key: (data[key] if val.default == val.empty else data.get(key, val.default))
			for key, val in inspect.signature(BoxerProfile).parameters.items()
		}
	)

