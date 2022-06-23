from datetime import date
import inspect

from dataclasses import dataclass, field


@dataclass
class Fight:
    """Dataclass that represents all data extracted from a fight."""

    event_fight_id: str = field(default=None)
    event_id: int = field(default=None)
    fight_id: int = field(default=None)
    fight_date: date = field(default=None)
    fight_winner_id: int = field(default=None)
    fight_winner_name: str = field(default=None)
    fight_loser_id: int = field(default=None)
    fight_loser_name: str = field(default=None)
    fight_weightclass: str = field(default=None)
    title_fight_flag: str = field(default=None)
    fight_venue: str = field(default=None)
    fight_rounds_completed: int = field(default=None)
    fight_rounds_scheduled: int = field(default=None)
    boxer_name_l: str = field(default=None)
    boxer_name_r: str = field(default=None)
    boxer_boxrec_id_l: int = field(default=0)
    boxer_boxrec_id_r: int = field(default=0)
    boxer_wins_before_l: int = field(default=0)
    boxer_losses_before_l: int = field(default=0)
    boxer_draws_before_l: int = field(default=0)
    boxer_kos_before_l: int = field(default=0)
    boxer_wins_before_r: int = field(default=0)
    boxer_losses_before_r: int = field(default=0)
    boxer_draws_before_r: int = field(default=0)
    boxer_kos_before_r: int = field(default=0)
    boxer_age_l: int = field(default=0)
    boxer_age_r: int = field(default=0)
    boxer_stance_l: str = field(default=None)
    boxer_stance_r: str = field(default=None)
    boxer_height_l: str = field(default=None)
    boxer_height_r: str = field(default=None)
    boxers_height_ft_l: float = field(default=0.0)
    boxers_height_ft_r: float = field(default=0.0)
    boxer_height_cm_l: int = field(default=0)
    boxer_height_cm_r: int = field(default=0)
    boxer_reach_l: str = field(default=None)
    boxer_reach_r: str = field(default=None)
    boxers_reach_in_l: float = field(default=0.0)
    boxers_reach_in_r: float = field(default=0.0)
    boxer_reach_cm_l: int = field(default=0)
    boxer_reach_cm_r: int = field(default=0)
    boxer_points_after_l: float = field(default=0.0)
    boxer_points_after_r: float = field(default=0.0)
    fight_referee_name: str = field(default=None)
    fight_judge1_name: str = field(default=None)
    fight_judge1_score_l: int = field(default=0)
    fight_judge1_score_r: int = field(default=0)
    fight_judge2_name: str = field(default=None)
    fight_judge2_score_l: int = field(default=0)
    fight_judge2_score_r: int = field(default=0)
    fight_judge3_name: str = field(default=None)
    fight_judge3_score_l: int = field(default=0)
    fight_judge3_score_r: int = field(default=0)

    fight_titles_avail: list[str] = field(default_factory=list)


def from_dict_to_dataclass(cls, data):
    """Helper function that converts a dictionary into an instance of Fight."""

    return cls(
        **{
            key: (data[key] if val.default == val.empty else data.get(key, val.default))
            for key, val in inspect.signature(Fight).parameters.items()
        }
    )
