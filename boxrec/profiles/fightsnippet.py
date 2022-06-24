from datetime import date
import inspect

from dataclasses import dataclass, field


@dataclass
class FightSnippet:
    """Dataclass that represents all data extracted from a profile fight snippet."""

    fsnip_event_fight_id: str = field(default="")
    fsnip_fight_id: int = field(default=0)
    fsnip_fight_date: date = field(default=None)
    br_boxer_id: int = field(default=0)
    boxer_name: str = field(default="")
    fsnip_boxer_weighin_weight: float = field(default=0.0)
    fsnip_fight_result: str = field(default="")
    fsnip_fight_result_type: str = field(default="")
    fsnip_br_opp_id: int = field(default=0)
    fsnip_opp_name: str = field(default="")
    fsnip_opp_weighin_weight: float = field(default=0.0)
    fsnip_fight_rounds_completed: int = field(default=0)
    fsnip_fight_rounds_scheduled: int = field(default=0)
    # snip_fight_stoppage_round_time: str = field(default="")


def from_dict_to_dataclass(cls, data):
    """Helper function that converts a dictionary into a FightSnippet instance."""

    return cls(
        **{
            key: (data[key] if val.default == val.empty else data.get(key, val.default))
            for key, val in inspect.signature(FightSnippet).parameters.items()
        }
    )
