import json
from dataclasses import dataclass


@dataclass
class ScrapeConfig:
    """Dataclass that contains cofiguration settings from a configuration file (json)."""

    server: str
    database: str
    fights_store: str
    html_folder: str
    csv_folder: str 
    fights_csv_prefix: str 
    user_agent_list: str
    scrape_output_folder: str
    root_url: str
    base_url: str
    base_href: str
    collected_fights_dir: str
    headers: str


def read_config(config_file: str) -> ScrapeConfig:
    """Reads a json config file and unpacks to create an instance of ScrapeConfig."""
    
    with open(config_file) as file:
        data = json.load(file)
        return ScrapeConfig(**data['config'])




