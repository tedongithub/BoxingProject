import logging
from datetime import datetime


now = datetime.now()
date = now.strftime("%y%m%d")

logging.basicConfig(
    # filename=f"./boxrec/boxer_profiles/logs/{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}_scraper.log",
    filename=f"./logs/{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}_scraper.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(message)s",
    datefmt="'%Y_%m_%d-%I_%M_%S_%p'",
)


def log_msg(msg: str) -> None:
    """Logs and prints a given message at the info level."""
    
    logging.info(msg)
    print(msg)