from os.path import abspath

import redis
import yaml
from ibkr_web_api import IbApi
from ibkr_web_api.session_storage import RedisStorage

config_path = abspath("config_local.yaml")
config = yaml.full_load(open(config_path))

username = config["username"]
password = config["password"]
paper = config["paper"]
secret = config["secret"]
redis_config = config["redis"]
instruments = config['instruments']
dashboard_csv_path = config["dashboard_csv_path"]

_redis_client = None
_ib_instance = None


def get_redis_client():
    """
    Функция на случай, если захотим заменить это connection pool
    """
    global _redis_client

    if not _redis_client:
        _redis_client = redis.Redis(
            redis_config["host"],
            redis_config["port"],
            redis_config["db"],
            redis_config["password"],
        )

    return _redis_client


def get_ib_instance():
    global _ib_instance
    storage = RedisStorage(
        session_name=username,
        redis_client=get_redis_client(),
        secret=secret
    )

    if not _ib_instance:
        _ib_instance = IbApi(username, password, session_storage=storage, paper=paper, debug=False)

    return _ib_instance
