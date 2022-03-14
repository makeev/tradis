from os.path import abspath

import redis
import yaml
from ibkr_web_api import IbApi
from ibkr_web_api.session_storage import RedisStorage

_config = None
_redis_client = None
_ib_instance = None


def get_config(config_path=None):
    global _config

    if not _config:
        if config_path is None:
            raise Exception('config not defined')

        config_path = abspath(config_path)
        _config = yaml.full_load(open(config_path))

    return _config


def get_redis_client(config=None):
    """
    Функция на случай, если захотим заменить это connection pool
    """
    global _redis_client

    if not _redis_client:
        if config is None:
            config = get_config()

        _redis_client = redis.Redis(
            config['redis']["host"],
            config['redis']["port"],
            config['redis']["db"],
            config['redis']["password"],
        )

    return _redis_client


def get_ib_instance(config=None):
    global _ib_instance

    if not _ib_instance:
        if config is None:
            config = get_config()

        storage = RedisStorage(
            session_name=config['username'],
            redis_client=get_redis_client(config),
            secret=config['secret']
        )
        _ib_instance = IbApi(config['username'], config['password'],
                             session_storage=storage, paper=config['paper'],
                             debug=False)

    return _ib_instance
