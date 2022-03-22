import traceback
import asyncio
import aioredis
from functools import wraps


def coro(f):
    """
    Враппер чтобы использовать вместе с @click.command с асинхронным main()
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


def get_async_redis_client(host, port, db, password):
    redis_url = 'redis://{host}:{port}'.format(
        host=host, port=port
    )
    return aioredis.from_url(redis_url, db=db, password=password)


def get_traceback(e):
    lines = traceback.format_exception(type(e), e, e.__traceback__)
    return ''.join(lines)
