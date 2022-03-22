import asyncio
import json
import time
from datetime import datetime
from functools import partial

import aioredis
import websockets
import click
from termcolor import cprint
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosedOK, ConnectionClosed

from config import get_ib_instance, get_config
from utils import coro, get_async_redis_client, get_traceback


# как часто слать tic
TIC_EVERY_SECONDS = 60

# максимум секунд сколько ждать какой-то ответ,
# если дольше, то считаем что сокет сломался
RECV_TIMEOUT = 15


class IbkrWebsocketClient(WebSocketClientProtocol):
    # присваивается в init
    ib = None
    config = None
    instruments_by_conid = None
    get_redis_func = None

    current_time_seconds = time.time()  # обновляется при каждом recv

    # protected
    _last_data_ts = {}  # когда приходили последние данные по инструментам
    _last_heartbeat_seconds = 0  # когда приходил последний ech+hb
    _last_messages_ts = 0  # time() последнего recv, пока не используется
    _last_tic_seconds = 0  # чтобы слать tic каждые TIC_EVERY_SECONDS
    _redis_client = None  # создается в init

    def init(self, ib, config):
        """
        Потому что до обычного __init__ не дотянуться
        """
        self.ib = ib
        self.config = config
        self.instruments_by_conid = {i["conid"]: i for i in config['instruments']}
        self.get_redis_func = partial(
            get_async_redis_client,
            config['redis']['host'],
            config['redis']['port'],
            config['redis']['db'],
            config['redis']['password'],
        )
        self.init_redis()

    def init_redis(self):
        self._redis_client = self.get_redis_func()

    async def listen_messages(self):
        async for msg in self:
            # питоновская магия
            self._last_messages_ts = time.time()
        cprint('сообщения внезапно закончились', 'red')

    ###
    # do_ команды выполняются в ответ на сообщения из сокета
    ###
    async def do_auth(self):
        # авторизация
        self.ib.load_session()  # @TODO тоже бы асинхронным сделать
        cp = self.ib.session.cookies.get("cp")
        await self.send('{"session": "%s"}' % cp)

    async def do_parse_market_data(self, json_data):
        price = json_data.get('31')
        conid = json_data.get("conid")

        if not price or not conid:
            # иногда приходит просто _updated
            return

        symbol = "{symbol}.{exchange}".format(**self.instruments_by_conid[conid])
        updated = datetime.utcfromtimestamp(json_data["_updated"] / 1000)
        msg = {
            "dt": updated,
            "price": price,
            "conid": conid,
            "symbol": symbol,
        }
        self._last_data_ts[conid] = time.time()
        json_str = json.dumps(msg, indent=None, default=str)
        await self._redis_client.publish(f"{symbol}:TRADES", json_str)

    async def do_heartbeat(self, json_data):
        # @TODO непонятно как считается параметр hb
        hb_time_seconds = int(str(json_data.get('hb'))[:10])
        diff_seconds = self.current_time_seconds - hb_time_seconds
        if diff_seconds > 0:
            cprint("heartbeat запаздывает на %d секунд" % diff_seconds, "red")

        if self.current_time_seconds - self._last_heartbeat_seconds >= 30:
            await self.send('ech+hb')
            self._last_heartbeat_seconds = int(time.time())

    async def do_handle_status(self, json_data):
        args = json_data.get('args', {})
        authenticated = args.get('authenticated')
        if authenticated is not None:
            if authenticated:
                cprint("авторизовались!", "green")
            else:
                cprint("не авторизовались :-(", "red")

        # @TODO тут как-то обрабатывать статусы и слать в телегу

    async def do_handle_system(self, json_data):
        # @TODO
        # системные сообщения
        pass

    async def do_handle_notification(self, json_data):
        # @TODO
        # системные сообщения
        pass

    async def do_handle_bulletin(self, json_data):
        # @TODO
        # системные сообщения
        pass

    async def do_handle_tic(self, json_data):
        # @TODO
        # системные сообщения
        pass

    async def do_pong(self):
        # ответ на ech+hb
        # @TODO что-то делать тут наверное
        pass

    async def recv(self):
        """
        Тут будем ловить сообщения и слать ответ
        чтобы не городить очереди.
        recv неявно дергается в listen_messages
        """
        data = await asyncio.wait_for(super().recv(), RECV_TIMEOUT)
        cprint('recv: %s' % data, "yellow")

        self.current_time_seconds = int(time.time())

        # приходят и строки и байты
        if type(data) == bytes:
            data = data.decode()

        json_data = None
        text_data = data

        try:
            json_data = json.loads(data)
        except json.decoder.JSONDecodeError:
            # значит смотрим text_data
            pass
        except Exception as e:
            cprint("произошла неведомая хуйня:\n %s" % get_traceback(e), "red")

        if json_data:
            topic = json_data.get('topic', '')
            if json_data.get('message') == 'waiting for session':  # authentication
                await self.do_auth()
            elif json_data.get('hb'):  # heartbeat
                await self.do_heartbeat(json_data)
            elif topic.startswith('smd'):  # market data
                await self.do_parse_market_data(json_data)
            elif topic.startswith('smh'):  # history data
                # @TODO
                pass
            elif topic in ["sor", "uor"]:  # live orders
                # @TODO
                pass
            elif topic in ["str", "utr"]:  # trades
                # @TODO
                pass
            elif topic in ["spl", "upl"]:  # profit and loss
                # @TODO
                pass
            elif topic == 'sts':
                await self.do_handle_status(json_data)
            elif topic == 'system':
                await self.do_handle_system(json_data)
            elif topic == 'nt':
                await self.do_handle_notification(json_data)
            elif topic == 'blt':
                await self.do_handle_bulletin(json_data)
            elif topic == 'tic':
                await self.do_handle_tic(json_data)
            else:
                cprint("что-то непонятное: %s" % json_data, "red")
        elif text_data == "ech+hb":
            await self.do_pong()
        else:
            cprint('пришло что-то новое и непонятное: %s' % text_data)

        # проверяем надо ли обновить подписку
        for instrument in self.config['instruments']:
            conid = instrument["conid"]
            last_data_ts = self._last_data_ts.get(conid, 0)
            if time.time() - last_data_ts > 10:
                # данных не было 10 секунд, пробуем подписаться заново
                cmd = f"smd+{conid}+" + '{"fields":["31"]}'
                await self.send(cmd)

        # tic
        if self.current_time_seconds - self._last_tic_seconds >= TIC_EVERY_SECONDS:
            await self.send('tic')
            self._last_tic_seconds = self.current_time_seconds

        return data

    async def send(self, message):
        cprint('send: %s' % message, "yellow")
        return await super().send(message)

    async def force_close(self):
        try:
            await self._redis_client.close()
        except:
            pass

        try:
            await self.close()
        except:
            pass


@click.command()
@coro
@click.argument('config_path', type=click.Path(exists=True))
async def main(config_path):
    config = get_config(config_path)
    ib = get_ib_instance(config)

    ws = await get_ws_client(ib, config)

    counter = 0
    while True:
        try:
            # вообще такого быть не должно
            # но если закроется, то listen_messages не бросит сам исключение
            if ws.closed:
                raise ConnectionClosed(None, None)

            await ws.listen_messages()
        except (ConnectionClosedOK, ConnectionClosed) as e:
            # websocket закрылся
            cprint('websocket закрылся %s' % e, "red")
            ws = await get_ws_client(ib, config)
            await asyncio.sleep(1)
        except aioredis.exceptions.ConnectionError:
            # редис упал, пробуем реконнект
            cprint('redis error', 'red')
            await asyncio.sleep(3)
            ws.init_redis()
        except asyncio.exceptions.TimeoutError:
            # сообщений не было дольше RECV_TIMEOUT секунд, переоткрываем сокет
            cprint('timeout', "red")
            await ws.force_close()  # не бросает исключений
            ws = await get_ws_client(ib, config)
        except Exception as e:
            cprint("неведомый пиздец %s" % get_traceback(e), "red")
            # скорее всего само починится
        finally:
            counter += 1
            cprint('iteration %d' % counter, 'grey')


async def get_ws_client(ib, config):
    ws = await websockets.connect(
        ib.get_websocket_url(),
        create_protocol=IbkrWebsocketClient,
        ping_interval=None,
    )
    # такой вот monkey patching, т.к. не можем передать в __init__
    ws.init(ib, config)

    return ws

asyncio.get_event_loop().run_until_complete(main())
