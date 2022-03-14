import json
import logging
import multiprocessing as mp
import queue
import sys
import time
from datetime import datetime

import click
import websocket
from termcolor import cprint

from config import get_redis_client, get_config, get_ib_instance

log = logging.getLogger(__name__)


logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def send_message(data, instruments, redis_client):
    if price := data.get("31"):
        conid = data.get("conid")
        instruments_by_conid = {i["conid"]: i for i in instruments}
        symbol = "{symbol}.{exchange}".format(**instruments_by_conid[conid])
        msg = {
            "dt": data.get("updated"),
            "price": price,
            "conid": conid,
            "symbol": symbol,
        }
        # print(json.dumps(msg, indent=None, default=str))
        json_str = json.dumps(msg, indent=None, default=str)
        redis_client.publish(f"{symbol}:TRADES", json_str)


def parse_data(d, instruments, redis_client):
    try:
        j = json.loads(d)
        if "_updated" in j:
            j["updated"] = datetime.utcfromtimestamp(j["_updated"] / 1000)
        if "hb" in j:
            j["dt"] = datetime.utcfromtimestamp(j["hb"] / 1000)

        # Тут что-то поделать с сообщениями
        if j.get("topic") != "tic":
            cprint(json.dumps(j, indent=None, default=str), "white")
            if "31" in j:
                send_message(j, instruments, redis_client)

    except Exception as e:
        cprint(f"Bad JSON? {d}, {e}", "red")


def worker(config, hb_queue, data_queue):
    print(config)
    ib = get_ib_instance(config)

    ib.load_session()
    cp = ib.session.cookies.get("cp")

    print("CP", cp)

    cprint("CONNECT", "green")
    ws = websocket.create_connection(ib.get_websocket_url(), cookie=f"cp={cp}")

    time.sleep(1)

    cprint("SUBSCRIBE", "green")
    for instrument in config['instruments']:
        conid = instrument["conid"]
        ws.send(f"smd+{conid}+" + '{"fields":["31"]}')

    last_echo = datetime.now()
    last_tic = datetime.now()

    try:
        while d := ws.recv():
            try:
                d = d.decode()
                data_queue.put(d)
                hb_queue.put("ALIVE")
            except Exception as e:
                cprint(f"Unknown decoding exception {e}", "red")

            # recv прерывается не реже, чем таймаут watchdog
            # Здесь можно проверить, не пора ли подергать сокет

            delay = (datetime.now() - last_echo).total_seconds()
            if delay > 27:
                cprint(f"NEW ECHO", "yellow")
                ws.send("ech+hb")
                last_echo = datetime.now()

            delay = (datetime.now() - last_tic).total_seconds()
            if delay > 57:
                cprint(f"NEW TIC", "yellow")
                ws.send("tic")
                last_tic = datetime.now()

    except websocket.WebSocketConnectionClosedException:
        data_queue.put("CLOSED")

    except KeyboardInterrupt:
        for instrument in config['instruments']:
            conid = instrument["conid"]
            ws.send(f"umd+{conid}" + '{}')
        cprint(f"STOPPED", "red")
        data_queue.put("STOPPED")

    except Exception as e:
        cprint(f"Unknown exception {e}", "red")
        data_queue.put("EXCEPTION")


def watchdog(hb_queue, data_queue):
    """
    This check the queue for updates and send a signal to it
    when the child process isn't sending anything for too long
    """
    while True:
        try:
            hb_queue.get(timeout=15)
        except queue.Empty as e:
            cprint(f"[WATCHDOG]: Maybe WORKER is slacking {e}", "red")
            data_queue.put("KILL WORKER")


def start_process(hb_queue, data_queue):
    config = get_config()
    process = mp.Process(
        target=worker,
        args=(config, hb_queue, data_queue),
    )
    process.start()
    return process


@click.command()
@click.argument('config_path', type=click.Path(exists=True))
def main(config_path):
    """The main process"""
    config = get_config(config_path)
    redis_client = get_redis_client(config)

    hb_queue = mp.Queue()
    data_queue = mp.Queue()

    watchdog_process = mp.Process(
        target=watchdog,
        args=(hb_queue, data_queue),
    )
    watchdog_process.daemon = True
    watchdog_process.start()

    workr = start_process(hb_queue, data_queue)

    while True:
        msg = data_queue.get()

        if msg and '"authenticated": false' in msg:
            cprint(f"[MAIN]: unauthenticated, {msg}", "red")
            time.sleep(10)
            msg = "KILL WORKER"

        if msg and msg[0] == "{":
            # Сообщение от IBKR
            parse_data(msg, config['instruments'], redis_client)

        elif msg == "KILL WORKER":
            cprint("[MAIN]: Terminating slacking WORKER", "yellow")
            workr.terminate()
            time.sleep(0.1)
            if not workr.is_alive():
                cprint("[MAIN]: WORKER is a goner", "yellow")
                workr.join(timeout=1.0)
                cprint("[MAIN]: Joined WORKER successfully!", "yellow")

                cprint("\n\nSTART AGAIN", "green")
                workr = start_process(hb_queue, data_queue)
            else:
                cprint("[MAIN] что-то пошло не так", "red")
                pass

        elif msg == "STOPPED":
            data_queue.close()
            hb_queue.close()
            break

        else:
            cprint(msg, "blue")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("DONE")
