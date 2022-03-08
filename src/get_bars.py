import json
import sys
import logging
import orjson
import pandas_market_calendars as mcal
from time import sleep
from random import shuffle
from functools import cache
from collections import Counter
from termcolor import cprint
from datetime import datetime, timedelta, timezone
from os.path import abspath, join, dirname
from config import get_ib_instance, instruments, dashboard_csv_path, get_redis_client

log = logging.getLogger("loader")


logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

ib = get_ib_instance()
PORTAL_URL = "https://%s/portal.proxy/v1/portal" % ib.base_hostname
HISTORY_URL = PORTAL_URL + "/iserver/marketdata/history"

EXCHANGE_SCHEDULE = {
    "NASDAQ": "NASDAQ",
    "NYMEX": "NYSE",
    "NYSE": "NYSE",
    "ARCA": "NYSE",
    "GLOBEX": "CME_Rate",
}


def get_stats_for_hour(data):
    cnt = Counter()

    for line in data:
        try:
            j = orjson.loads(line.decode())
        except ValueError:
            cnt["error"] += 1
            continue
        if j.get("error"):
            cnt["error"] += 1
        elif j.get("closed"):
            cnt["closed"] += 1
        elif j.get("empty"):
            cnt["empty"] += 1
        elif j.get("fix") or j.get("late"):
            cnt["fix"] += 1
        else:
            cnt["ok"] += 1

    return cnt


def update_dash(instruments, csv_path, redis_client):
    """
    Обновление CSV со статусами по часам.
    """
    end = datetime.utcnow()
    start = end - timedelta(hours=120)
    start = start.replace(minute=0, second=0, microsecond=0)

    dash_csv_data = "ticker,group,ok,closed,error,fix,empty\n"

    for instrument in instruments:
        key = get_key(instrument)

        cur_hour_interval = start
        for n in range(300):
            # Запросить данные для этого интервала
            data = redis_client.zrangebyscore(
                key,
                dt_to_ts(cur_hour_interval),
                dt_to_ts(cur_hour_interval) + 3599
            )
            stats = get_stats_for_hour(data)
            dash_csv_data += (
                f"{key},{cur_hour_interval},"
                f"{stats['ok']},{stats['closed']},"
                f"{stats['error']},{stats['fix']},"
                f"{stats['empty']}\n"
            )

            cur_hour_interval += timedelta(hours=1)
            if cur_hour_interval > datetime.utcnow():
                break

    with open(csv_path, "w") as f:
        f.write(dash_csv_data)


def dt_range(start, end, step=timedelta(minutes=1)):
    curr = start
    while curr <= end:
        yield curr
        curr += step


def ts_to_dt(ts):
    return datetime.utcfromtimestamp(ts / 1000)


def dt_to_ts(dt):
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def get_key(instrument):
    # Всё правильно, в базу бары складываются с ключом TRADES
    return "{symbol}.{exchange}:TRADES".format(**instrument)


def replace_data(instrument, line, ts, redis_client):
    """
    Запись в базу с заменой старых данных.
    """
    key = get_key(instrument)
    line_str = json.dumps(line, indent=None, separators=(',', ':'), default=str)
    dt = ts_to_dt(ts*1000)
    cprint(f"{key}, {ts}, {dt}, {line_str}", "white")
    redis_client.zremrangebyscore(key, ts, ts)
    redis_client.zadd(key, {line_str: ts})

    symbol = "{symbol}.{exchange}".format(**instrument)
    line["conid"] = instrument["conid"]
    line["symbol"] = symbol
    line_str = json.dumps(line, indent=None, separators=(',', ':'), default=str)
    redis_client.publish(f"{symbol}:BARS", line_str)


@cache
def get_calendar_and_schedule(exchange):
    """
    Календарь и расписание для биржи.
    """
    calendar = mcal.get_calendar(EXCHANGE_SCHEDULE[exchange])

    # нужно покрыть вперед и назад все возможные выходные
    start = datetime.utcnow() - timedelta(days=10)
    end = datetime.utcnow() + timedelta(days=100)

    # TODO: убрать хардкодинг
    if EXCHANGE_SCHEDULE[exchange] in ["NYSE", "NASDAQ"]:
        schedule = calendar.schedule(start, end, start="pre", end="post")
    else:
        schedule = calendar.schedule(start, end)

    return calendar, schedule


@cache
def check_open_time(exchange, cur_interval):
    """
    Открыта ли эта биржа в указанный момент.
    """
    calendar, schedule = get_calendar_and_schedule(exchange)
    cur_interval_utc = cur_interval.replace(tzinfo=timezone.utc)
    try:
        return calendar.open_at_time(schedule, cur_interval_utc)
    except ValueError as e:
        print(schedule)
        raise e


def format_valid_interval(interval):
    return {
        "dt": datetime.strftime(interval["dt"], "%Y-%m-%d %H:%M:%S"),
        "o": interval["o"],
        "h": interval["h"],
        "l": interval["l"],
        "c": interval["c"],
        "vol": interval["v"],
        # "rth": int(interval["rth"]),
    }


def load_intervals_from_ibkr(ib, instrument, period, data_grid):
    # Запрос в IBKR
    q = f"?conid={instrument['conid']}&period={period}min&bar=1min&outsideRth=true"
    try:
        ib.reset_session()
        ib.load_session()
        res_json = ib.iserver_request(HISTORY_URL + q, "GET")
        print(res_json)
        print('='*80)
    except Exception as e:
        cprint(f"ERROR requests {e}", "red")
        raise e

    # TODO: определять ситуацию, когда данные в начале торгового дня приходят
    # TODO: только за прошлый день (значит за этот день данных еще не было)

    # Если сейчас премаркет или основная сессия,
    # а данные приходят за постмаркет предыдущего дня.

    # В этом случае записать пустые данные с флагом EMPTY
    # во все ячейки сетки, когда биржа уже была открыта.

    try:
        data = res_json.get("data")
        # print(data)

        mnt = timedelta(minutes=1)
        prev_dt = None
        for interval in data:
            ts = interval["t"] // 1000

            dt = ts_to_dt(interval["t"])
            # cprint(dt, "green")

            # Если перед этим интервалом был гэп — навставлять EMPTY
            if dt and prev_dt and dt - prev_dt > mnt:
                cprint("large gap", "blue")
                for gap_dt in dt_range(prev_dt + mnt, dt - mnt):
                    if check_open_time(instrument["exchange"], gap_dt):
                        # cprint(f"{gap_dt} open, but no data", "yellow")
                        gap_ts = dt_to_ts(gap_dt)
                        if gap_ts in data_grid:
                            interval["dt"] = gap_dt
                            data_grid[gap_ts]["new"] = {
                                "dt": datetime.strftime(gap_dt, "%Y-%m-%d %H:%M:%S"),
                                "empty": 1,
                            }
                            data_grid[gap_ts]["t"] = gap_ts * 1000

            if ts in data_grid:
                interval["dt"] = dt
                data_grid[ts]["new"] = format_valid_interval(interval)
                data_grid[ts]["t"] = interval["t"]
            else:
                log.debug(f"Time is not in data_grid {ts} {dt}")
            prev_dt = dt

        # print('\n\n\n')
    except Exception as e:
        cprint(f"ERROR: {res_json} {e}", "yellow")
        raise e

    return data_grid


def fill_gaps(ib, instrument, data_grid):

    # # Последний успешно загруженный с биржи интервал
    # last_good_ts = dt_to_ts(datetime.utcnow())
    # for score, line in data_grid.items():
    #     data = line.get("old")
    #     if data and not ("ERROR" in data or "CLOSE" in data):
    #         last_good_ts = score * 1000

    # Последний интервал, когда биржа была открыта.
    # От него считается period.
    last_open_dt = datetime.utcnow() - timedelta(days=10)  # далеко в прошлом
    for score, line in data_grid.items():
        if line["is_it_open"]:
            last_open_dt = datetime.strptime(line["dt"], "%Y-%m-%d %H:%M:%S")

    # Первый ключ плохих данных, которые нужно перезагружать
    first_bad_dt = None
    for score, line in data_grid.items():
        dt = datetime.strptime(line["dt"], "%Y-%m-%d %H:%M:%S")
        delta = (last_open_dt - dt).total_seconds() // 60
        if delta > 1000:
            # Сшилком далеко в прошлое, не рассматриваем
            continue
        if delta < 0:
            # Интервал был после последнего закрытия, не рассматриваем
            break

        if not line.get("old") or ("error" in line.get("old")):
            first_bad_dt = datetime.strptime(line["dt"], "%Y-%m-%d %H:%M:%S")
            break

    print()
    print("NOW UTC  ", datetime.utcnow().replace(microsecond=0))
    print("First bad", first_bad_dt)
    print("Last open", last_open_dt, dt_to_ts(last_open_dt))

    if first_bad_dt:
        # Хотим загрузить какие-то недогруженные данные.
        # Запас +5 нужен, чтобы поймать gap.
        period = int((last_open_dt - first_bad_dt).total_seconds() // 60) + 5
        cprint(f"GET IBKR DATA, period: {period}", "blue")
        try:
            data_grid = load_intervals_from_ibkr(ib, instrument, period, data_grid)
        except Exception as e:
            log.error("load_intervals_from_ibkr")
            log.exception(e)

    # Сгенерить данные для закрытых интервалов
    for score, line in data_grid.items():
        if line.get("is_it_open") is False:
            if not line.get("old") or ("error" in line.get("old")):
                data_grid[score]["new"] = {
                    "dt": line["dt"],
                    "closed": 1,
                }

    return data_grid


def update_instrument(ib, interval_dt, symbol, redis_client):

    # IBKR позволяет грузить данные только на 1000 интервалов назад,
    # но в них не входят интервалы закрытой биржи, поэтому делаю запас.
    cur_minute = datetime.utcnow().replace(second=0, microsecond=0)
    start = cur_minute - timedelta(days=3)

    # Пустая сетка интервалов с расписанием биржи
    data_grid = {}
    for cur_interval in dt_range(start, interval_dt):
        is_it_open = check_open_time(symbol["exchange"], cur_interval)
        data_grid[dt_to_ts(cur_interval)] = {
            "dt": datetime.strftime(cur_interval, "%Y-%m-%d %H:%M:%S"),
            "is_it_open": is_it_open,
        }

    # Интервалы в базе данных от start до конца
    key = get_key(symbol)
    data_in_db = redis_client.zrangebyscore(key, dt_to_ts(start), 10 ** 10)

    # Положить интервалы из базы в сетку
    for line in data_in_db:
        line = line.decode()
        try:
            line_data = orjson.loads(line)
        except orjson.JSONDecodeError:
            log.error(f"JSONDecodeError: {line}")
            return False
        dt = datetime.strptime(line_data["dt"], "%Y-%m-%d %H:%M:%S")
        ts = dt_to_ts(dt)
        if ts in data_grid:
            data_grid[ts]["old"] = line_data

    # Метод заполняет пробелы из IBKR или флагом "CLOSED"
    data_grid = fill_gaps(ib, symbol, data_grid)

    # # print(json.dumps(data_grid, indent=2, default=str))
    # for line in data_grid.values():
    #     print("old:", line.get("old"), "  new:", line.get("new"))
    # sys.exit()

    # Найти различачающиеся данные и сохранить или вывести ошибку
    for score, line in data_grid.items():

        # Уже были данные
        if "old" in line:
            # Но теперь есть другие данные
            old_line = line["old"]
            old_line.pop('late', None)
            old_line.pop('fix', None)
            old_line.pop('avg', None)
            old_line.pop('cnt', None)
            old_line.pop('rth', None)
            if "new" in line and line["new"] != old_line:
                new_line = line["new"]
                new_line["fix"] = 1
                replace_data(symbol, new_line, score, redis_client)

        # Данных не было
        else:
            if new_line := line.get("new"):
                # Если данные пришли не real-time, то ставлю флаг LATE
                if datetime.strptime(line["dt"], "%Y-%m-%d %H:%M:%S") < interval_dt:
                    new_line["late"] = 1
                replace_data(symbol, new_line, score, redis_client)
            else:
                log.debug("Данных всё нет и нет")

    current_interval_data = data_grid[dt_to_ts(interval_dt)]

    # Загрузка считается успешной, если появился new
    # или если есть old в статусе, не требующем изменения (не ошибка)
    done = bool(current_interval_data.get("new"))
    done = done or ("ERROR" not in current_interval_data.get("old", ""))
    return done


def loader(ib, dt_start, instruments, redis_client):
    """
    Грузить интервал, пока не загрузится или не наступит новый интервал.
    """
    timeout = 10  # максимальное время на загрузку одного инструмента
    sleep_time = 3

    # Какой интервал грузить
    cur_minute = dt_start.replace(second=0, microsecond=0)
    interval_dt = cur_minute - timedelta(minutes=1)

    # Перемешиваю, чтобы при залипании первого инструмента не застряли все
    shuffle(instruments)

    for symbol in instruments:
        while True:
            try:
                if update_instrument(ib, interval_dt, symbol, redis_client):
                    # Успешно загрузилось
                    break
            except Exception as e:
                cprint(f"ERROR load_interval {e}", "yellow")
                log.exception(e)

            dt = datetime.utcnow()

            # Если интервал так и не загрузился — записать ошибку

            if dt - dt_start > timedelta(seconds=timeout):
                cprint(f"ERROR интервал долго не грузится", "red")
                line_data = {
                    "dt": datetime.strftime(interval_dt, "%Y-%m-%d %H:%M:%S"),
                    "error": 2,
                }
                replace_data(symbol, line_data, dt_to_ts(interval_dt), redis_client)

                # Прекращаем грузить инструмент
                break

            if dt.minute != dt_start.minute:
                cprint("ERROR пора грузить новый интервал", "red")
                line_data = {
                    "dt": datetime.strftime(interval_dt, "%Y-%m-%d %H:%M:%S"),
                    "error": 3,
                }
                replace_data(symbol, line_data, dt_to_ts(interval_dt), redis_client)

                # Прекращаем грузить данный интервал
                return

            # Перерыв после неудачной попытки
            sleep(sleep_time)


def main(ib):
    base_dir = abspath(dirname(__file__))
    csv_path = abspath(join(base_dir, dashboard_csv_path))

    prev_dt = datetime(2000, 1, 1)
    redis_client = get_redis_client()

    while True:
        dt = datetime.utcnow()
        if dt.minute != prev_dt.minute and dt.second > 10:
            # Начать загрузку нового минутного интервала
            prev_dt = dt
            loader(ib, dt, instruments, redis_client)
            update_dash(instruments, csv_path, redis_client)
            # redis_client.close()
            print()
            print("-------- конец итерации ---------", datetime.utcnow())
            print()
        else:
            sleep(1)


if __name__ == "__main__":
    ib = get_ib_instance()

    try:
        main(ib)
    except KeyboardInterrupt:
        print("DONE")
