from time import sleep

import click
from termcolor import cprint
from config import get_config, get_ib_instance


@click.command()
@click.argument('config_path', type=click.Path(exists=True))
def main(config_path):
    config = get_config(config_path)
    ib = get_ib_instance(config)
    ib.load_session()

    while True:
        # Проверить, есть жива ли SSO-сессия
        sso = ib.sso_validate()

        # Если сессия не работает — перелогин.
        if sso.get("_ERROR") or not sso.get("USER_ID"):
            cprint(" FULL RELOGIN ", "red", attrs=['reverse'])
            ib.portal_logout()
            ib.sso_logout()
            if not ib.obtain_session():
                print("Wait before reconnect")
                sleep(10)
            continue

        # Тут должна быть живая сессия,
        # проверить аунтетнификацию в iserver.
        iserver = ib.iserver_auth_status()

        # Не проверяется — перелогин.
        if iserver.get("_ERROR") is not False:
            print("bad iserver_status", iserver)
            sleep(10)
            continue

        # Сессия есть, но iserver не authenticated.
        # Попробовать оживить.
        if not iserver.get("authenticated"):
            print("iserver is not authenticated")
            print("SOFT REAUTH")
            iserver = ib.init_iserver_session()

        # Если оживить не получилось — перелогин.
        if not iserver.get("authenticated"):
            ib.portal_logout()
            ib.sso_logout()
            continue

        cprint(" GOOD SESSION ", "green", attrs=['reverse'])

        sleep(1)

        try:
            ib.keep_session_alive()
        except Exception as e:
            cprint(f"Tickle exception {e}", "red")
            sleep(3)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("DONE")
