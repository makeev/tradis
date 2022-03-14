# Скрипты сессии IBKR и торговых данных 


## config_local.yaml
Конфигурация скриптов, обновляющих сессию и real-time данные.

Пример лежит в config_example.yaml.

### secret
Ключ ширования для данных сессии в Redis.

#### Генерация нового ключа
```
from cryptography.fernet import Fernet
Fernet.generate_key()
```

### instruments
Список инструментов для наблюдения.
* conid — id контракта в IBKR;
* exchange — биржа или провайдер данных, чьи данные используются.
```
instruments:
  - conid: 265598
    symbol: AAPL
    exchange: NASDAQ

  - conid: 461318791
    symbol: MES
    exchange: GLOBEX
```


## session_keeper.py ../config_example.yaml

Поддерживает живую торговую сессию для аккаунта из конфига.
Сессию складывает в Redis. Должен быть запущен, чтобы у скриптов ниже была живая сессия.


## get_bars.py ../config_example.yaml

Загружает минутные бары. Сохраняет dash.csv для отображения дашборда по наличию данных.


## get_trades.py ../config_example.yaml

Подписывается на стрим сделок и кладет их в Redis.


# Dashboard

В директории dash лежит фронтенд дашборда. Туда же складывается результат обновления баров.
