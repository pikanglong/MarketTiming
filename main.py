import ccxt
import time
import logging
import requests
import configparser
import pandas as pd
from datetime import datetime, timedelta

logging.basicConfig(filename='main.log', format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

pd.set_option('expand_frame_repr', False)

cp = configparser.ConfigParser()
cp.read('main.conf')

apiKey = cp.get('binance', 'apiKey')
secret = cp.get('binance', 'secret')
sckey = cp.get('serverchan', 'sckey')

api = 'https://sc.ftqq.com/' + sckey + '.send'

time_interval = '15m'

exchange_id = 'binance'
exchange_class = getattr(ccxt, exchange_id)
exchange = exchange_class({
    'apiKey': apiKey,
    'secret': secret,
    'timeout': 30000,
    'enableRateLimit': True,
})

symbol = 'ETH/USDT'
base_coin = symbol.split('/')[-1]
trade_coin = symbol.split('/')[0]

length = 100
mult = 3

def next_run_time(time_interval):
    if time_interval.endswith('m'):
        now_time = datetime.now()
        time_interval = int(time_interval.strip('m'))
        target_min = (int(now_time.minute / time_interval) + 1) * time_interval

        if target_min < 60:
            target_time = now_time.replace(minute=target_min, second=0, microsecond=0)
        else:
            if now_time.hour == 23:
                target_time = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
                target_time += timedelta(days=1)
            else:
                target_time = now_time.replace(hour=now_time.hour + 1, minute=0, second=0, microsecond=0)

        if (target_time - datetime.now()).seconds < 3:
            logging.info('Less than 3 second away from the target time, the program will run in the next cycle.')
            target_time += timedelta(minutes=time_interval)
        logging.info('Next run time: %s', target_time)
        return target_time
    else:
        logging.error('time_interval doesn\'t end with m')

def get_candle_data(exchange, symbol, time_interval, length):
    content = exchange.fetch_ohlcv(symbol, timeframe=time_interval, limit=length+3)

    df = pd.DataFrame(content, dtype=float)
    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'}, inplace=True)
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')
    df['candle_begin_time_GMT8'] = df['candle_begin_time'] + timedelta(hours=8)
    df = df[['candle_begin_time_GMT8', 'open', 'high', 'low', 'close', 'volume']]

    return df

def get_signal(df, length, mult):
    n = length
    m = mult

    df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)
    df['median'] = df['close'].rolling(n, min_periods=1).mean()
    df['upper'] = df['median'] + m * df['std']
    df['lower'] = df['median'] - m * df['std']

    condition1 = df['close'] > df['upper']
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)
    df.loc[condition1 & condition2, 'signal_long'] = 1

    condition1 = df['close'] < df['median']
    condition2 = df['close'].shift(1) >= df['median'].shift(1)
    df.loc[condition1 & condition2, 'signal_long'] = 0

    condition1 = df['close'] < df['lower']
    condition2 = df['close'].shift(1) >= df['lower'].shift(1)
    df.loc[condition1 & condition2, 'signal_short'] = -1

    condition1 = df['close'] > df['median']
    condition2 = df['close'].shift(1) <= df['median'].shift(1)
    df.loc[condition1 & condition2, 'signal_short'] = 0
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, skipna=True)

    return df

def send_message(api, title, content):
    data = {
        'text': title,
        'desp': content
    }
    requests.post(api, data = data)

while True:
    logging.info('started')

    balance = exchange.fetch_balance()['total']
    base_coin_amount = float(balance[base_coin])
    trade_coin_amount = float(balance[trade_coin])
    logging.info('current account value: %.8f%s %.8f%s', base_coin_amount, base_coin, trade_coin_amount, trade_coin)

    run_time = next_run_time(time_interval)
    time.sleep(max(0, (run_time - datetime.now()).seconds))

    while True:
        if datetime.now() < run_time:
            continue
        else:
            break
    
    while True:
        df = get_candle_data(exchange, symbol, time_interval, length)
        temp = df[df['candle_begin_time_GMT8'] == (run_time - timedelta(minutes=int(time_interval.strip('m'))))]
        if temp.empty:
            logging.info('Data is not new enough.')
            continue
        else:
            break
    
    df = df[df['candle_begin_time_GMT8'] < pd.to_datetime(run_time)]
    df = get_signal(df, length, mult)
    signal = df.iloc[-1]['signal']

    # TODO: buy or sell

    logging.info('finished')

    time.sleep(1)