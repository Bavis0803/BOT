import requests
import concurrent.futures
import pandas as pd
import time
import pandas_ta as ta
import numpy as np
symbols = []


def get_all_ticker():
    global symbols
    url = "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"

    response = requests.get(url)
    response_json = response.json()
    if response.status_code == 200:
        symbols = [item['symbol'] for item in response_json['data']]
        return symbols
    else:
        print("Error:", response.status_code)
        return None


def send_message(symbol, side, timeframe, limit, tp, stoploss):
    bot_token = '6637417095:AAEhdi-XyNF_JHlANK08uj9L_14rB9g_1Z8'
    chat_id = -1002096734555

    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    if side == "L":
        message_text = f"""🟢🟢[BOT {timeframe}]]🟢🟢
{symbol} - LONG LIMIT
LIMIT: {limit}
SL: {stoploss}
TP: {tp}
"""
        params = {
            'chat_id': chat_id,
            "message_thread_id": 4,
            'text': message_text,
        }
    elif side == "S":
        message_text = f"""🔴🔴[BOT {timeframe}]🔴🔴
{symbol} - SELL LIMIT
LIMIT: {limit}
SL: {stoploss}
TP: {tp}
"""
        params = {
            'chat_id': chat_id,
            "message_thread_id": 4,
            'text': message_text,
        }
    try:
        response = requests.post(url, params=params)
        if response.status_code == 200:
            print('Message sent successfully!')
        else:
            print(
                f'Error sending message. Status code: {response.status_code}')
            time.sleep(2)
            send_message(symbol, side, timeframe, limit, tp, stoploss)
    except Exception as e:
        print(f'An error occurred: {e}')


symbols = get_all_ticker()
symbol_dict = {symbol: 'O' for symbol in symbols}


def perform_strategy(symbol, timeframe):
    global symbol_dict
    url = f"https://api.bitget.com/api/v2/mix/market/candles?symbol={symbol}&granularity={timeframe}&limit=1000&productType=usdt-futures"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()['data']
        columns = ['timestamp', 'open', 'high', 'low',
                   'close', 'volume', 'quote currency']
        df = pd.DataFrame(data, columns=columns)
        pd.set_option('display.float_format', '{:.10f}'.format)
        df = df.applymap(pd.to_numeric, errors='ignore')
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df[['lower_band', 'mid', 'upper_band']] = ta.bbands(
            df.close, length=20, std=2).iloc[:, :3]
        df.dropna(subset=["mid"], inplace=True)
        signal_mapping = {1: 'L', -1: 'S', 0: 'O'}
        df['signal'] = np.where(df['close'] > df['upper_band'], -1, np.where(
            df['close'] < df['lower_band'], 1, 0))
        df['signal'] = df['signal'].map(signal_mapping)
        df['type'] = np.where(df['close'] > df['open'], 'G', np.where(
            df['close'] < df['open'], 'R', "None"))
        index_signal = None
        bot_signal = None
        bot_type = None
        for index, row in df.tail(5).iterrows():
            if row['signal'] in ['L', 'S'] and (row['lower_band'] != row['upper_band']):
                index_signal = index
                bot_signal = row['signal']
                bot_type = row['type']
                low = row['low']
                high = row['high']
                break
        if index_signal is not None:
            row = df.loc[index_signal+1]
            # long condition
            if bot_signal == 'L' and bot_type == 'R' and row['type'] == 'G':
                limit = row['close']
                stoploss = low
                stoploss_percentage = (
                    (limit - stoploss) / limit) * 100
                tp = limit * (1 + stoploss_percentage * 0.015)
                if symbol_dict[symbol] == 'L':
                    pass
                else:
                    symbol_dict[symbol] = 'L'
                    # call send mess function here
                    send_message(symbol, 'L', timeframe, limit,
                                 tp, stoploss)
            elif bot_signal == 'S' and bot_type == 'G' and row['type'] == 'R':
                limit = row['close']
                stoploss = high
                stoploss_percentage = (
                    (stoploss - limit) / limit) * 100
                tp = limit * (1 - (stoploss_percentage * 1.5 / 100))
                if symbol_dict[symbol] == 'S':
                    pass
                else:
                    symbol_dict[symbol] = 'S'
                    # call send mess function here
                    send_message(symbol, 'S', timeframe, limit,
                                 tp, stoploss)
    else:
        print("Error fetching data:", response.status_code)
        return None


def task_range(start, end):
    for i in range(start, end + 1):
        perform_strategy(symbols[i], "5m")
        time.sleep(1)


while True:
    ranges = [(i, i + 4) for i in range(0, 195, 5)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for start, end in ranges:
            future = executor.submit(task_range, start, end)
            futures.append(future)
        for future in concurrent.futures.as_completed(futures):
            future.result()
    time.sleep(300)
