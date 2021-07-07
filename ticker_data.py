#! ~/anaconda3/envs/quant/bin/python3
import requests
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm
import logging

logging.basicConfig(filename="logs/data.log",
                    format='%(asctime)s %(message)s',
                    filemode='w',
                    level=logging.INFO)

API_KEY = "ddI6wJUdt_Qdg4SAfbpX9rpt56YXnRID"
INFO_URL = "https://api.polygon.io/vX/reference/tickers/{}?&apiKey={}"
DATA_URL = "https://api.polygon.io/v2/aggs/ticker/{}/range/1/day/2011-01-01/2020-12-31?unadjusted=false&sort=asc&limit=50000&apiKey={}"

DUPLICATES = [('GOOGL', 'GOOG'), ('DISCA', 'DISKC'), ('FOXA', 'FOX'), ('NWSA', 'NWS'), ('UAA', 'UA')]

def __acquire_sp500():
    logging.info('Fetching SP500 constituents...')
    try:
        response = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'Connection': 'close'})
    except (
        requests.ConnectionError,
        requests.exceptions.ReadTimeout,
        requests.exceptions.Timeout,
        requests.exceptions.ConnectTimeout
    ) as e:
        logging.warning('Fetching %s data failed w/ error: %s' % str(e))
        exit(-1)
    if not response.ok:
        logging.warning('Fetching SP500')
        exit(-1)
    logging.info('Fetching SP500 success.')
    tickers = pd.read_html(response.text)[0]['Symbol'].values
    return [ticker for ticker in list(tickers) if ticker not in DUPLICATES]

def get_tickers():
    return list(pd.read_csv('data/tickers.csv')['tickers'].values)    

def __acquire_ticker_data(tickers):
    all_ticker_info = []
    
    # First get market data
    for market in ['SPY', 'NDAQ']:
        logging.info('Fetching %s data..' % market)
        try:
            data_response = requests.get(DATA_URL.format(market, API_KEY), headers={'Connection': 'close'})
        except (
            requests.ConnectionError, 
            requests.exceptions.ReadTimeout,
            requests.exceptions.Timeout,
            requests.exceptions.ConnectTimeout
        ) as e:
            logging.warning('Fetching %s data failed w/ error: %s' % (market, str(e)))
            exit(-1)
        if not data_response.ok:
            logging.warning('Fetching %s data failed w/ error: 404 Not Found' % market)
            exit(-1)
        else:
            logging.info('Fetching %s data success.' % market)
        market_data = pd.DataFrame(data_response.json()['results'])[['t','o','h','l','c','v','vw','n']].rename(
                                    columns={'t': 'date', 'o': 'open', 'h': 'high', 'l': 'low',
                                            'c': 'close', 'v': 'volume', 'vw': 'weighted volume', 
                                            'n': 'transactions'})
        market_data['date'] = pd.to_datetime(market_data['date'], unit='ms')
        market_data = market_data.set_index('date')
        market_data.to_csv('data/ohlc/%s.csv' % market)

    # Then get ticker data
    for ticker in tqdm(tickers):
        logging.info('Fetching %s data...' % ticker)
        try:
            info_response = requests.get(INFO_URL.format(ticker, API_KEY), headers={'Connection': 'close'})
            data_response = requests.get(DATA_URL.format(ticker, API_KEY), headers={'Connection': 'close'})
        except (
            requests.ConnectionError, 
            requests.exceptions.ReadTimeout,
            requests.exceptions.Timeout,
            requests.exceptions.ConnectTimeout
        ) as e:
            logging.warning('Fetching %s data failed w/ error: %s' % (ticker, str(e)))
            continue
        if not info_response.ok or not data_response.ok:
            logging.warning('Fetching %s data failed w/ error: 404 Not Found' % ticker)
            continue
        else:
            logging.info('Fetching %s data success.' % ticker)
        ticker_info = info_response.json()['results']
        filtered_ticker_info = {}
        for key in ['ticker', 'name', 'sic_code', 'sic_description']:
            if key not in ticker_info:
                logging.warning('Fetching %s data does not contain %s' % (ticker, key))
            else:
                filtered_ticker_info[key] = ticker_info[key]  
        all_ticker_info.append(filtered_ticker_info)
        ticker_data = pd.DataFrame(data_response.json()['results'])[['t','o','h','l','c','v','vw','n']].rename(
                                    columns={'t': 'date', 'o': 'open', 'h': 'high', 'l': 'low',
                                             'c': 'close', 'v': 'volume', 'vw': 'weighted volume', 
                                             'n': 'transactions'})
        ticker_data['date'] = pd.to_datetime(ticker_data['date'], unit='ms')
        ticker_data = ticker_data.set_index('date')
        ticker_data.to_csv('data/ohlc/%s.csv' % ticker)
        logging.info('Fetching %s data saved.' % ticker)

    all_ticker_info = pd.DataFrame(all_ticker_info).set_index('ticker')
    all_ticker_info.to_csv('data/tickers.csv')
    logging.info('Saved all ticker info.')

def get_ticker_data(ticker, start, end):
    return pd.read_csv('data/ohlc/%s.csv' % ticker, index_col=0, parse_dates=True).loc[start:end]

def get_market_data(start, end):
    return get_ticker_data('SPY', start, end), get_ticker_data('NDAQ', start, end)

def __consolidate_ticker_data():
    index = pd.read_csv('data/ohlc/ndaq.csv', index_col=0, parse_dates=True).index
    tickers = list(pd.read_csv('data/tickers.csv')['ticker'].values)
    all_ticker_data = pd.DataFrame(index=index)
    for i, ticker in enumerate(tqdm(tickers)):
        logging.info('Consolidating %s data.' % ticker)
        ticker_data = pd.read_csv('data/ohlc/%s.csv' % ticker, index_col=0, parse_dates=True)
        all_ticker_data[ticker] = ticker_data['close']
    pct_change = all_ticker_data.pct_change(fill_method=None).iloc[1:]
    log_diff = np.log(1 + pct_change)
    pct_change.to_csv('data/cross_sectional/pct_change.csv')
    log_diff.to_csv('data/cross_sectional/log_diff.csv')
    logging.info('Saved cross_sectional percent change data.')

def get_cross_sectional_data(start, end, data="pct_change"):
    if data not in ["pct_change", "log_diff"]:
        raise ValueError("data should be one of %s" % str(["pct_change", "log_diff"]))
    return pd.read_csv('data/cross_sectional/%s.csv' % data, index_col=0, parse_dates=True).loc[start:end].dropna(axis=1)

if __name__ == "__main__":
    print('Fetching S&P 500') 
    tickers = __acquire_sp500()

    print('Fetching ticker data')
    __acquire_ticker_data(tickers)

    print('Consolidating ticker data')
    __consolidate_ticker_data()