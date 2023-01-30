import csv
import functools
from datetime import datetime
import pandas as pd
import requests
import finnhub
from finnhub.exceptions import FinnhubAPIException
from io import StringIO

historical_api_key = 'CRQYLDV361ZDL8NC'  # 'FKPZKNCNF7741FAK'
realtime_api_key = 'cfbh1q1r01qre8p5nmr0cfbh1q1r01qre8p5nmrg'

# historical_endpoint = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={' \
#                       'symbol}&interval={interval}min&slice={slice}&apikey={apikey} '
historical_endpoint = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}min&outputsize=full&apikey={apikey}&datatype=csv'


class Collector:
    def historical(self, symbol, interval):
        # years = (1, 2)
        # months = range(1, 13)
        # data_list = []
        # for y in years:
        #     for m in months:
        #         slice = f'year{y}month{m}'
        #         data_list.append(self.historical_slice(symbol, interval, slice))
        # data = pd.concat(data_list)
        data = self.historical_slice(symbol, interval, None)
        return data

    def historical_slice(self, symbol, interval, slice):
        with requests.Session() as s:
            # url = historical_endpoint.format(symbol=symbol, interval=interval, slice=slice, apikey=historical_api_key)
            url = historical_endpoint.format(symbol=symbol, interval=interval, apikey=historical_api_key)
            download = s.get(url)
            decoded_content = download.content.decode('utf-8')
            if not download.ok:
                raise Exception(download.raise_for_status())
            data = pd.read_csv(StringIO(decoded_content))
            try:
                # data['datetime'] = pd.to_datetime(data['time'], format='%Y-%m-%d %H:%M')
                data['datetime'] = pd.to_datetime(data['timestamp'], format='%Y-%m-%d %H:%M')
            except KeyError:
                raise Exception('exceeded allowable calls of data API')
            data = data.rename(columns={'close': 'price'})
            data['ticker'] = symbol
            return data.loc[:, ['datetime', 'ticker', 'price']].set_index('datetime').sort_index()

    def realtime(self, ticker):
        try:
            finnhub_client = finnhub.Client(api_key=realtime_api_key)
            return {ticker: [self._subset_and_convert_realtime(finnhub_client.quote(ticker), ticker)]}
        except FinnhubAPIException:
            raise Exception('temporarily rate limited')


    def lookup_symbol(self, symbol):
        finnhub_client = finnhub.Client(api_key=realtime_api_key)
        response = finnhub_client.symbol_lookup(symbol)
        return any(map(functools.partial(self._symbol_exists, symbol), response['result']))

    @staticmethod
    def _subset_and_convert_realtime(data, ticker):
        return {'datetime': datetime.fromtimestamp(data['t']), 'price': data['c'], 'ticker': ticker}

    @staticmethod
    def _symbol_exists(symbol, result):
        matching = symbol == result['symbol']
        return matching
