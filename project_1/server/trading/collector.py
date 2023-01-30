import csv
import functools
from datetime import datetime
import pandas as pd
import requests
import finnhub
from io import StringIO

historical_api_key = 'FKPZKNCNF7741FAK'
realtime_api_key = 'cfbh1q1r01qre8p5nmr0cfbh1q1r01qre8p5nmrg'

historical_endpoint = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={' \
                      'symbol}&interval={interval}min&slice={slice}&apikey={apikey} '


class Collector:
    def historical(self, symbol, interval):
        years = (1, 2)
        months = range(1, 13)
        data_list = []
        for y in years:
            for m in months:
                slice = f'year{y}month{m}'
                data_list.append(self.historical_slice(symbol, interval, slice))
        data = pd.concat(data_list)
        return data

    def historical_slice(self, symbol, interval, slice):
        with requests.Session() as s:
            url = historical_endpoint.format(symbol=symbol, interval=interval, slice=slice, apikey=historical_api_key)
            download = s.get(url)
            decoded_content = download.content.decode('utf-8')
            # print(decoded_content)
            # cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            # row_list = list(cr)
            # return row_list
            data = pd.read_csv(StringIO(decoded_content))
            data['time'] = pd.to_datetime(data['time'], format='%Y-%m-%d %H:%M')
            return data.loc[:, ['time', 'close']]

    def realtime(self, ticker):
        finnhub_client = finnhub.Client(api_key=realtime_api_key)
        return self._subset_and_convert_realtime(finnhub_client.quote(ticker))

    def lookup_symbol(self, symbol):
        finnhub_client = finnhub.Client(api_key=realtime_api_key)
        response = finnhub_client.symbol_lookup(symbol)
        return any(map(functools.partial(self._symbol_exists, symbol), response['result']))

    @staticmethod
    def _subset_and_convert_realtime(data):
        return [datetime.fromtimestamp(data['t']), data['c']]

    @staticmethod
    def _symbol_exists(symbol, result):
        return symbol == result['symbol']
