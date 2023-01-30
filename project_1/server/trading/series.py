import json
import sys
from multiprocessing.managers import BaseManager
import pandas as pd

from .calc import signal, pnl

debug_mode = False


class PriceSeries:
    def __init__(self):
        self._data = {}
        self._window_str = '24H'
        self._window_int = 24
        self._interval = None

    def set_interval(self, interval):
        self._interval = interval

    def add(self, symbol, data: pd.DataFrame):
        self._data.update({symbol: data})

    def delete(self, symbol):
        self._data.pop(symbol)

    def update(self, data):
        for datum in data:

            symbol, d = next(iter(datum.items()))
            df = self._data.get(symbol)
            new_record = pd.DataFrame(d).set_index('datetime')
            df_concat = pd.concat([df, new_record]).sort_index()

            if not df.equals(df_concat):
                self.add(symbol, df_concat)
                relevant_periods = self._window_int * 60 / self._interval
                df_mini = df_concat.tail(int(relevant_periods)).copy()
                df_built = self._build_single_set(df_mini)
                df_concat.iloc[-1] = df_built.iloc[-1]
                self.add(symbol, df_concat)

    def build(self):
        for symbol in self._data.keys():
            self.build_single_symbol(symbol)

    def build_single_symbol(self, symbol):
        d = self._data.get(symbol)
        b = self._build_single_set(d)
        self.add(symbol, b)

    def _build_single_set(self, data: pd.DataFrame):
        avg = self._average_series(data.loc[:, 'price'])
        std = self._std_series(data.loc[:, 'price'])
        data.loc[:, 'avg'] = avg
        data.loc[:, 'std'] = std
        data.loc[:, 'signal'] = data.apply(lambda row: signal(row['price'], row['avg'], row['std'], row), axis=1)
        data.loc[:, 'pos'] = data['signal'].cumsum()
        data.loc[:, 'price_1'] = data['price'].shift(1).fillna(0)
        data.loc[:, 'pos_1'] = data['pos'].shift(1).fillna(0)
        data.loc[:, 'pnl'] = data.apply(lambda row: pnl(row['pos_1'], row['price'], row['price_1']), axis=1)
        return data

    def _average_series(self, df: pd.Series):
        return df.rolling(window=self._window_str, min_periods=1).mean().rename('avg')

    def _std_series(self, df: pd.Series):
        return df.rolling(window=self._window_str, min_periods=1).std().rename('std')

    def report(self):
        try:
            data = pd.concat(list(self._data.values()))
            data = data.sort_values(by=['datetime', 'ticker'])
            if not debug_mode:
                data = data.loc[:, ['ticker', 'price', 'signal', 'pnl']]
            return data
        except KeyError as e:
            traceback = sys.exc_info()[2]
            raise e.with_traceback(traceback)

    def _lastest_record_as_of(self, df, as_of):
        data = df.loc[df.index < as_of, ['ticker', 'price', 'signal']]
        if len(data) == 0:
            raise IndexError('Server has no data')
        # return data.iloc[[0]]
        return data.loc[data.index == data.index.max()]

    def snapshot(self, as_of_datetime):
        data_list = [self._lastest_record_as_of(d, as_of_datetime) for d in self._data.values()]
        data = pd.concat(data_list)
        # data = data.sort_values(by=['datetime', 'ticker'])
        return json.loads(data.to_json(orient='split'))


class PriceSeriesManager(BaseManager):
    pass


def create_PriceSeriesManager():
    PriceSeriesManager.register(
        'PriceSeries',
        PriceSeries,
        # exposed=['add', 'delete', 'build', 'sample', 'average_series', 'std_series', 'report', 'report_compact']
    )
    return PriceSeriesManager()
