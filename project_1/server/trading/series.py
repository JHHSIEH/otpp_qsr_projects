from multiprocessing.managers import BaseManager

import pandas as pd


class PriceSeries:
    def __init__(self):
        self._data = {}

    def add(self, symbol, data: pd.DataFrame):
        self._data.update({symbol: data})

    def delete(self, symbol):
        self._data.pop(symbol)

    def update(self, data):
        pass

    def build(self):
        pass

    def average_series(self):
        pass

    def std_series(self):
        pass

    def report(self):
        data = pd.concat([])
        data = data.sort_values(by=['datetime', 'ticker'])
        return data

    def _lastest_record_as_of(self, df, as_of):
        data = df.loc[df['datetime'] < as_of, ['ticker', 'price', 'signal']]
        if len(data) == 0:
            raise IndexError('Server has no data')
        return data

    def snapshot(self, as_of_datetime):
        data_list = [self._lastest_record_as_of(d, as_of_datetime) for d in self._data.values()]
        data = pd.concat(data_list)
        data = data.sort_values(by='datetime')
        return data


class PriceSeriesManager(BaseManager):
    pass


def create_PriceSeriesManager():
    PriceSeriesManager.register(
        'PriceSeries',
        PriceSeries,
        # exposed=['add', 'delete', 'build', 'sample', 'average_series', 'std_series', 'report', 'report_compact']
    )
    return PriceSeriesManager()
