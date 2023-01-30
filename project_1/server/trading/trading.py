import concurrent.futures
from datetime import datetime
from multiprocessing import Process, Manager
from multiprocessing.connection import Connection
from time import sleep

from .collector import Collector
from .util import _convert_datetime_timezone, write
from .series import PriceSeries, create_PriceSeriesManager


class Trading:
    def __init__(self, conn, tickers, sampling):
        self.initial_raw_ticker_list = tickers
        self.tickers = self._create_proxy_list([])
        self.sampling = sampling
        self.collector = self._create_collector()
        self.data: PriceSeries = None
        self.conn: Connection = conn

    def run(self):
        price_series_manager = create_PriceSeriesManager()
        price_series_manager.start()
        self.data = price_series_manager.PriceSeries()

        self.initialize_data()
        self.update_report()

        p_poll = Process(target=self.run_poller, args=(self.data, self.tickers))
        p_listener = Process(target=self.run_listener, args=(self.data, self.tickers))

        p_poll.start()
        p_listener.start()
        print('Ready to accept requests')
        p_poll.join()
        p_listener.join()

    def run_poller(self, data, tickers):
        data.set_interval(self.sampling)

        while True:
            sleep(self.sampling * 60)
            with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
                latest_data = executor.map(self.collector.realtime, self.tickers)
            data.update(list(latest_data))

    def run_listener(self, data, tickers):
        while True:
            method_name, *args = self.conn.recv()

            try:
                method = getattr(self, method_name)
            except AttributeError:
                raise NotImplementedError(
                    "Class `{}` does not implement `{}`".format(Trading.__class__.__name__, method_name))

            try:
                response = method(*args)
                self.conn.send(response)
            except Exception as e:
                self.conn.send(e)

    @staticmethod
    def _create_collector():
        return Collector()

    @staticmethod
    def _create_proxy_list(items: list):
        manager = Manager()
        return manager.list(items)

    def latest_as_of(self, as_of_text=None):
        if as_of_text is None:
            as_of_datetime = datetime.now()
        else:
            as_of_datetime = _convert_datetime_timezone(as_of_text, 'UTC', 'Canada/Eastern')

        return self.data.snapshot(as_of_datetime)

    def update_report(self):
        report = self.data.report()
        write(report)
        return 'Report generated'

    def add_ticker(self, ticker):
        if not ticker:
            raise ValueError('please provide a ticker')
        if ticker in self.tickers:
            return
        if self._valid_ticker(ticker):
            self.tickers.append(ticker)
            data = self.collector.historical(ticker, self.sampling)
            self.data.add(ticker, data)
            self.data.build_single_symbol(ticker)
            return
        raise ValueError(f'invalid ticker: {ticker}')

    def delete_ticker(self, ticker):
        if not ticker:
            raise ValueError('please provide a ticker')
        if ticker in self.tickers:
            self.tickers.remove(ticker)
            self.data.delete(ticker)
            return
        raise ValueError('please provide a valid ticker')

    def _valid_ticker(self, ticker):
        return self.collector.lookup_symbol(ticker)

    def initialize_data(self):
        for ticker in self.initial_raw_ticker_list:
            self.add_ticker(ticker)
        self.data.build()
