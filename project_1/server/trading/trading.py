import concurrent.futures
from datetime import datetime
from multiprocessing import Process, Manager
from multiprocessing.connection import Connection
from time import sleep
import pytz

from server.trading.collector import Collector
from server.trading.out import Out
from server.trading.series import PriceSeries


class Trading:
    def __int__(self, conn: Connection, data, tickers, sampling):
        self.initial_raw_ticker_list = tickers
        self.tickers = self._create_proxy_list([])
        self.sampling = sampling
        self.collector = self._create_collector()
        self.data: PriceSeries = data
        self.conn: Connection = conn

    def run(self):
        self.initialize_data()
        self.update_report()

        p_poll = Process(target=self.run_poller, args=(data, self.tickers))
        p_listener = Process(target=self.run_listener, args=(data, self.tickers))

        p_poll.start()
        p_listener.start()
        p_poll.join()
        p_listener.join()

    def run_poller(self):
        while True:
            sleep(self.sampling)
            with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
                latest_data = executor.map(self.collector.realtime, self.tickers)
            self.data.update(latest_data)

    def run_listener(self):
        while True:
            method_name, *args = self.conn.recv()

            try:
                method = getattr(Trading, method_name)
            except AttributeError:
                raise NotImplementedError(
                    "Class `{}` does not implement `{}`".format(Trading.__class__.__name__, method_name))

            response = method(*args)
            self.conn.send(response)

    @staticmethod
    def _create_collector():
        return Collector()

    @staticmethod
    def _create_proxy_list(items: list):
        manager = Manager()
        return manager.list(items)

    @staticmethod
    def _convert_datetime_timezone(txt, tz1, tz2):
        format = '%Y-%m-%d-%H:%M'
        tz1 = pytz.timezone(tz1)
        tz2 = pytz.timezone(tz2)

        dt = datetime.strptime(txt, format)
        dt = tz1.localize(dt)
        dt = dt.astimezone(tz2)
        dt = dt.replace(tzinfo=None)
        return dt

    def latest_as_of(self, as_of_text):
        if as_of_text is None:
            as_of_datetime = datetime.now()
        else:
            as_of_datetime = self._convert_datetime_timezone(as_of_text, 'UTC', 'Canada/Eastern')

        return self.data.snapshot(as_of_datetime)

    def update_report(self):
        report = self.data.report()
        Out.write(report)

    def add_ticker(self, ticker):
        if not ticker:
            raise ValueError('please provide a ticker')
        if ticker in self.tickers:
            return
        if self._valid_ticker(ticker):
            self.tickers.append(ticker)
            data = self.collector.historical(ticker)
            self.data.add(ticker, data)
            return
        raise ValueError(f'invalid ticker: {ticker}')

    def delete_ticker(self, ticker):
        if not ticker:
            raise ValueError('please provide a ticker')
        if ticker in self.tickers:
            self.tickers.remove(ticker)
            self.data.delete(ticker)
        raise ValueError('please provide a valid ticker')

    def _valid_ticker(self, ticker):
        return self.collector.lookup_symbol(ticker)

    def initialize_data(self):
        for ticker in self.initial_raw_ticker_list:
            self.add_ticker(ticker)
        self.data.build()
