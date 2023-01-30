from unittest import TestCase

import pandas as pd

from server.trading import Trading
from server.trading.series import PriceSeries


class MockTrading(Trading):
    def _create_proxy_list(self, input):
        return input

    def _create_collector(self):
        pass


class TestTrading(TestCase):
    def test_run_poller(self):
        fake_conn = 'fake_conn'
        tickers = 'fake_ticker_1,fake_ticker_2,fake_ticker_3'
        sampling = 5
        t = Trading(fake_conn, tickers, sampling)

        df = pd.DataFrame(
            {
                'ticker': ['AAPL', 'AAPL', 'AAPL', 'AAPL'],
                'price': [145.90, 145.93, 145.98, 145.94]
            },
            index=[pd.Timestamp('2023-01-27 19:50:00'), pd.Timestamp('2023-01-27 19:55:00'), pd.Timestamp('2023-01-27 20:00:00'), pd.Timestamp('2023-01-27 20:05:00')]
        )
        ps = PriceSeries()
        ps.add('AAPL', df)
        ps.build()

        t.run_poller(ps, tickers)
