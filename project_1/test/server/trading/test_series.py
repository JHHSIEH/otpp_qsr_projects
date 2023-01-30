from unittest import TestCase

import pandas as pd

from server.trading.series import PriceSeries


class TestPriceSeries(TestCase):
    def test__build_single_set(self):
        pd.set_option('display.max_columns', None)

        df = pd.DataFrame(
            {
                'ticker': ['AAPL', 'AAPL', 'AAPL', 'AAPL'],
                'price': [145.90, 145.93, 145.98, 145.94]
            },
            index=[pd.Timestamp('2023-01-27 19:50:00'), pd.Timestamp('2023-01-27 19:55:00'), pd.Timestamp('2023-01-27 20:00:00'), pd.Timestamp('2023-01-27 20:05:00')]
        )
        ps = PriceSeries()
        df2 = ps._build_single_set(df)
        print(df2)
