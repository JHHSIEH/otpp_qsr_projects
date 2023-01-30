from multiprocessing import Pipe, Process
from multiprocessing.connection import Connection

import click

from api import create_app
from server.trading.series import PriceSeries, create_PriceSeriesManager
from trading import Trading


@click.command()
@click.option('--tickers', default='AAPL,MSFT,TOST')
@click.option('--port', default=8000)
@click.option('--sampling', default=5)
def run(tickers, port, sampling):
    sampling_periods = (5, 15, 30, 60)
    if sampling not in sampling_periods:
        print(f'sampling period must be one of {sampling_periods}')
        raise SystemExit()

    price_series_manager = create_PriceSeriesManager()
    price_series_manager.start()
    data = price_series_manager.PriceSeries()

    api_conn, trading_conn = Pipe()
    p_app = Process(target=run_app, args=(api_conn, data, port))
    p_trading = Process(target=run_trading, args=(trading_conn, data, tickers, sampling))

    p_app.start()
    p_trading.start()
    p_app.join()
    p_trading.join()


def run_app(conn, data, port):
    app = create_app(conn, data)
    app.run(port=port, use_reloader=False)


def run_trading(conn: Connection, data, tickers: list, sampling: int):
    trading = Trading(conn, data, tickers.split(','), sampling)
    trading.run()


if __name__ == '__main__':
    run()
