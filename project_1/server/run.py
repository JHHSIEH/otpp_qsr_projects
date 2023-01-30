from multiprocessing import Pipe, Process
from multiprocessing.connection import Connection

import click

from api import create_app
from trading import Trading


def run_app(conn, port):
    app = create_app(conn)
    app.run(port=port, use_reloader=False)


def run_trading(conn: Connection, tickers: str, sampling: int):
    trading = Trading(conn, tickers.split(','), sampling)
    trading.run()


@click.command()
@click.option('--tickers', default='AAPL,MSFT,TOST')
@click.option('--port', default=8000)
@click.option('--sampling', default=5)
def get_options(tickers, port, sampling):
    sampling_periods = (5, 15, 30, 60)
    if sampling not in sampling_periods:
        print(f'sampling period must be one of {sampling_periods}')
        raise SystemExit()
    return tickers, port, sampling


if __name__ == '__main__':
    tickers, port, sampling = get_options(standalone_mode=False)

    api_conn, trading_conn = Pipe()

    p_app = Process(target=run_app, args=(api_conn, port))
    p_trading = Process(target=run_trading, args=(trading_conn, tickers, sampling))

    p_app.start()
    p_trading.start()
    p_app.join()
    p_trading.join()
