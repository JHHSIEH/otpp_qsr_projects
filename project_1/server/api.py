from multiprocessing.connection import Connection
from flask import request, Flask, make_response


def create_app(conn: Connection):
    app = Flask(__name__)
    app.config['conn'] = conn

    @app.get('/data')
    def data():
        try:
            utc_as_of_datetime = request.args.get('as_of_datetime')
            app.config['conn'].send(('latest_as_of', utc_as_of_datetime))
            result = app.config['conn'].recv()
            return make_response(result, 200)
        except Exception as e:
            return make_response(e, 500)

    @app.post('/tickers')
    def tickers_post():
        try:
            ticker = request.form.get('ticker')
            app.config['conn'].send(('add_ticker', ticker))
            result = app.config['conn'].recv()
            if isinstance(result, Exception):
                raise result
            return '0'
        except ValueError:
            return '2'
        except Exception:
            return '1'

    @app.delete('/tickers/<ticker>')
    def tickers_delete(ticker):
        try:
            app.config['conn'].send(('delete_ticker', ticker))
            result = app.config['conn'].recv()
            if isinstance(result, Exception):
                raise result
            return '0'
        except ValueError:
            return '2'
        except Exception:
            return '1'

    @app.put('/report')
    def report():
        app.config['conn'].send(('update_report',))
        resp = app.config['conn'].recv()
        return resp

    return app
