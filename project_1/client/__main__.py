import sys
from cmd import Cmd
import signal
import argparse
from typing import Union, Iterable
import pandas as pd

from client import Client


debug_mode = False


class MyCmd(Cmd):
    def __init__(self, client):
        super().__init__()
        signal.signal(signal.SIGINT, handler=self._ctrl_c_handler)
        self.prompt = "> "
        self.client: Client = client

    def _ctrl_c_handler(self, signal, frame):
        raise SystemExit()

    def default(self, line):
        print(f'Unknown command: {line}')

    def do_data(self, args):
        try:
            as_of_datetime = parse_args(args, (0, 1))
            result = self.client.data(as_of_datetime)
            print(format_results_for_print(result))
        except Exception as e:
            if debug_mode:
                traceback = sys.exc_info()[2]
                raise e.with_traceback(traceback)
            print(f'Error: {e}')
            return

    def do_delete(self, args):
        try:
            ticker = parse_args(args, 1)
            print(self.client.delete(ticker))
        except Exception as e:
            if debug_mode:
                traceback = sys.exc_info()[2]
                raise e.with_traceback(traceback)
            print(f'Error: {e}')
            return

    def do_add(self, args):
        try:
            ticker = parse_args(args, 1)
            print(self.client.add(ticker))
        except Exception as e:
            if debug_mode:
                traceback = sys.exc_info()[2]
                raise e.with_traceback(traceback)
            print(f'Error: {e}')
            return

    def do_report(self, args):
        try:
            parse_args(args, 0)
            print(self.client.report())
        except Exception as e:
            if debug_mode:
                traceback = sys.exc_info()[2]
                raise e.with_traceback(traceback)
            print(f'Error: {e}')
            return


def format_results_for_print(s):
    try:
        f = pd.read_json(s, orient='split').to_string(index=False, header=False)
        return f
    except Exception as e:
        print(f's: {s}')


def parse_args(args, expected_args: Union[int, Iterable]):
    if args.strip() != '':
        args_list = args.split(' ')
    else:
        args_list = list()
    if not hasattr(expected_args, '__contains__'):
        expected_args = tuple([expected_args])
    if len(args_list) in expected_args:
        if len(args_list) > 1:
            return args_list
        elif len(args_list) == 1:
            return args_list[0]
        else:
            return
    raise TypeError(f'{expected_args} arguments expected')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default='127.0.0.1:8000', type=str, dest='server_address')
    args = parser.parse_args()

    client = Client(args.server_address)
    app = MyCmd(client)
    app.cmdloop('Enter a command to do something: data, add, delete, report')
