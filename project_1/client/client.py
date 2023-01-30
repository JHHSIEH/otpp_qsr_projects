import requests


class Client:
    def __init__(self, server_address):
        self.server_address = f'http://{server_address}'

    def data(self, param):
        response = requests.get(f'{self.server_address}/data', params={'as_of_datetime': param})
        return response.text

    def delete(self, param):
        response = requests.delete(f'{self.server_address}/tickers/{param}')
        return response.text

    def add(self, param):
        response = requests.post(f'{self.server_address}/tickers', data={'ticker': param})
        return response.text

    def report(self):
        response = requests.put(f'{self.server_address}/report')
