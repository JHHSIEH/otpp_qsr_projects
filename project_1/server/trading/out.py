import csv
import pandas as pd


class Out:
    @staticmethod
    def write(df: pd.DataFrame):
        df.to_csv('report.csv', index=False)
