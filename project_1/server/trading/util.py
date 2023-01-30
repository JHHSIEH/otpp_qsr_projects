from datetime import datetime

import pandas as pd
import pytz


def write(df: pd.DataFrame):
    df.to_csv('report.csv')


def _convert_datetime_timezone(txt, tz1, tz2):
    format = '%Y-%m-%d-%H:%M'
    tz1 = pytz.timezone(tz1)
    tz2 = pytz.timezone(tz2)

    dt = datetime.strptime(txt, format)
    dt = tz1.localize(dt)
    dt = dt.astimezone(tz2)
    dt = dt.replace(tzinfo=None)
    return dt