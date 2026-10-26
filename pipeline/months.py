import pandas as pd


def shift(month, count=1):
    return str(pd.Period(month, freq='M') + count)


def window(month):
    return [shift(month, -2), shift(month, -1), month]
