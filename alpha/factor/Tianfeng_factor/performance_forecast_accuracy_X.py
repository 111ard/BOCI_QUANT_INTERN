from rqfactor import *
import rqdatac
from rqfactor.extension import *
import pandas as pd


def Floor(order_book_ids, start_date, end_date):
    test = rqdatac.performance_forecast(order_book_ids)
    test = test.reset_index()
    test = test.drop_duplicates(subset=['order_book_id', 'end_date'])
    test = test.reset_index().pivot(columns='order_book_id', index='end_date', values='forecast_np_floor')
    date_range = rqdatac.get_trading_dates(start_date=test.index.min(), end_date=test.index.max(),
                                           market='cn')  # freq="D"表示按天，可以按分钟，月，季度，年等
    test = test.reindex(index=pd.to_datetime(date_range))
    test = test.fillna(method='ffill', limit=15)
    return test.loc[start_date:end_date, :].reindex(columns=order_book_ids)


def Ceiling(order_book_ids, start_date, end_date):
    test = rqdatac.performance_forecast(order_book_ids)
    test = test.reset_index()
    test = test.drop_duplicates(subset=['order_book_id', 'end_date'])
    test = test.reset_index().pivot(columns='order_book_id', index='end_date', values='forecast_np_ceiling')
    date_range = rqdatac.get_trading_dates(start_date=test.index.min(), end_date=test.index.max(),
                                           market='cn')  # freq="D"表示按天，可以按分钟，月，季度，年等
    test = test.reindex(index=pd.to_datetime(date_range))
    test = test.fillna(method='ffill', limit=15)
    return test.loc[start_date:end_date, :].reindex(columns=order_book_ids)


Floor = UserDefinedLeafFactor('Floor', Floor)
Ceiling = UserDefinedLeafFactor('Ceiling', Ceiling)


def main():
    return -2 * ABS(Floor - Ceiling)/(Floor + Ceiling)
