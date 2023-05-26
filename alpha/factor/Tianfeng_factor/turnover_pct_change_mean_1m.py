from rqfactor import *
import rqdatac
from rqfactor.extension import *
import pandas as pd


def turnover_daily(order_book_ids, start_date, end_date):
    final = rqdatac.get_turnover_rate(order_book_ids, start_date=start_date, end_date=end_date, fields=None,
                                      expect_df=True).reset_index().pivot(columns='order_book_id', index='tradedate',
                                                                          values='today')
    return final.reindex(columns=order_book_ids)


turnover_daily = UserDefinedLeafFactor('turnover_daily', turnover_daily)


def main():
    return MA(turnover_daily, 20)
