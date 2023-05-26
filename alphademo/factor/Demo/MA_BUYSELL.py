from rqfactor import *
from rqfactor.extension import *
from rqfactor.notebook import *
import rqdatac
import numpy as np
import pandas as pd

rqdatac.init()

#定义自定义因子
def buy_volume(order_book_ids,start_date,end_date):
    return rqdatac.get_capital_flow(order_book_ids,start_date,end_date).buy_volume.unstack('order_book_id').reindex(columns=order_book_ids,index =pd.to_datetime(rqdatac.get_trading_dates(start_date,end_date)))

def sell_volume(order_book_ids,start_date,end_date):
    return rqdatac.get_capital_flow(order_book_ids,start_date,end_date).sell_volume.unstack('order_book_id').reindex(columns=order_book_ids,index =pd.to_datetime(rqdatac.get_trading_dates(start_date,end_date)))

BUY_VOLUME = UserDefinedLeafFactor('BUY_VOLUME',buy_volume)
SELL_VOLUME = UserDefinedLeafFactor('SELL_VOLUME',sell_volume)

f = DELTA(MA(BUY_VOLUME-SELL_VOLUME,13)/IF(MA(ABS(BUY_VOLUME-SELL_VOLUME),13) !=0,MA(ABS(BUY_VOLUME-SELL_VOLUME),13),np.nan),3)

def main():
    return f
#d1='20190101'
#d2='20200101'

#df = execute_factor(main(),rqdatac.index_components('000300.XSHG', d1),d1,d2)