from rqfactor import *
import rqdatac
from rqfactor.extension import *
import pandas as pd

def ep_1y_percentile(order_book_ids,start_date,end_date):
    bp_lyr = rqdatac.get_factor(order_book_ids, 'ep_ratio_lf', start_date=start_date, end_date=end_date, universe=None,expect_df=True)
    bp_lyr = bp_lyr.unstack('order_book_id').reset_index(drop =True)
    bp_lyr.index = pd.to_datetime(rqdatac.get_trading_dates(start_date, end_date))
    #bp_lyr1 = bp_lyr.reset_index()
    #bp_lyr2 = bp_lyr1.set_index('date',drop=True)
    bp_lyr.columns=bp_lyr.columns.droplevel(0)
  #  bp_lyr2.columns = order_book_ids
  #  bp_lyr2['bp_lyr_percentile'] = bp_lyr2.rolling(5).apply(lambda x: scipy.stats.percentileofscore(x, x.iloc[-1]))
   # bp_lyr2 = bp_lyr2.loc[:,['bp_lyr_percentile']]
    return bp_lyr.reindex(columns=order_book_ids)

ep_1Y_Percentile = UserDefinedLeafFactor('ep_1Y_percentile',ep_1y_percentile)

#test = bp_3y_percentile(order_book_ids = ['000001.XSHE'],start_date='20230101',end_date='20230201')

def main():
    return TS_RANK(ep_1y_percentile,250)
