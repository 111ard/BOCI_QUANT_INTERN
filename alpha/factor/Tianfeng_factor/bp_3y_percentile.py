from rqfactor import *
import rqdatac
from rqfactor.extension import *
import pandas as pd



rqdatac.init()
#pwd = os.path.abspath(__file__)[:-13]
#all_in = rqdatac.all_instruments()
#stock_pool = pd.read_excel('E:\BOCI_Intern/alpha\stock_pool.xlsx')
#all_in = pd.merge(stock_pool, all_in, left_on='证券名称', right_on='symbol',how = 'right')
# 此处dropna会去掉1、ST的股票以及2、米筐中不存在其数据的股票
#all_in = all_in.dropna(subset = ['证券代码','证券名称'])
# 最终用列表储存所有的可用股票代码
#stock_pool = list(all_in['order_book_id'])
def bp_3y_percentile(order_book_ids,start_date,end_date):
    bp_lyr = rqdatac.get_factor(order_book_ids, 'book_to_market_ratio_lyr', start_date=start_date, end_date=end_date, universe=None,expect_df=True)
    bp_lyr = bp_lyr.unstack('order_book_id').reset_index(drop =True)
    bp_lyr.index = pd.to_datetime(rqdatac.get_trading_dates(start_date, end_date))
    #bp_lyr1 = bp_lyr.reset_index()
    #bp_lyr2 = bp_lyr1.set_index('date',drop=True)
    bp_lyr.columns=bp_lyr.columns.droplevel(0)
  #  bp_lyr2.columns = order_book_ids
  #  bp_lyr2['bp_lyr_percentile'] = bp_lyr2.rolling(5).apply(lambda x: scipy.stats.percentileofscore(x, x.iloc[-1]))
   # bp_lyr2 = bp_lyr2.loc[:,['bp_lyr_percentile']]
    return bp_lyr.reindex(columns=order_book_ids)

BP_3Y_Percentile = UserDefinedLeafFactor('BP_3Y_percentile',bp_3y_percentile)

#test = bp_3y_percentile(order_book_ids = ['000001.XSHE'],start_date='20230101',end_date='20230201')

def main():
    return TS_RANK(BP_3Y_Percentile,750)
#factor1 = main()
#df = execute_factor(factor1, stock_pool, start_date='20230101', end_date='20230202')