from rqfactor import *
import rqdatac
from rqfactor.extension import *
import pandas as pd
import statsmodels.api as sm
from rqfactor.extension import *
from datetime import *
from statsmodels.regression.rolling import RollingOLS
import statsmodels
import warnings

warnings.filterwarnings("ignore")

rqdatac.init()

all_in = rqdatac.all_instruments()
stock_pool = pd.read_excel('E:\BOCI_Intern/alpha\stock_pool.xlsx')
all_in = pd.merge(stock_pool, all_in, left_on='证券名称', right_on='symbol',how = 'right')

all_in = all_in.dropna(subset = ['证券代码','证券名称'])
# 最终用列表储存所有的可用股票代码
stock_pool = list(all_in['order_book_id'])

# 一下用于计算每个交易日的三因子
def FF_calculation_per_day(df):
    df['HML'] = 'M'
    df.loc[df.book_to_market_ratio_lyr >= df.book_to_market_ratio_lyr.quantile(0.7), 'HML'] = 'H'
    df.loc[df.book_to_market_ratio_lyr <= df.book_to_market_ratio_lyr.quantile(0.3), 'HML'] = 'L'
    df['SMB'] = 'S'
    df.loc[df.market_cap_3 >= df.market_cap_3.quantile(0.5), 'SMB'] = 'B'
    df['FF'] = df.SMB + '/' + df.HML
    df = df.dropna()
    FAMA_factor_return = df.groupby('FF').apply(lambda x: sum(x['return'] * x.market_cap_3) / x.market_cap_3.sum())
    HML = (FAMA_factor_return['S/H'] + FAMA_factor_return['B/H']) / 2 - (
                FAMA_factor_return['S/L'] + FAMA_factor_return['B/L']) / 2
    SMB = (FAMA_factor_return['S/H'] + FAMA_factor_return['S/M'] + FAMA_factor_return['S/L']) / 3 - (
                FAMA_factor_return['B/H'] + FAMA_factor_return['B/M'] + FAMA_factor_return['B/L']) / 3
    return HML, SMB


def regression_specificity(df):
    #df1 = df.dropna()
    x = df.loc[:,['HML','SMB','benchmark']]  # 输入
    y = df.loc[:,['return']]
    result = statsmodels.regression.rolling.RollingOLS(y,x,20).fit()
   # result = model.fit()  # 模型拟合
    R2 =result.rsquared
    return R2


def FF_3factor_Specificity(order_book_ids,start_date, end_date):
    #global final
    start_date1 = pd.to_datetime(start_date) + timedelta(days=-40)
    market_cap = rqdatac.get_factor(order_book_ids, 'market_cap_3', start_date=start_date1, end_date=end_date,
                                    universe=None, expect_df=True)
    close = rqdatac.get_price(order_book_ids, start_date=start_date1, end_date=end_date, frequency='1d', fields=None,
                              adjust_type='pre', skip_suspended=False, market='cn', expect_df=True, time_slice=None)
    bm = rqdatac.get_factor(order_book_ids, 'book_to_market_ratio_lyr', start_date=start_date1, end_date=end_date,
                            universe=None, expect_df=True)
    all_data = bm.join(close)
    all_data = all_data.join(market_cap)
    all_data['return'] = all_data.close / all_data.prev_close - 1
    benchmark = rqdatac.get_price('000905.XSHG', start_date=start_date1, end_date=end_date, frequency='1d',
                                  fields=None, adjust_type='pre', skip_suspended=False, market='cn', expect_df=True,
                                  time_slice=None)
    benchmark = benchmark.reset_index().set_index('date')
    final_calculation = all_data.groupby('date').apply(FF_calculation_per_day)
    # 以上计算了每天的FF因子
    LAST = final_calculation.apply(pd.Series)
    LAST.columns = ['HML', 'SMB']
    benchmark['benchmark'] = benchmark.close / benchmark.prev_close - 1
    benchmark = benchmark.loc[:, 'benchmark']
    # 计算每天的return
    final = LAST.join(benchmark)
    final = all_data.reset_index().set_index('date').loc[:,['return','order_book_id']].join(final)
  #  final['test'] = final.groupby('order_book_id').apply(regression_specificity)
    final = final.dropna()
    final = 1- final.groupby('order_book_id').apply(regression_specificity).apply(pd.Series)
    final = final.reset_index().pivot(index='date', columns='order_book_id', values=0).loc[start_date:,:]
    #final = final.reset_index
    #final = final.pivot(index = date, columns = order_book_id)
    # 调整格式
    #per_day_specificity = per_day_specificity.T
    #per_day_specificity.columns = order_book_ids
    #per_day_specificity.index = [end_date]
    return final.reindex(columns = order_book_ids)#final.groupby('order_book_id').apply(regression_specificity).T.dropna().loc[start_date:,:]


#FF_3factor_Specificity(order_book_ids=stock_pool, start_date='20230101', end_date='20230201')
FF_3factor_Specificity = UserDefinedLeafFactor('FF_3factor_Specificity', FF_3factor_Specificity)

def main():
    return FF_3factor_Specificity

