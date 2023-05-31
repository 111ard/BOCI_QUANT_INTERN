import pandas as pd
import os
import rqdatac
import numpy as np
import json
rqdatac.init()
pwd = os.path.abspath(__file__)[:-len(os.path.basename(__file__))]
with open(pwd + 'config.json') as f:
    data = json.load(f)

start_date = data['start_date']
end_date = data['end_date']
start_time = data['start_time']
end_time = data['end_time']
h5_dir_path = data['h5_dir_path']

stock_pool = pd.read_excel(r'E:\BOCI_QUANT_INTERN\data_process\stock_pool.xlsx').证券代码.apply(lambda x: str(x).rjust(6,'0'))
dirs = os.listdir(h5_dir_path)
list1 = [x for x in stock_pool]
list2 = [x[0:6] for x in dirs]
stock_pool_final = list(set(list1) & set(list2))
final =[]
for x in dirs:
    if x[0:6] in stock_pool_final:
        final.append(x)

all_trading_dates = rqdatac.get_trading_dates(start_date, end_date)
start_date = all_trading_dates[0]
end_date = all_trading_dates[-1]
print(all_trading_dates)


class Tickdata():
    """
    用于处理得到给定日期和时间段的回测值
    """
    def __init__(self):
        """

        """


    def data_process(self):
        """

        :return:
        """
        for stock in final:
            d

    def daily_VWAP(self,  start_time, end_time):
        """

        :param start_time:
        :param end_time:
        :return:
        """

        start_time = start_time * 1000
        end_time = end_time * 1000
        k = []
        for stock in final:
            vwap_per_stock = []
            for date in all_trading_dates:
                try:
                    df = pd.read_hdf(h5_dir_path + stock, key=date.strftime('%Y%m%d'))
                except:
                    vwap_per_stock.append(np.nan)
                    continue
                df['volume_change'] = df.volume - df.volume.shift(1)
                df = df.loc[(df.time >= start_time) & (df.time <= end_time),:]
                VWAP = (df.volume_change * (df.high / 2 + df.low / 2)).sum() / df.volume_change.sum() / 10000
                vwap_per_stock.append(VWAP)
            k.append(pd.DataFrame(vwap_per_stock,columns =[stock],index = all_trading_dates).T)

        return pd.concat(k).T

    def daily_TWAP(self,  start_time, end_time):
        """

        :param start_time:
        :param end_time:
        :return:
        """

        start_time = start_time * 1000
        end_time = end_time * 1000
        k = []
        for stock in final:
            twap_per_stock = []
            for date in all_trading_dates:
                try:
                    df = pd.read_hdf("D:\stock/"+stock, key=date.strftime('%Y%m%d'))
                except:
                    twap_per_stock.append(np.nan)
                    continue
                df['volume_change'] = df.volume - df.volume.shift(1)
                df = df.loc[(df.time >= start_time) & (df.time <= end_time),:]
                TWAP = ((df.high/2+df.low/2)).sum()/len(df)/10000
                twap_per_stock.append(TWAP)
            k.append(pd.DataFrame(twap_per_stock,columns =[stock],index = all_trading_dates).T)

        return pd.concat(k).T


func_list = [Tickdata().daily_VWAP(start_time=start_time, end_time=end_time),
             Tickdata().daily_TWAP(start_time=start_time, end_time=end_time)]

d = {}
for function in func_list:
    d['{}'.format()]=function

