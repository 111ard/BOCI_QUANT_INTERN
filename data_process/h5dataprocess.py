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
start_time = data['start_time']*1000
end_time = data['end_time']*1000
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
# print(all_trading_dates)


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
        d = {}
        for i in range(len(func_list)):
            d['variable{}'.format(i)] = pd.DataFrame(columns = final, index = all_trading_dates)
        for stock in final:
            for date in all_trading_dates:
                try:
                    data = pd.read_hdf(h5_dir_path + stock, key=date.strftime('%Y%m%d'))
                except:
                    continue
                for i,function in enumerate(func_list):
                    d["variable{}".format(i)].loc[date,stock] = function(data)
                    # print(function(data))
                # print(d)
                # print(date)

        d.keys = func_name_list

        return d

    def daily_VWAP(self,  data):
        """

        :param start_time:
        :param end_time:
        :return:
        """
        data['volume_change'] = data.volume - data.volume.shift(1)
        data = data.loc[(data.time >= start_time) & (data.time <= end_time),:]
        VWAP = (data.volume_change * (data.high / 2 + data.low / 2)).sum() / data.volume_change.sum() / 10000


        return VWAP

    def daily_TWAP(self,  data):
        """

        :param start_time:
        :param end_time:
        :return:
        """

        data['volume_change'] = data.volume - data.volume.shift(1)
        data = data.loc[(data.time >= start_time) & (data.time <= end_time),:]
        TWAP = ((data.high/2+data.low/2)).sum()/len(data)/10000

        return TWAP

func_name_list = ['vwap','twap']
func_list = [Tickdata().daily_VWAP,
             Tickdata().daily_TWAP]

test = Tickdata().data_process()