import pandas as pd
import os
import rqdatac
import numpy as np
import json
import time

start_time_c = time.time()
rqdatac.init()
pwd = os.path.abspath(__file__)[:-len(os.path.basename(__file__))]
with open(pwd + 'config.json') as f:
    data = json.load(f)

start_date = data['start_date']
end_date = data['end_date']
start_time = data['start_time']*1000
end_time = data['end_time']*1000
h5_dir_path = data['h5_dir_path']
stock_pool_path = data['stock_pool_path']
output_dir = data['output_dir']
# func_list = data['func_list']

stock_pool = pd.read_excel(stock_pool_path).证券代码.apply(lambda x: str(x).rjust(6,'0'))
dirs = os.listdir(h5_dir_path)
list1 = [x for x in stock_pool]
list2 = [x[0:6] for x in dirs]
stock_pool_final = list(set(list1) & set(list2))
all_stocks =[]
for x in dirs:
    if x[0:6] in stock_pool_final:
        all_stocks.append(x)
all_stocks = all_stocks[:1]
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
        func_list = self.find_calculate_methods()
        d = {}
        for i in func_list:
            d['variable{}'.format(i)] = pd.DataFrame(columns = all_stocks, index = all_trading_dates)
        for stock in all_stocks:
            for date in all_trading_dates:
                try:
                    data = pd.read_hdf(h5_dir_path + stock, key=date.strftime('%Y%m%d'))
                except:
                    continue
                for i in func_list:
                    d["variable{}".format(i)].loc[date,stock] =getattr(Tickdata,i)(data,data)
                    # print(getattr(Tickdata,i)(data,data))
        #d.keys = func_list
        for key in list(d.keys()):
            d[key].to_csv(output_dir + key + '.csv')

        return d

    def find_calculate_methods(self):
        return (list(filter(lambda m: not m.startswith("_") and callable(getattr(self, m)) and m.startswith("calculate"),
                            dir(self))))

    def calculate_VWAP(self,  data):
        """

        :param start_time:
        :param end_time:
        :return:
        """
        data['volume_change'] = data.volume - data.volume.shift(1)
        data = data.loc[(data.time >= start_time) & (data.time <= end_time),:]
        VWAP = (data.volume_change * (data.high / 2 + data.low / 2)).sum() / data.volume_change.sum() / 10000


        return VWAP

    def calculate_TWAP(self,  data):
        """

        :param start_time:
        :param end_time:
        :return:
        """

        data['volume_change'] = data.volume - data.volume.shift(1)
        data = data.loc[(data.time >= start_time) & (data.time <= end_time),:]
        TWAP = ((data.high/2+data.low/2)).sum()/len(data)/10000

        return TWAP


#func_list = [Tickdata().daily_VWAP,
#             Tickdata().daily_TWAP]
Tickdata().data_process()
end_time_c = time.time()

print(end_time_c -start_time_c)