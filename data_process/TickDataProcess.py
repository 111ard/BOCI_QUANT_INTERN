import pandas as pd
import os
import rqdatac
import numpy as np
import json
import time
import warnings
import logging

#默认的warning级别，只输出warning以上的
#使用basicConfig()来指定日志级别和相关信息

logging.basicConfig(level=logging.DEBUG #设置日志输出格式
                    ,filename="BOCI_QUANT_INTERN/data_process/log/demo.log" #log日志输出的文件位置和文件名
                    ,filemode="w" #文件的写入格式，w为重新写入文件，默认是追加
                    ,format="%(asctime)s - %(name)s - %(levelname)-9s - %(filename)-8s : %(lineno)s line - %(message)s" #日志输出的格式
                    # -8表示占位符，让输出左对齐，输出长度都为8位
                    ,datefmt="%Y-%m-%d %H:%M:%S" #时间输出的格式
                    )


warnings.filterwarnings("ignore")
start_time_c = time.time()
rqdatac.init()
pwd = os.path.abspath(__file__)[:-len(os.path.basename(__file__))]
with open(os.path.join(pwd, 'config.json')) as f:
    data = json.load(f)

start_date = data['start_date']
end_date = data['end_date']
start_time = data['start_time'] * 1000
end_time = data['end_time'] * 1000
h5_dir_path = data['h5_dir_path']
stock_pool_path = data['stock_pool_path']
output_dir = data['output_dir']

stock_pool = pd.read_excel(stock_pool_path).证券代码.apply(lambda x: str(x).rjust(6, '0'))
dirs = os.listdir(h5_dir_path)
list1 = [x for x in stock_pool]
list2 = [x[0:6] for x in dirs]
stock_pool_final = list(set(list1) & set(list2))
all_stocks = []
for x in dirs:
    if x[0:6] in stock_pool_final:
        all_stocks.append(x)
all_trading_dates = rqdatac.get_trading_dates(start_date, end_date)
all_trading_dates = list(map(lambda x: x.strftime(format='%Y%m%d'), all_trading_dates))
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
        days_needed_to_append = []
        for i in func_list:
            try:
                variable_i = pd.read_csv(os.path.join(output_dir , 'variable_' + i + '.csv'), index_col=0)
                variable_i = variable_i.iloc[:-1,:]
                variable_i.index = variable_i.index.astype(str)
                d['variable {}'.format(i)] = variable_i
            except:
                d['variable {}'.format(i)] = pd.DataFrame(columns=all_stocks)  # , index=all_trading_dates[0])

            days_needed_to_append_temp = list(set(all_trading_dates) - set(list(d['variable {}'.format(i)].index)))
            if len(days_needed_to_append_temp) == 0:
                logging.info(i + ' 已经更新到最新')
            days_needed_to_append += days_needed_to_append_temp
        days_needed_to_append = list(set(days_needed_to_append))

        for stock in all_stocks:
            for date in days_needed_to_append:
                try:
                    data = pd.read_hdf(os.path.join(h5_dir_path , stock), key=date)
                except:
                    continue
                for i in func_list:
                    d["variable {}".format(i)].loc[date, stock] = getattr(Tickdata, i)(data, data)
                    logging.info(i+' '+ date+' '+stock+ ' 已经完成')
        for i in func_list:
            d['variable {}'.format(i)].sort_index().to_csv(os.path.join(output_dir ,  'variable_'  +i + '.csv'))

    def find_calculate_methods(self):
        return (
            list(filter(lambda m: not m.startswith("_") and callable(getattr(self, m)) and m.startswith("calculate"),
                        dir(self))))

    def calculate_VWAP(self, data):
        """

        :param start_time:
        :param end_time:
        :return:
        """
        data['volume_change'] = data.volume - data.volume.shift(1)
        data = data.loc[(data.time >= start_time) & (data.time <= end_time), :]
        VWAP = (data.volume_change * (data.high / 2 + data.low / 2)).sum() / data.volume_change.sum() / 10000

        return VWAP

    def calculate_TWAP(self, data):
        """

        :param start_time:
        :param end_time:
        :return:
        """

        data['volume_change'] = data.volume - data.volume.shift(1)
        data = data.loc[(data.time >= start_time) & (data.time <= end_time), :]
        TWAP = ((data.high / 2 + data.low / 2)).sum() / len(data) / 10000

        return TWAP

    def calculate_
Tickdata().data_process()
end_time_c = time.time()

print(end_time_c - start_time_c)

