import rqdatac
import numpy as np
import pandas as pd
from sympy import *
from functools import partial
import datetime as dt
import matplotlib.pyplot as plt
import warnings
import math

warnings.filterwarnings("ignore")
rqdatac.init()

class CalculatePerformance():
    """
    计算业绩指标的类，包括成立至今收益，年化收益，最大回撤，年化波动率，夏普
    """

    def __init__(self, netValuelist, valueFrequency):
        """
        初始化
        :param netValuelist:Series，收益序列,其中索引必须为datetime格式且升序排列
        :param valueFrequency:float，序列的频率，用于计算年化波动率
        """
        self.netValuelist = netValuelist  # 自有化净值序列
        self.valueFrequency = valueFrequency  # 自有化序列的频率

    def returnRate(self):
        """
        计算策略总收益率
        :return:totalReturn, float：总收益率
        """
        totalReturn = self.netValuelist[len(self.netValuelist) - 1] - 1  # 计算区间总的收益

        return totalReturn

    def annualReturn(self):
        """
        计算年化收益率
        :param colName:str, 净值序列列名称
        :return: annualReturn, float:年化收益率
        """
        operatingYears = float(((self.netValuelist.index[-1] - self.netValuelist.index[0])).days / 365)  # 计算区间年数长度
        annualReturn = (self.netValuelist[len(self.netValuelist) - 1]) ** (1 / operatingYears) - 1  # 计算年化收益率

        return annualReturn

    def volatility(self):
        """
        计算年化波动率
        :param colName: str, 净值序列列名称
        :return: annualizedVolatility float, 年化波动率
        """
        self.netValuelist['return'] = self.netValuelist.shift() / self.netValuelist - 1  # 通过净值序列计算回报
        annualizedVolatility = np.std(self.netValuelist['return']) * math.sqrt(self.valueFrequency)  # 计算年化波动率

        return annualizedVolatility

    def maxDrawdown(self):
        """
        计算最大回撤指标
        :param colName: str, 净值序列列名称
        :return:maxDrawdown float, 最大回撤值
        """
        maxDrawdown = (1 - self.netValuelist / self.netValuelist.cummax()).max()  # 计算最大回撤

        return maxDrawdown


def summaryOutput(originalSequence, valueFrequency, risk_free_rate):
    """
    将最后的业绩结果汇总进行保存
    """
    summary = pd.DataFrame({'总收益率': [], '年化收益率': [], '年化波动率': [], '最大回撤': []})  # 创建空的dataframe
    # 对于各个策略或指数，计算各种业绩指标，得到summary
    columnNamelist = originalSequence.columns
    for strategyName in columnNamelist:
        summaryTemp = pd.DataFrame(
            {'总收益率': [CalculatePerformance(originalSequence[strategyName],
                                           valueFrequency).returnRate()],
             '年化收益率': [CalculatePerformance(originalSequence[strategyName],
                                            valueFrequency).annualReturn()],
             '年化波动率': [CalculatePerformance(originalSequence[strategyName],
                                            valueFrequency).volatility()],
             '最大回撤': [CalculatePerformance(originalSequence[strategyName],
                                           valueFrequency).maxDrawdown()]})
        summary = summary.append(summaryTemp)  # 向summary中添加一行
    summary.index = columnNamelist  # 索引设置为列名
    summary['夏普'] = (summary['年化收益率'] - risk_free_rate) / summary['年化波动率']  # 计算夏普比，此处假设无风险利率为1%
    # summary.总收益率 = f'{summary.总收益率:.2%}'
    summary.总收益率 = summary.总收益率.apply(lambda x: "%.2f%%" % (x * 100))
    summary.年化收益率 = summary.年化收益率.apply(lambda x: "%.2f%%" % (x * 100))
    summary.年化波动率 = summary.年化波动率.apply(lambda x: "%.2f%%" % (x * 100))
    summary.最大回撤 = summary.最大回撤.apply(lambda x: "%.2f%%" % (x * 100))
    summary.夏普 = summary.夏普.apply(lambda x: '%.2f' % x)
    return summary

def daily_return(test, size, n):
    x = symbols('x')
    test = test.reset_index()
    xu = test.loc[:size, :].index
    yu = test.loc[:size, :].open
    reg = np.polyfit(xu, yu, n)
    f = ''
    for i in range(len(reg)):
        f = f + str(reg[i]) + '*x**' + str(len(reg) - i - 1) + '+'
    f = f[:-6]
    expr = sympify(f)
    f2 = lambdify(x, diff(expr, x), 'numpy')
    test.loc[size:size+50, '一阶导数'] = f2(np.arange(size, size+51, 1))
    f3 = lambdify(x, diff(expr, x, 2), 'numpy')
    test.loc[size:size+50, '二阶导数'] = f3(np.arange(size, size+51, 1))
    test['return'] = test.open / test.open.shift(1) - 1
    test.loc[:size, '信号'] = '0'
    test.loc[(test.一阶导数 >= 0) & (test.二阶导数 >= 0), '信号'] = '多'
    test.loc[(test.一阶导数 <= 0) & (test.二阶导数 <= 0), '信号'] = '空'
    test.loc[(test.一阶导数 <= 0) & (test.二阶导数 >= 0), '信号'] = '平'
    test.loc[(test.一阶导数 >= 0) & (test.二阶导数 <= 0), '信号'] = '平'
    t = 0
    last = '平'
    for i in range(len(test)):

        if pd.isnull(test.信号.iloc[i]):
            break

        now = test.信号.iloc[i]
        if last != now and now != '平':
            t = t + 1
            start_indicator = i
            signal = now

        if last != now and now == '平':
            t = t + 1
            end_indicator = i
        # print('平了')
        last = now
        #  print(t)
        if t == 2:
            # print(last,now)
            # print(start_indicator,i)
            break
    if t == 2:
        try:
            df = test.loc[start_indicator:end_indicator, 'return']
        except:
            df = test.loc[start_indicator:start_indicator + 16, 'return']
        return np.cumprod(df + 1).iloc[-1] - 1
    if t == 1 and signal == '多':
        return np.cumprod(test.loc[start_indicator:, 'return'] + 1).iloc[-1] - 1
    if t == 1 and signal == '空':
        return np.cumprod(-test.loc[start_indicator:, 'return'] + 1).iloc[-1] - 1
    if t == 0:
        return 0


#original_data = rqdatac.get_price('000905.XSHG',start_date = '20170101',end_date = '20230101',frequency = '1m')
#original_data = original_data.reset_index()
#original_data['date'] = original_data['datetime'].apply(lambda x: x.strftime(format ='%Y%m%d'))
def calculate_return(start_date,end_date,n,size,stock):
    original_data = rqdatac.get_price(stock, start_date=start_date, end_date=end_date, frequency='1m')
    original_data = original_data.reset_index()
    original_data['date'] = original_data['datetime'].apply(lambda x: x.strftime(format='%Y%m%d'))
    daily_return_partial  = partial(daily_return, n = n,size =size)
    return_list = original_data.groupby('date').apply(daily_return_partial)
    return np.cumprod(1+return_list)
    #total_return = np.cumprod(1+return_list -0.0001).iloc[-1] -1
    #start_date = dt.datetime.strptime(start_date, "%Y%m%d").date()  ##datetime.date(2018, 1, 6)
    #end_date = dt.datetime.strptime(end_date, "%Y%m%d").date()  ##datetime.date(2018, 1, 9)
    # （2）计算两个日期date的天数差
    #Days = (end_date - start_date).days
    #return (total_return+1)**(365/Days) -1

# test = calculate_return(start_date = '20200101',end_date = '20210101',n =5,size =50,stock ='000905.XSHG')


n_list = [3, 4, 5, 6, 7]
size_list = [30, 40, 50, 100, 150, 180]
start_date = '20190101'
end_date = '20230101'
# n_list = [3,4]
# size_list = [30,40]
#final_df = pd.DataFrame(columns=size_list, index=n_list)
nv_df = pd.DataFrame(index = rqdatac.get_trading_dates(start_date =start_date , end_date = end_date))
for n in n_list:
    for size in size_list:

        temp = pd.DataFrame(calculate_return(start_date=start_date, end_date=end_date, n=n, size=size,
                                                 stock='000905.XSHG'),columns = [str(n) + ' + ' +str(size)] )
        temp.index = nv_df.index
        nv_df = nv_df.join(temp)
        print(n, size)
#for n in n_list:
#    plt.plot(final_df.columns, final_df.loc[n, :])

#plt.legend(n_list)
#plt.show()

final = summaryOutput(nv_df,252,0.01)


final1 = final.sort_values(by = '夏普',ascending =False)


