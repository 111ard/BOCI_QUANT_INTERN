import rqdatac
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as mdate


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


def daily_search_function_1m(df):
    df['max_till_now'] = df.close.cummax()
    df['1m_return'] = (df.close.shift(-1) / df.close)
    df_used_data = df.iloc[60:239]
    df_used_data['breakup_indicator'] = 0
    df_used_data.loc[df_used_data.close >= df_used_data.max_till_now.shift(1), 'breakup_indicator'] = 1
    if df_used_data.breakup_indicator.sum() == 0:
        return 1

    df_final_used = df_used_data.loc[df_used_data.breakup_indicator == 1, :]

    daily_return = np.cumprod((df_final_used['1m_return'] - 0.0001)).iloc[-1]

    return daily_return


def daily_search_function_60m(df):
    df = df.reset_index(drop=True)
    df['max_till_now'] = df.close.cummax()
    df['60m_return'] = (df.close.shift(-60) / df.close) - 1
    df['hold_to_end_return'] = df.close.iloc[-4] / df.close - 1
    df.loc[180:238, '60m_return'] = df.loc[180:238, 'hold_to_end_return']
    df_used_data = df.iloc[60:]
    df_used_data['breakup_indicator'] = 0
    df_used_data.loc[df_used_data.close >= df_used_data.max_till_now, 'breakup_indicator'] = 1
    if df_used_data.breakup_indicator.sum() == 0:
        return 1

    df_final_used = df_used_data.loc[df_used_data.breakup_indicator == 1, :]
    df_final_used = df_final_used.iloc[0:4, :]
    df_final_used['60m_return'] = 0.2 * (df_final_used['60m_return'] - 0.0004)
    daily_return = np.cumprod(1 + df_final_used['60m_return']).iloc[-1]

    return daily_return


rqdatac.init()
original_data = rqdatac.get_price('000905.XSHG', start_date='20170101', end_date='20230101', frequency='1m')
original_data_reset_index = original_data.reset_index()
original_data_reset_index['trading_date'] = original_data_reset_index['datetime'].dt.strftime('%Y%m%d')
original_data_reset_index['average'] = (original_data_reset_index.open + original_data_reset_index.close) / 2

k = original_data_reset_index.groupby('trading_date').apply(daily_search_function_60m)
NV_sequence = np.cumprod(k)
NV_sequence.index = pd.to_datetime(NV_sequence.index,format='%Y%m%d')
benchmark = original_data_reset_index.groupby('trading_date').close.apply(lambda x: x.iloc[-1])
benchmark = benchmark/benchmark.iloc[0]
benchmark.index = pd.to_datetime(benchmark.index, format='%Y%m%d')
ax = plt.gca()
ax.xaxis.set_major_locator(ticker.MultipleLocator(300))
ax.xaxis.set_major_formatter(mdate.DateFormatter('%Y'))
plt.plot(NV_sequence)
plt.plot(benchmark)
plt.legend(['net_value', 'benchmark'])
original_summary = pd.DataFrame({'策略':NV_sequence,'基准':benchmark})
final = summaryOutput(originalSequence = original_summary,valueFrequency= 252,risk_free_rate=0.01)