from rqfactor import *
from rqfactor.notebook import *
import rqdatac
import os
import shutil
import pandas as pd
import numpy as np


class OutputResult():
    """
    用于使用ricequant函数输出因子的IC、分组回测表现以及收益情况
    """
    def __init__(self,factor_result_path,pwd):
        """
        初始化
        """
        self.factor_result_path = factor_result_path
        self.pwd = pwd

    def factor_analysis(self, df,factor_name):
        """
        用于输出一个因子的回测结果，放入文件夹中
        :param df: 因子值
        """

        engine = FactorAnalysisEngine()
        engine.append(('rank_ic_analysis', ICAnalysis(rank_ic=True, industry_classification='sws', max_decay=3)))
        engine.append(('quantile', QuantileReturnAnalysis(quantile=5, benchmark=None)))
        engine.append(('return', FactorReturnAnalysis()))
        result = engine.analysis(df, 'daily', ascending=True, periods=[1], keep_preprocess_result=False)
        result['rank_ic_analysis'].summary().to_csv(self.factor_result_path + '/csv/'+ 'ic_analysis/'+"rank_ic_analysis.csv")
        result['rank_ic_analysis'].ic.to_csv(self.factor_result_path + '/csv/'+ 'ic_analysis/'+'ic_analysis.csv')
        result['rank_ic_analysis'].ic_decay.to_csv(self.factor_result_path + '/csv/'+ 'ic_analysis/'+'ic_decay.csv')
        result['rank_ic_analysis'].ic_industry_distribute.to_excel(self.factor_result_path + '/csv/'+ 'ic_analysis/'+'ic_industry_distribute.xlsx')
        result['quantile'].quantile_returns.to_csv(self.factor_result_path + '/csv/'+ 'quantile_analysis/'+'quantile_returns.csv')
        result['quantile'].quantile_turnover.to_csv(self.factor_result_path + '/csv/'+ 'quantile_analysis/'+'quantile_turnover.csv')
        result['quantile'].top_minus_bottom_returns.to_csv(self.factor_result_path + '/csv/'+ 'quantile_analysis/'+'top_minus_bottom_returns.csv')
        result['quantile'].quantile_detail.to_csv(self.factor_result_path + '/csv/'+ 'quantile_analysis/'+'quantile_detail.csv')
        result['return'].factor_returns.to_csv(self.factor_result_path + '/csv/'+ 'return_analysis/'+'factor_returns.csv')
        result['return'].max_drawdown().to_csv(self.factor_result_path + '/csv/'+ 'return_analysis/'+'factor_returns_max_drawdown.csv')
        result['return'].std().to_csv(self.factor_result_path + '/csv/'+ 'return_analysis/'+'factor_returns_std.csv')

        for k in (['rank_ic_analysis', 'quantile', 'return']):
            try:
                result[k].show()
                shutil.move(k + '.html', self.factor_result_path +'/jpg')

            except:
                pass

            try:
                os.remove(k + '.html')
            except:
                pass
        # top_minus_bottom_returns = float(result['quantile'].top_minus_bottom_returns['P_1'] [-1])
        # factor_return = result['return'].factor_returns['P_1'][-1]
        # factor_mdd = result['return'].max_drawdown().iloc[0]
        # factor_std = result['return'].std().iloc[0]
        # temp_dataframe = pd.DataFrame(
        #     {'factor_name': [top_minus_bottom_returns, factor_return, factor_mdd, factor_std]},
        #     index=['第一组减第五组累积收益', '因子累积收益(米筐算法)', '因子收益最大回撤', '因子收益波动率'])
        # factor_average_ic_df = pd.DataFrame(result['rank_ic_analysis'].summary()['P_1'])
        # factor_average_ic_df.index = 'IC_' + factor_average_ic_df.index
        # factor_average_ic_df.columns = ['factor_name']
        # final = temp_dataframe.append(factor_average_ic_df)
        # final.to_csv(os.path.join(self.pwd, 'result', 'temp_dir', factor_name+'_temp.csv'))

    def generate_report(self):
        df = []
        temp_path = self.pwd + '/result/temp_dir'
        temp_list = os.listdir(temp_path)
        for report_unique in temp_list:
            temp = pd.read_excel(os.path.join(temp_path, report_unique)).T
            df.append(temp)
        df = pd.concat(df).T
        df = df.T.drop_duplicates()
        df = df.T.set_index('Unnamed: 0', drop=True)
        df.T.to_excel(self.pwd + '/result/summary.xlsx')

    def temp_generation(self,factor):
        """
        总共要读取五个csv
        :param factor:
        :return:
        """
        top_minus_bottom_returns = float(pd.read_csv(self.factor_result_path + '/csv/'+ 'quantile_analysis/'+'top_minus_bottom_returns.csv').iloc[-1]['P_1'])
        factor_return = float(pd.read_csv(self.factor_result_path + '/csv/'+ 'return_analysis/'+'factor_returns.csv').iloc[-1]['P_1'])
        factor_mdd = float(pd.read_csv(self.factor_result_path + '/csv/'+ 'return_analysis/'+'factor_returns_max_drawdown.csv').loc[0,'0'])
        factor_std = float(pd.read_csv(self.factor_result_path + '/csv/'+ 'return_analysis/'+'factor_returns_std.csv').iloc[-1].values[1])
        temp_dataframe = pd.DataFrame(
            {'factor_name': [top_minus_bottom_returns, factor_return, factor_mdd, factor_std]},
            index=['第一组减第五组累积收益', '因子累积收益(米筐算法)', '因子收益最大回撤', '因子收益波动率'])
        ic_analysis = pd.read_csv(self.factor_result_path + '/csv/'+ 'ic_analysis/'+"rank_ic_analysis.csv",index_col = 0)
        ic_analysis.index = 'IC_'+ ic_analysis.index
        ic_analysis.columns = ['factor_name']
        final = temp_dataframe.append(ic_analysis)
        final.to_excel(os.path.join(self.pwd, 'result', 'temp_dir', factor + '_temp.xlsx'))