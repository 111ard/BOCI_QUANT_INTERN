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
    def __init__(self, factor_input, factor_name,stock_pool,pwd,classification):
        """
        初始化
        :param factor_input, Factor: 米筐因子
        :param factor_name, string: 因子的名称
        """
        self.factor_input = factor_input
        self.factor_name = factor_name
        self.stock_pool = stock_pool
        # 创建文件夹

        os.makedirs(pwd + '/result', exist_ok=True)
        os.makedirs(pwd + '/log', exist_ok=True)

        os.makedirs(pwd +'/result/' + classification, exist_ok=True)
        os.makedirs(pwd +'/result/' + classification+ '/'+factor_name + '/jpg', exist_ok=True)
        os.makedirs(pwd +'/result/'  + classification+ '/'+factor_name + '/csv', exist_ok=True)
        os.makedirs(pwd +'/result/'  + classification+'/'+ factor_name + '/csv/'+ 'ic_analysis', exist_ok=True)
        os.makedirs(pwd +'/result/'  + classification+ '/'+factor_name + '/csv/'+ 'quantile_analysis', exist_ok=True)
        os.makedirs(pwd +'/result/'  + classification+ '/'+factor_name + '/csv/'+ 'return_analysis', exist_ok=True)
        os.makedirs(pwd +'/result/'  + classification+ '/'+factor_name + '/csv/'+ 'factor_value', exist_ok=True)        # 此处dropna会去掉1、ST的股票以及2、米筐中不存在其数据的股票
        self.stock_pool = stock_pool
        self.pwd=pwd
        self.classification=classification


    def factor_value(self,start_date, end_date):
        """
        用于将因子的执行结果保存至csv文件
        :return: factorValue, DataFrame: 因子值，索引为日期，列名为因子
        """
        # 此处会存在缺失值，因为股票停牌退市或者米筐无数据
        df = execute_factor(self.factor_input, self.stock_pool, start_date, end_date)
        df.to_csv(self.pwd +'/result/' +self.classification+'/'+ self.factor_name + '/csv/'+ 'factor_value/'+self.factor_name+'_factor_value.csv')
        return df



    def factor_analysis(self, df):
        """
        用于输出一个因子的回测结果，放入文件夹中
        :param start_date: 回测起始日期
        :param df: 因子值
        :param end_date: 回测截止日期
        """

        engine = FactorAnalysisEngine()
        # 构建管道，对因子进行预处理
        #  engine.append(('winzorization-mad', Winzorization(method='mad')))
        #   engine.append(('normalization', Normalization()))
        #   engine.append(('neutralization', Neutralization(industry='citics_2019',
        #                                                   style_factors=['size', 'beta', 'earnings_yield', 'growth',
        #                                                                 'liquidity',
        #                                                                 'leverage', 'book_to_price',
        #                                                                 'residual_volatility',
        #                                                                'non_linear_size'])))
        # 构建管道，添加因子分析器

        engine.append(('rank_ic_analysis', ICAnalysis(rank_ic=True, industry_classification='sws', max_decay=3)))
        engine.append(('quantile', QuantileReturnAnalysis(quantile=5, benchmark=None)))
        engine.append(('return', FactorReturnAnalysis()))
        # 调仓周期为1，3，5
        result = engine.analysis(df, 'daily', ascending=True, periods=[1], keep_preprocess_result=False)


        result['rank_ic_analysis'].summary().to_csv(self.pwd +'/result/' +self.classification+'/'+ self.factor_name + '/csv/'+ 'ic_analysis/'+"rank_ic_analysis.csv")
        result['rank_ic_analysis'].ic.to_csv(self.pwd +'/result/' +self.classification+'/'+ self.factor_name + '/csv/'+ 'ic_analysis/'+'ic_analysis.csv')
        result['rank_ic_analysis'].ic_decay.to_csv(self.pwd +'/result/' +self.classification+'/'+ self.factor_name + '/csv/'+ 'ic_analysis/'+'ic_decay.csv')
        result['rank_ic_analysis'].ic_industry_distribute.to_excel(self.pwd +'/result/' +self.classification+'/'+ self.factor_name + '/csv/'+ 'ic_analysis/'+'ic_industry_distribute.xlsx')
        result['quantile'].quantile_returns.to_csv(self.pwd +'/result/' +self.classification+'/'+ self.factor_name + '/csv/'+ 'quantile_analysis/'+'quantile_returns.csv')
        result['quantile'].quantile_turnover.to_csv(self.pwd +'/result/' +self.classification+'/'+ self.factor_name + '/csv/'+ 'quantile_analysis/'+'quantile_turnover.csv')
        result['quantile'].top_minus_bottom_returns.to_csv(self.pwd +'/result/'+self.classification+'/' + self.factor_name + '/csv/'+ 'quantile_analysis/'+'top_minus_bottom_returns.csv')
        result['quantile'].quantile_detail.to_csv(self.pwd +'/result/' +self.classification+'/'+ self.factor_name + '/csv/'+ 'quantile_analysis/'+'quantile_detail.csv')
        result['return'].factor_returns.to_csv(self.pwd +'/result/' +self.classification+'/'+ self.factor_name + '/csv/'+ 'return_analysis/'+'factor_returns.csv')
        result['return'].max_drawdown().to_csv(self.pwd +'/result/'+self.classification+'/' + self.factor_name + '/csv/'+ 'return_analysis/'+'factor_returns_max_drawdown.csv')
        result['return'].std().to_csv(self.pwd +'/result/' +self.classification+'/'+ self.factor_name + '/csv/'+ 'return_analysis/'+'factor_returns_std.csv')


        for k in (['rank_ic_analysis', 'quantile', 'return']):
            try:
                result[k].show()
                shutil.move(k + '.html', self.pwd +'/result/' +self.classification+'/'+ self.factor_name +'/jpg')

            except:
                pass

            try:
                os.remove(k + '.html')
            except:
                pass
        """
        app = xw.App(visible=False)
        workbook = app.books.add()
        # 在工作簿中添加一个名为"Sheet1"的工作表并选定该工作表
        worksheet = workbook.sheets.add("rank_IC_analysis")
        worksheet.select()
        # 在单元格A1中写入值
        worksheet.range('A1').value = 'summary'
        worksheet.range('A2').value = result['rank_ic_analysis'].summary()
        worksheet.range('G1').value = 'IC_daily'
        worksheet.range('G2').value = result['rank_ic_analysis'].ic
        worksheet.range('M1').value = 'IC_decay'
        worksheet.range('M2').value = result['rank_ic_analysis'].ic_decay
        worksheet.range('A16').value = 'IC_industry'
        worksheet.range('A17').value = result['rank_ic_analysis'].ic_industry_distribute

        worksheet = workbook.sheets.add("quantile_analysis")
        worksheet.select()
        # 在单元格A1中写入值
        worksheet.range('A1').value = 'quantile_return'
        worksheet.range('A2').value = result['quantile'].quantile_returns
        worksheet.range('G1').value = 'quantile_turnover'
        worksheet.range('G2').value = result['quantile'].quantile_turnover
        worksheet.range('M1').value = 'top_minus_bottom_returns'
        worksheet.range('M2').value = result['quantile'].top_minus_bottom_returns
        worksheet.range('S1').value = 'quantile_detail'
        worksheet.range('S2').value = result['quantile'].quantile_detail

        worksheet = workbook.sheets.add("factor_return_analysis")
        worksheet.select()
        # 在单元格A1中写入值
        worksheet.range('A1').value = 'factor_daily_return'
        worksheet.range('A2').value = result['return'].factor_returns
        worksheet.range('G1').value = 'factor_Max_draw_down'
        worksheet.range('G2').value = result['return'].max_drawdown()
        worksheet.range('G10').value = 'factor_std'
        worksheet.range('G11').value = result['return'].std()


        # 保存工作簿并关闭Excel应用程序
        workbook.save(self.pwd +'/result/' + self.factor_name + '/csv/'+ self.factor_name + "_factor_summary.xlsx")
        """
        top_minus_bottom_returns = float(np.cumprod(1 + result['quantile'].top_minus_bottom_returns['P_1'] / 2)[-1]) - 1
        factor_return = result['return'].factor_returns['P_1'][-1]
        factor_mdd = result['return'].max_drawdown().iloc[0]
        factor_std = result['return'].std().iloc[0]
        temp_dataframe = pd.DataFrame(
            {'factor_name': [top_minus_bottom_returns, factor_return, factor_mdd, factor_std]},
            index=['第一组减第五组累积收益', '因子累积收益(米筐算法)', '因子收益最大回撤', '因子收益波动率'])
        factor_average_ic_df = pd.DataFrame(result['rank_ic_analysis'].summary()['P_1'])
        factor_average_ic_df.index = 'IC_' + factor_average_ic_df.index
        factor_average_ic_df.columns = ['factor_name']
        final = temp_dataframe.append(factor_average_ic_df)

        return final

    def generate_report(self):
        df = []
        temp_path = self.pwd + '/result/temp_dir'
        temp_list = os.listdir(temp_path)
        for report_unique in temp_list:
            temp = pd.read_excel(temp_path + '/' + report_unique).T
            df.append(temp)
        df = pd.concat(df).T
        df = df.T.drop_duplicates()
        df = df.T.set_index('Unnamed: 0', drop=True)
        df.to_excel(self.pwd + '/result/summary.xlsx')

    def main(self, start_date, end_date, df):
        self.factor_value(start_date, end_date)
        self.factor_analysis(start_date, end_date, df)
        self.generate_report()