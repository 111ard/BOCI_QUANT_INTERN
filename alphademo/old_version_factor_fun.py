from rqfactor import *
from rqfactor.extension import *
from rqfactor.notebook import *
import rqdatac
#from factor.Factor1 import Factor1_VWAP
#from factor.Factor_combined import Factor_combined
import pandas as pd
import os
import numpy as np
import shutil
import output_Func as output
import subprocess
import time
import warnings
import multiprocessing
import importlib
import argparse
import json


start_time = time.time()
warnings.filterwarnings("ignore")

# 因子组合
#f1 = Factor1_VWAP()
#f2 =Factor_combined()
rqdatac.init()
# 指定因子
#factor_list = [f1, f2]
# 指定因子名称
#name_list = ['factor1', 'factor2']

"""
def factor_analysis_all(file_name):
    if file_name.endswith('.py'):
        module_name = os.path.splitext(file_name)[0]
        module = importlib.import_module('factor.' + module_name)
        # 构造完整的文件路径
        pwd = os.getcwd()
        factor_path = pwd + '/factor/'
        # file_path = os.path.join(factor_path, file_name)
        # 使用subprocess模块调用python解释器执行该文件
        # function_name = filename[:-3]
        # cmd = subprocess.call(['python', '-c', 'from '+'factor.' +'{} import {}; {}()'.format(filename[:-3], 'main', 'main')])
        result = getattr(module, 'main')()

        # output = subprocess.check_output(cmd, universal_newlines=True)
        # result = eval(output.strip())

        value_csv = output.OutputResult(factor_input=result, factor_name=file_name[:-3]).factor_value(
            start_date=start_date, end_date=end_date)
        if analysis == True:
            temp = output.OutputResult(factor_input=result, factor_name=file_name[:-3]).factor_analysis(
                start_date=start_date, end_date=end_date, df=value_csv)
        if summary ==True:
            temp.columns = [file_name[:-3]]

        print('因子 ' + file_name[:-3] + ' 已经完成')
        return temp
"""
def main(category, factor_input, analysis, summary):
    """
    主函数，运行后得到想要的因子值或者因子分析，会生成相应的结果保存至对应文件夹
    :param factor_input, list, 列表中的元素需为米筐因子: 因子列表。当为独立传入的因子时，为dataframe组成的列表，index为时间格式的日期，columns为米筐代码
    """
    if category =='selected_factor':
        directory = pwd + '/factor/'
        final = pd.DataFrame(index=['第一组减第五组累积收益', '因子累积收益(米筐算法)', '因子收益最大回撤', '因子收益波动率', 'IC_mean',
       'IC_std', 'IC_positive', 'IC_negative', 'IC_significance',
       'IC_sig_positive', 'IC_sig_negative', 'IC_t_stat', 'IC_p_value',
       'IC_skew', 'IC_kurtosis', 'IC_ir'])
        for factor in factor_input:
            i = 0
            for root, dirs, files in os.walk(directory):
                root = root[len(pwd) + 1:]
                for fileName in files:
                    if fileName.endswith(post_fix) and fileName[:-3] == factor:

                        factor_full_path = (root + '/' + fileName)[:-3].replace("/", '.')
                        factor_full_path = factor_full_path.replace('\\', '.')
                        classification = factor_full_path.split('.')[1]
                        module = importlib.import_module(factor_full_path)
                        result = getattr(module, 'main')()
                # 此处计算因子
                        fileName = fileName[:-3]
                        value_csv = output.OutputResult(factor_input=result, factor_name=fileName,stock_pool = stock_pool,pwd=pwd,classification=classification).factor_value(start_date=start_date, end_date=end_date)
                # 输入因子进行分析
                        if analysis ==True:
                            temp = output.OutputResult(factor_input=result, factor_name=fileName,stock_pool = stock_pool,pwd=pwd,classification=classification).factor_analysis(df=value_csv)
                        if summary ==True:
                            temp.columns = [fileName]
                            final = final.join(temp)
                        i+=1
                    else:
                        continue
            if i==0:
                print('因子 '+ factor +' 无法找到，请检查factor文件夹')
            else:
                print('因子 ' + factor + ' 已经完成')
        if summary ==True:
            final.T.to_excel(pwd +r'\result\summary.xlsx')
        """
    if category == 'self_input':
        final = pd.DataFrame(index=['第一组减第五组累积收益', '因子累积收益(米筐算法)', '因子收益最大回撤', '因子收益波动率', 'IC_mean',
       'IC_std', 'IC_positive', 'IC_negative', 'IC_significance',
       'IC_sig_positive', 'IC_sig_negative', 'IC_t_stat', 'IC_p_value',
       'IC_skew', 'IC_kurtosis', 'IC_ir'])
        if analysis ==True:
            for i in range(len(factor_input)):
                temp = output.OutputResult(factor_input=factor_input[i], factor_name=name_list[i]).factor_analysis(start_date=start_date, end_date=end_date,df=factor_input[i])
                if summary ==True:
                    temp.columns = [name_list[i]]
                    final = final.join(temp)
                print('因子 ' + name_list[i] +' 已经完成')
        if summary ==True:
            final.T.to_excel(r'result\summary.xlsx')
        """
    if category == 'all':
        directory = pwd + '/factor/'
        final = pd.DataFrame(index=['第一组减第五组累积收益', '因子累积收益(米筐算法)', '因子收益最大回撤', '因子收益波动率', 'IC_mean',
       'IC_std', 'IC_positive', 'IC_negative', 'IC_significance',
       'IC_sig_positive', 'IC_sig_negative', 'IC_t_stat', 'IC_p_value',
       'IC_skew', 'IC_kurtosis', 'IC_ir'])

        for root, dirs, files in os.walk(directory):
            root = root[len(pwd) + 1:]
            for fileName in files:
                if fileName.endswith(post_fix):
                    factor_full_path = (root+'/'+ fileName)[:-3].replace("/",'.')
                    factor_full_path = factor_full_path.replace('\\','.')
                    classification = factor_full_path.split('.')[1]
                    module = importlib.import_module(factor_full_path)
                    # 构造完整的文件路径
                    # file_path = os.path.join(factor_path, file_name)
                    # 使用subprocess模块调用python解释器执行该文件
                    # function_name = filename[:-3]
                    # cmd = subprocess.call(['python', '-c', 'from '+'factor.' +'{} import {}; {}()'.format(filename[:-3], 'main', 'main')])
                    result = getattr(module, 'main')()

                    # output = subprocess.check_output(cmd, universal_newlines=True)
                    # result = eval(output.strip())

                    value_csv = output.OutputResult(pwd=pwd,factor_input=result, factor_name=fileName[:-3],stock_pool = stock_pool,classification=classification).factor_value(
                        start_date=start_date, end_date=end_date)
                    if analysis == True:
                        temp = output.OutputResult(pwd=pwd,factor_input=result, factor_name=fileName[:-3],stock_pool = stock_pool,classification=classification).factor_analysis(
                            df=value_csv)
                    if summary == True:
                        temp.columns = [fileName[:-3]]
                        final = final.join(temp)
                    print('因子 ' + fileName[:-3] + ' 已经完成')
                else:
                    continue
        if summary == True:
            final.T.to_excel(pwd +r'\result\summary.xlsx')

    if category =='selected_files':
        final = pd.DataFrame(index=['第一组减第五组累积收益', '因子累积收益(米筐算法)', '因子收益最大回撤', '因子收益波动率', 'IC_mean',
                                    'IC_std', 'IC_positive', 'IC_negative', 'IC_significance',
                                    'IC_sig_positive', 'IC_sig_negative', 'IC_t_stat', 'IC_p_value',
                                    'IC_skew', 'IC_kurtosis', 'IC_ir'])
        for directory_name in factor_input:
            factor_path = pwd + '/factor/' + directory_name
            try:
                factor_list = os.listdir(factor_path)
            except:
                print("文件夹 " + directory_name+ ' 不存在，请检查')
                continue
            for file_name in factor_list:
                if file_name.endswith('.py'):
                    module_name = os.path.splitext(file_name)[0]
                    module = importlib.import_module('factor.' +directory_name+'.'+ module_name)
                    # 构造完整的文件路径
                    # file_path = os.path.join(factor_path, file_name)
                    # 使用subprocess模块调用python解释器执行该文件
                    # function_name = filename[:-3]
                    # cmd = subprocess.call(['python', '-c', 'from '+'factor.' +'{} import {}; {}()'.format(filename[:-3], 'main', 'main')])
                    result = getattr(module, 'main')()

                    # output = subprocess.check_output(cmd, universal_newlines=True)
                    # result = eval(output.strip())

                    value_csv = output.OutputResult(pwd=pwd, factor_input=result, factor_name=file_name[:-3],
                                                    stock_pool=stock_pool,classification=directory_name).factor_value(
                        start_date=start_date, end_date=end_date)
                    if analysis == True:
                        temp = output.OutputResult(pwd=pwd, factor_input=result, factor_name=file_name[:-3],
                                                   stock_pool=stock_pool,classification=directory_name).factor_analysis(
                            df=value_csv)
                    if summary == True:
                        temp.columns = [file_name[:-3]]
                        final = final.join(temp)
                    print('因子 ' + file_name[:-3] + ' 已经完成')
        if summary == True:
            final.T.to_excel(pwd + r'\result\summary.xlsx')




# 当需要指定因子进行计算时，先import在factor文件夹中对应的模块，然后对于因子定义，更改命名列表，然后运行下面的程序，可以在demands处更改需求
#main(factor_input=[f1, f2],demands='使用米筐计算得到的因子分析')
# 此处的顺序要和factor_input对应
#name_list = ['factor1', 'factor2']


# open_browser = False
# 起始截止日期
pwd = os.path.abspath(__file__)[:-len(os.path.basename(__file__))]
with open(pwd+ 'config.json') as f:
    data = json.load(f)

parser = argparse.ArgumentParser()
parser.add_argument('--category', type=str, help='execute_method')
parser.add_argument('--factor_input', type=str, nargs='+', help='factor_input')
parser.add_argument("--analysis", action="store_true")
parser.add_argument("--summary", action="store_true")
# parser.add_argument('--start_date', type=str, help='start_date')
# parser.add_argument('--end_date', type=str, help='end_date')

args = parser.parse_args()
start_date = data['start_date']
end_date = data['end_date']

all_in = rqdatac.all_instruments()
stock_pool = pd.read_excel(pwd+ 'stock_pool.xlsx')
all_in = pd.merge(stock_pool, all_in, left_on='证券名称', right_on='symbol',how = 'right')
# 此处dropna会去掉1、ST的股票以及2、米筐中不存在其数据的股票
all_in = all_in.dropna(subset = ['证券代码'])
# 最终用列表储存所有的可用股票代码
stock_pool = list(all_in['order_book_id'])
# 当扫描factor文件夹中所有因子并进行计算和分析，运行下面一行
summary = args.summary
analysis = args.analysis
post_fix = '.py'
main(category=args.category,factor_input=args.factor_input, analysis=analysis,summary=summary)


end_time=time.time()

print('用时为' + str( end_time - start_time) + 's')