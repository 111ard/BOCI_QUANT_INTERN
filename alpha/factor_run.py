from rqfactor import *
from rqfactor.extension import *
from rqfactor.notebook import *
import rqdatac
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
from functools import partial
from time import sleep
import generate_report
import logging

start_time = time.time()
warnings.filterwarnings("ignore")
rqdatac.init()
logging.basicConfig(level=logging.DEBUG #设置日志输出格式
                    ,filename="BOCI_QUANT_INTERN/alpha/log/demo.log" #log日志输出的文件位置和文件名
                    ,filemode="w" #文件的写入格式，w为重新写入文件，默认是追加
                    ,format="%(asctime)s - %(name)s - %(levelname)-9s - %(filename)-8s : %(lineno)s line - %(message)s" #日志输出的格式
                    # -8表示占位符，让输出左对齐，输出长度都为8位
                    ,datefmt="%Y-%m-%d %H:%M:%S" #时间输出的格式
                    )


class Run_Factor():
    """
    把原来的部分全部封装到class中
    """

    def __init__(self, method,category, analysis, summary):
        """
        初始化
        """

        self.category = category
        self.analysis = analysis
        self.summary = summary
        self.method = method
        self.omitted = []
        try:
            shutil.rmtree(os.path.join(pwd , '/result/temp_dir'))
        except:
            pass
        os.makedirs(os.path.join(pwd , '/result/temp_dir'), exist_ok=True)
        with open(os.path.join(pwd , "omitted.txt"), "w") as f:
            f.write('')

    def all_file_analysis(self, fileName):
        try:
            factor_full_path = os.path.join(self.root , fileName)
            classification = factor_full_path.split('.')[1]
            value_csv = pd.read_csv(os.path.join(pwd, factor_full_path),index_col = 0)
            value_csv.index = pd.to_datetime(value_csv.index)
            if analysis == True:
                temp = output.OutputResult(pwd=pwd, factor_input='', factor_name=fileName[:-17], stock_pool=stock_pool,
                                           classification=classification).factor_analysis(
                    df=value_csv)
            if summary == True:
                temp.columns = [fileName[:-17]]
                temp.to_excel(pwd + '/result/temp_dir/' + fileName[:-4] + '_temp.xlsx')
            logging.info('因子 ' + fileName[:-4] + ' 已经完成')
            # print('因子 ' + fileName[:-4] + ' 已经完成')
        except Exception:
            logging.info('因子 '+ fileName+ ' 无法执行，请检查定义或网络连接情况')
            # print('因子 '+ fileName+ ' 无法执行，请检查定义或网络连接情况')

            with open(pwd + "omitted.txt", "a") as f:
                f.write(fileName + ',')
        except (AttributeError):
            logging.info('因子 ' + fileName +  ' 返回值为空')
            # print('因子 ' + fileName +  ' 返回值为空')
            pass
            with open(pwd + "omitted.txt", "a") as f:
                f.write(fileName + ',')

    def all_file_search(self, fileName, classification):
        try:
            factor_full_path = (self.root + '/' + fileName)[:-3].replace("/", '.')
            factor_full_path = factor_full_path.replace('\\', '.')
            classification = factor_full_path.split('.')[1]
            module = importlib.import_module(factor_full_path)
            result = getattr(module, 'main')()
            try:
                value_csv = pd.read_csv(os.path.join(pwd, result,classification, fileName[:-3],'csv','factor_value',fileName+'_factor_value.csv' ),index_col = 0)
                logging.info(fileName[:-3] + '已经存在，已读取')
                date_needed_append_start = value_csv.index[-1]
                value_csv = pd.concat(value_csv, output.OutputResult(pwd=pwd, factor_input=result, factor_name=fileName[:-3], stock_pool=stock_pool,
                                                classification=classification).factor_value(
                    start_date=date_needed_append_start, end_date=end_date).iloc[1:])
                value_csv.to_csv(os.path.join(pwd, 'result',classification, fileName[:-3],'csv','factor_value',fileName[:-3]+'_factor_value.csv' ))
                logging.info(fileName[:-3] + '已经完成并输出')
            except:
                logging.info(fileName[:-3] +'不存在，从起始日期开始计算')
                value_csv = output.OutputResult(pwd=pwd, factor_input=result, factor_name=fileName[:-3], stock_pool=stock_pool,
                                                classification=classification).factor_value(
                    start_date=start_date, end_date=end_date)
                logging.info(fileName[:-3] + '已经完成并输出')
            if analysis == True:
                temp = output.OutputResult(pwd=pwd, factor_input=result, factor_name=fileName[:-3], stock_pool=stock_pool,
                                           classification=classification).factor_analysis(
                    df=value_csv)
            if summary == True:
                temp.columns = [fileName[:-3]]
                temp.to_excel(pwd + '/result/temp_dir/' + fileName[:-3] + '_temp.xlsx')
            # print('因子 ' + fileName[:-3] + ' 已经完成')
        except Exception:
            logging.info(fileName[:-3] + '无法执行，请检查定义或网络连接情况')
            #print('因子 '+ fileName+ ' 无法执行，请检查定义或网络连接情况')

            with open(pwd + "omitted.txt", "a") as f:
                f.write(fileName + ',')
        except (AttributeError):
            logging.info(fileName[:-3] + '返回值为空')
            # print('因子 ' + fileName[:-3] +  ' 返回值为空')
            pass
            with open(pwd + "omitted.txt", "a") as f:
                f.write(fileName[:-3] + ',')

    def factor_analysis_selected_files(self, directory_name,file_name):

        try:
            if file_name.endswith('.py'):

                module_name = os.path.splitext(file_name)[0]

                module = importlib.import_module('factor.' + directory_name + '.' + module_name)
                result = getattr(module, 'main')()
                try:

                    value_csv = pd.read_csv(os.path.join(pwd, 'result',directory_name, file_name[:-3],'csv','factor_value',file_name[:-3]+'_factor_value.csv' ),index_col = 0)
                    logging.info(file_name[:-3] + '已经存在，已读取')
                    date_needed_append_start = value_csv.index[-1]
                    value_csv = value_csv.append(value_csv,
                                          output.OutputResult(pwd=pwd, factor_input=result, factor_name=file_name[:-3],
                                                              stock_pool=stock_pool,
                                                              classification=directory_name).factor_value(
                                              start_date=date_needed_append_start, end_date=end_date).iloc[1:])
                    value_csv.to_csv(os.path.join(pwd, 'result',directory_name, file_name[:-3],'csv','factor_value',file_name[:-3]+'_factor_value.csv' ))
                    logging.info(file_name[:-3] + '已经完成并输出')
                except:
                    logging.info(file_name[:-3] + '不存在，从起始日期开始计算')
                    value_csv = output.OutputResult(pwd=pwd, factor_input=result, factor_name=file_name[:-3],
                                                stock_pool=stock_pool, classification=directory_name).factor_value(
                        start_date=start_date, end_date=end_date)
                    logging.info(file_name[:-3] +'已经完成并输出')
                if self.analysis == True:
                    temp = output.OutputResult(pwd=pwd, factor_input=result, factor_name=file_name[:-3],
                                               stock_pool=stock_pool, classification=directory_name).factor_analysis(
                        df=value_csv)
                if self.summary == True:
                    temp.columns = [file_name[:-3]]
                    temp.to_excel(pwd + '/result/temp_dir/' + file_name[:-3] + '_temp.xlsx')

                # print('因子 ' + file_name[:-3] + ' 已经完成')

        except Exception:
            pass
            logging.info('因子 '+ file_name+ ' 无法执行，请检查定义或网络连接情况')
 #           print('因子 '+ file_name+ ' 无法执行，请检查定义或网络连接情况')
            with open(pwd + "omitted.txt", "a") as f:
                f.write(file_name + ',')
        except (AttributeError):
            # print('因子 ' + file_name +  ' 返回值为空')
            logging.info('因子 ' + file_name +  ' 返回值为空')
            pass
            with open(pwd + "omitted.txt", "a") as f:
                f.write(file_name + ',')

    def main(self, factor_input):
        """
        主函数，运行后得到想要的因子值或者因子分析，会生成相应的结果保存至对应文件夹
        :param factor_input, list, 列表中的元素需为米筐因子: 因子列表。当为独立传入的因子时，为dataframe组成的列表，index为时间格式的日期，columns为米筐代码
        """
        if self.method not in ['report_only','backtest_and_report']:
            print ('method参数输入错误，请从report_only和backtest_and_report中选择')
        if self.method =='report_only':
            if self.category not in ['selected_factor', 'all', 'selected_files']:
                print('category参数输入错误，请选择selected_factor或all或selected_files')

            if self.category == 'selected_factor':
                classification_summary = '_'.join(factor_input)
                directory = pwd + '/result/'
                for factor in factor_input:
                    i = 0
                    for root, dirs, files in os.walk(directory):
                        root = root[len(pwd) + 1:]
                        for fileName in files:
                            if fileName.endswith('_factor_value.csv') and fileName[:-17] ==factor:
                                factor_full_path = (root + '/' + fileName)
                                classification = factor_full_path.split('.')[1]
                                try:
                                    value_csv =pd.read_csv(pwd+ factor_full_path,index_col=0)
                                    logging.info(fileName[:-17] + '已经存在，已读取')
                                    value_csv.index = pd.to_datetime(value_csv.index)

                                except:
                                    logging.info('因子'+fileName[:-17]+ '净值数据不存在，请检查')
                                    # print('因子'+fileName[:-17]+ '净值数据不存在，请检查')
                                    pass
                                # 此处计算因子
                                fileName = fileName[:-17]
                                if analysis == True:
                                    temp = output.OutputResult(factor_input=value_csv, factor_name=fileName,
                                                               stock_pool=stock_pool, pwd=pwd,
                                                               classification=classification).factor_analysis(df=value_csv)
                                if summary == True:
                                    temp.columns = [fileName]
                                    temp.to_excel(pwd + '/result/temp_dir/' + fileName + '_temp.xlsx')
                                i += 1
                            else:
                                continue
                    if i == 0:
                        logging.info('因子 ' + factor + ' 净值无法找到，请检查factor文件夹')

                        with open(pwd + "omitted.txt", "a") as f:
                            f.write(factor + ',')
                    else:
                        logging.info('因子 ' + factor + ' 已经完成')


            if self.category == 'all':
                classification_summary = 'all'
                directory = pwd + '/result/'
                for root, dirs, files in os.walk(directory):
                    root = root[len(pwd) + 1:]
                    self.root = root
                    files = [fileName for fileName in files if fileName.endswith('_factor_value.csv')]
                    if len(files) == 0:
                        continue
                    for file in files:
                        self.all_file_analysis(file)

            if self.category =='selected_files':
                classification_summary = '_'.join(factor_input)
                for directory_name in factor_input:
                    factor_path = pwd + '/result/' + directory_name
                    for root, dirs, files in os.walk(factor_path):
                        root = root[len(pwd) + 1:]
                        self.root = root
                        files = [fileName for fileName in files if fileName.endswith('_factor_value.csv')]
                        if len(files) == 0:
                            continue
                        for file in files:
                            self.all_file_analysis(file)



        if self.method =='backtest_and_report':
            if self.category not in ['selected_factor', 'all', 'selected_files']:
                print('category参数输入错误，请选择selected_factor或all或selected_files')

            if self.category == 'selected_factor':
                classification_summary = '_'.join(factor_input)
                directory = pwd + '/factor/'
                for factor in factor_input:

                    i = 0
                    for root, dirs, files in os.walk(directory):
                        root = root[len(pwd) + 1:]
                        for fileName in files:
                            if fileName.endswith(post_fix) and fileName[:-3] == factor:
                                factor_full_path = (root + '/' + fileName)[:-3].replace("/", '.')
                                factor_full_path_module = factor_full_path.replace('\\', '.')
                                classification = factor_full_path_module.split('.')[1]
                                module = importlib.import_module(factor_full_path_module)
                                result = getattr(module, 'main')()
                                # 此处计算因子
                                fileName = fileName[:-3]
                                try:
                                    value_csv = pd.read_csv(os.path.join(pwd, 'result',classification, factor,'csv','factor_value',fileName+'_factor_value.csv' ),index_col = 0)
                                    logging.info(fileName + '已经存在，已读取')
                                    dates_updated_start =value_csv.index[-1]
                                    value_csv = value_csv.append( output.OutputResult(factor_input=result, factor_name=fileName,
                                                                    stock_pool=stock_pool, pwd=pwd,
                                                                    classification=classification).factor_value(
                                        start_date=dates_updated_start, end_date=end_date).iloc[1:])
                                    value_csv.to_csv(os.path.join(pwd, 'result',classification, factor,'csv','factor_value',fileName+'_factor_value.csv' ))
                                    logging.info('因子 ' + fileName + ' 已经完成')
                                except:
                                    logging.info(fileName + '不存在，从起始日期开始计算')
                                    value_csv = output.OutputResult(factor_input=result, factor_name=fileName,
                                                                    stock_pool=stock_pool, pwd=pwd,
                                                                    classification=classification).factor_value(
                                        start_date=start_date, end_date=end_date)
                                    # 输入因子进行分析
                                    logging.info('因子 ' + fileName + ' 已经完成')
                                if analysis == True:
                                    temp = output.OutputResult(factor_input=result, factor_name=fileName,
                                                               stock_pool=stock_pool, pwd=pwd,
                                                               classification=classification).factor_analysis(df=value_csv)
                                if summary == True:
                                    temp.columns = [fileName[:-3]]
                                    temp.to_excel(pwd + '/result/temp_dir/' + fileName[:-3] + '_temp.xlsx')
                                i += 1
                                # finally:
                                #     pass
                            else:
                                continue
                    if i == 0:
                        logging.info('因子 ' + factor + ' 无法找到或返回值为空，请检查factor文件夹')
                        # print('因子 ' + factor + ' 无法找到或返回值为空，请检查factor文件夹')
                    else:
                        logging.info('因子 ' + factor + ' 已经完成')
                        # print('因子 ' + factor + ' 已经完成')


            if self.category == 'all':
                directory = pwd + '/factor/'
                for root, dirs, files in os.walk(directory):
                    root = root[len(pwd) + 1:]
                    self.root = root
                    # 抓取出所有py文件
                    files = [fileName for fileName in files if fileName.endswith(post_fix)]
                    if len(files) == 0:
                        continue
                    classification = os.path.basename(root)
                    for file in files:
                        self.all_file_search(file,classification)
                classification_summary = 'all'


            if self.category == 'selected_files':
                for directory_name in factor_input:
                    factor_path = pwd + '/factor/' + directory_name
                    try:
                        factor_list = os.listdir(factor_path)
                    except:
                        logging.info("文件夹 " + directory_name + ' 不存在，请检查')
                        # print("文件夹 " + directory_name + ' 不存在，请检查')
                        continue
                    with open(pwd + "omitted.txt", "w") as f:
                        f.write('')
                    for file_name in factor_list:
                        self.factor_analysis_selected_files(directory_name,file_name)

                classification_summary = '_'.join(factor_input)
        if summary == True:
            try:
                generate_report.main(classification_summary)
            except:
                pass




pwd = os.path.abspath(__file__)[:-len(os.path.basename(__file__))]
with open(os.path.join(pwd , 'config.json')) as f:
    data = json.load(f)

parser = argparse.ArgumentParser()
parser.add_argument('--process', type=int, help='execute_method')
parser.add_argument('--method', type=str, help='execute_method')
parser.add_argument('--category', type=str, help='execute_method')
parser.add_argument('--factor_input', type=str, nargs='+', help='factor_input')
parser.add_argument("--analysis", action="store_true")
parser.add_argument("--summary", action="store_true")
# parser.add_argument('--start_date', type=str, help='start_date')
# parser.add_argument('--end_date', type=str, help='end_date')

args = parser.parse_args()
start_date = data['start_date']
end_date = data['end_date']
can_not_execute_list = []
all_in = rqdatac.all_instruments()
stock_pool = pd.read_excel(pwd + 'stock_pool.xlsx')
all_in = pd.merge(stock_pool, all_in, left_on='证券名称', right_on='symbol', how='right')
# 此处dropna会去掉1、ST的股票以及2、米筐中不存在其数据的股票
all_in = all_in.dropna(subset=['证券代码'])
# 最终用列表储存所有的可用股票代码
stock_pool = list(all_in['order_book_id'])
# 当扫描factor文件夹中所有因子并进行计算和分析，运行下面一行

process_number = 1
summary = args.summary
analysis = args.analysis
post_fix = '.py'

# os.removedirs("/test")

# start_time = time.time()
# main(category=args.category,factor_input=args.factor_input, analysis=analysis,summary=summary)

Run_Factor(method = args.method,category=args.category, analysis=analysis, summary=summary).main(factor_input=args.factor_input)
end_time = time.time()
logging.info('用时为' + str(end_time - start_time) + 's')
# print('用时为' + str(end_time - start_time) + 's')

