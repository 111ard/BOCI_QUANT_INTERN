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

start_time = time.time()
warnings.filterwarnings("ignore")
rqdatac.init()


class Run_Factor():
    """
    把原来的部分全部封装到class中
    """

    def __init__(self, category, analysis, summary):
        """
        初始化
        """

        self.category = category
        self.analysis = analysis
        self.summary = summary
        self.omitted = []
        shutil.rmtree(pwd + '/result/temp_dir')

    def all_file_search(self, fileName):
        try:
            print(self.root)
            print(fileName)
            factor_full_path = (self.root  + '/'+ fileName)[:-3].replace("/", '.')
            factor_full_path = factor_full_path.replace('\\', '.')
            classification = factor_full_path.split('.')[1]
            print(factor_full_path)
            module = importlib.import_module(factor_full_path)
            result = getattr(module, 'main')()
            value_csv = output.OutputResult(pwd=pwd, factor_input=result, factor_name=fileName[:-3], stock_pool=stock_pool,
                                            classification=classification).factor_value(
                start_date=start_date, end_date=end_date)
            if analysis == True:
                temp = output.OutputResult(pwd=pwd, factor_input=result, factor_name=fileName[:-3], stock_pool=stock_pool,
                                           classification=classification).factor_analysis(
                    df=value_csv)
            if summary == True:
                temp.columns = [fileName[:-3]]
                temp.to_excel(pwd + '/result/temp_dir/' + fileName[:-3] + '.xlsx')
            print('因子 ' + fileName[:-3] + ' 已经完成')
        except Exception:
        #    print()
        #    self.omitted.append(fileName)
            print('因子 '+ fileName+ ' 无法执行，请检查定义或网络连接情况')

            with open(pwd + "omitted.txt", "a") as f:
                f.write(fileName + ',')

    def factor_analysis_selected_files(self, directory_name,file_name):

        try:
        #   start_time = time.time()
            if file_name.endswith('.py'):
                # print(file_name)
                module_name = os.path.splitext(file_name)[0]
                module = importlib.import_module('factor.' + directory_name + '.' + module_name)
                # 构造完整的文件路径
                # file_path = os.path.join(factor_path, file_name)
                # 使用subprocess模块调用python解释器执行该文件
                # function_name = filename[:-3]
                # cmd = subprocess.call(['python', '-c', 'from '+'factor.' +'{} import {}; {}()'.format(filename[:-3], 'main', 'main')])
                result = getattr(module, 'main')()

                # output = subprocess.check_output(cmd, universal_newlines=True)
                # result = eval(output.strip())

                value_csv = output.OutputResult(pwd=pwd, factor_input=result, factor_name=file_name[:-3],
                                                stock_pool=stock_pool, classification=directory_name).factor_value(
                    start_date=start_date, end_date=end_date)
                if self.analysis == True:
                    temp = output.OutputResult(pwd=pwd, factor_input=result, factor_name=file_name[:-3],
                                               stock_pool=stock_pool, classification=directory_name).factor_analysis(
                        df=value_csv)
                if self.summary == True:
                    temp.columns = [file_name[:-3]]
                    temp.to_excel(pwd + '/result/temp_dir/' + file_name[:-3] + '.xlsx')

                print('因子 ' + file_name[:-3] + ' 已经完成')

        except Exception:
            pass
            #print(e.args)
            #print(str(e))
            #print(repr(e))
            print('因子 '+ file_name+ ' 无法执行，请检查定义或网络连接情况')
        #except QuotaExceeded
            #sleep(5)
            #print(file_name[:-3])
            self.omitted.append(file_name)
            print(self.omitted)

            with open(pwd + "omitted.txt", "a") as f:
                f.write(file_name + ',')

    def main(self, factor_input):
        """
        主函数，运行后得到想要的因子值或者因子分析，会生成相应的结果保存至对应文件夹
        :param factor_input, list, 列表中的元素需为米筐因子: 因子列表。当为独立传入的因子时，为dataframe组成的列表，index为时间格式的日期，columns为米筐代码
        """
        if self.category not in ['selected_factor', 'all', 'selected_files']:
            print('category参数输入错误，请选择selected_factor或all或selected_files')

        if self.category == 'selected_factor':
            directory = pwd + '/factor/'
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
                            value_csv = output.OutputResult(factor_input=result, factor_name=fileName,
                                                            stock_pool=stock_pool, pwd=pwd,
                                                            classification=classification).factor_value(
                                start_date=start_date, end_date=end_date)
                            # 输入因子进行分析
                            if analysis == True:
                                temp = output.OutputResult(factor_input=result, factor_name=fileName,
                                                           stock_pool=stock_pool, pwd=pwd,
                                                           classification=classification).factor_analysis(df=value_csv)
                            if summary == True:
                                temp.columns = [fileName[:-3]]
                                temp.to_excel(pwd + '/result/temp_dir/' + fileName[:-3] + '.xlsx')
                            i += 1
                        else:
                            continue
                if i == 0:
                    print('因子 ' + factor + ' 无法找到，请检查factor文件夹')
                else:
                    print('因子 ' + factor + ' 已经完成')
                if summary == True:
                    generate_report.main()

        if self.category == 'all':
            directory = pwd + '/factor/'
            for root, dirs, files in os.walk(directory):
                root = root[len(pwd) + 1:]
                self.root = root
                files = [fileName for fileName in files if fileName.endswith(post_fix)]
                if len(files) == 0:
                    continue
                for file in files:
                    self.all_file_search(file)
                # root = [root] * len(files)
                # if __name__ == '__main__':
                    #    pool = multiprocessing.Pool(process_number)
                    #    for file_name in files:
                    #        pool.apply_async(func=self.factor_analysis_selected_files, args=(file_name, root))
                    #   pool.close()
                    #   pool.join()
               # with multiprocessing.Pool(process_number) as pool:
                    # 使用map方法将列表中的每个元素平方并返回结果列表
               #     pool.map(all_file_search, zip(files, root))
            if summary == True:
                generate_report.main()

        if self.category == 'selected_files':

            for directory_name in factor_input:
                #self.directory_name = directory_name
                factor_path = pwd + '/factor/' + directory_name
                try:
                    factor_list = os.listdir(factor_path)
                except:
                    print("文件夹 " + directory_name + ' 不存在，请检查')
                    continue
                # if __name__ == '__main__':
                with open(pwd + "omitted.txt", "w") as f:
                    f.write('')
                    # pool = multiprocessing.Pool(process_number)
                    # for file_name in factor_list:
                    #    pool.apply_async(func=self.factor_analysis_selected_files, args=(file_name,directory_name,))
                    #                    with multiprocessing.Pool(process_number) as pool:
                    # 使用map方法将列表中的每个元素平方并返回结果列表
                    #   pool = multiprocessing.Pool(process_number)
                for file_name in factor_list:
                    self.factor_analysis_selected_files(directory_name,file_name)
                #pfunc = partial(self.factor_analysis_selected_files, directory_name)
                # pool.map(pfunc, factor_list)
                   #pool.close()
                    #pool.join()
            if summary == True:
                generate_report.main()




pwd = os.path.abspath(__file__)[:-len(os.path.basename(__file__))]
with open(pwd + 'config.json') as f:
    data = json.load(f)

parser = argparse.ArgumentParser()
parser.add_argument('--process', type=int, help='execute_method')
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

Run_Factor(category=args.category, analysis=analysis, summary=summary).main(factor_input=args.factor_input)
end_time = time.time()
print('用时为' + str(end_time - start_time) + 's')

