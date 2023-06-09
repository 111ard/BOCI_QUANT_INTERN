import os
import rqdatac
import pandas as pd
import importlib
import argparse
from rqfactor import *
from rqfactor.notebook import *
import multiprocessing
import lib as lib
import warnings
import time
import json
import shutil

warnings.filterwarnings('ignore')


class FactorRun():
    """
    优化后的类
    """

    def __init__(self):
        """
        初始化
        """

        self.pwd = os.path.abspath(__file__)[:-len(os.path.basename(__file__))]
        with open(os.path.join(self.pwd, 'config.json')) as f:
            data = json.load(f)

        self.start_date = data['start_date']
        self.end_date = data['end_date']

        self.option = args.option
        self.category = args.category
        os.makedirs(self.pwd + '/result', exist_ok=True)
        os.makedirs(self.pwd + '/log', exist_ok=True)

        all_in = rqdatac.all_instruments()
        stock_pool = pd.read_excel(self.pwd + 'stock_pool.xlsx')
        all_in = pd.merge(stock_pool, all_in, left_on='证券名称', right_on='symbol', how='right')
        # 此处dropna会去掉1、ST的股票以及2、米筐中不存在其数据的股票
        all_in = all_in.dropna(subset=['证券代码'])
        # 最终用列表储存所有的可用股票代码
        stock_pool = list(all_in['order_book_id'])
        self.stock_pool = stock_pool

    def main(self):
        if 'backtest' in self.option :
            if self.category == 'selected_factors':
                all_factors = args.input
                self.run_basic_mode(all_factors)

            if self.category == 'selected_folders':
                # 扫描文件夹，找到所有的因子名，回到第一种模式
                all_folders = args.input
                all_factors = []
                for folder in all_folders:
                    all_factors += self.find_factors_in_folder(folder)
                # print(all_factors)
                self.run_basic_mode(all_factors)

            if self.category == 'all':
                # 扫描factor文件夹，得到所有的文件夹，再回到第二种模式
                all_folders = self.scan_factor_folder()
                all_factors = []
                for folder in all_folders:
                    all_factors += self.find_factors_in_folder(folder)
                self.run_basic_mode(all_factors)

        if 'analysis' in self.option:
            if self.category == 'selected_factors':
                all_factors = args.input
                self.run_analysis(all_factors)

            if self.category == 'selected_folders':
                all_folders = args.input
                all_factors = []
                for folder in all_folders:
                    all_factors += self.find_factors_in_folder(folder)
                self.run_analysis(all_factors)

            if self.category == 'all':
                # 扫描factor文件夹，得到所有的文件夹，再回到第二种模式
                all_folders = self.scan_factor_folder()
                all_factors = []
                for folder in all_folders:
                    all_factors += self.find_factors_in_folder(folder)
                self.run_analysis(all_factors)

        if 'report' in self.option:
            if self.category == 'selected_factors':
                all_factors = args.input
                self.factor_report(all_factors)

            if self.category == 'selected_folders':
                all_folders = args.input
                all_factors = []
                for folder in all_folders:
                    all_factors += self.find_factors_in_folder(folder)
                self.factor_report(all_factors)

            if self.category == 'all':
                # 扫描factor文件夹，得到所有的文件夹，再回到第二种模式
                all_folders = self.scan_factor_folder()
                all_factors = []
                for folder in all_folders:
                    all_factors += self.find_factors_in_folder(folder)
                self.factor_report(all_factors)

        #     run_analysis(all_factors)
        # if roption.report:
        #     run_report(all_factors)

    # ===================================
    # 最基础的单元是处理每个因子，抓住这个本质
    def run_basic_mode(self, all_factors):
        # 单个模式
        for factor_name in all_factors:
            self.process_factor(factor_name)

        # !!! 在此处改成multiprocess !!!
        # if __name__ == '__main__':
        #     with multiprocessing.Pool(processes=4) as pool:
        #         pool.map(self.process_factor, all_factors)

    # 多进程跑这个函数即可
    def process_factor(self, factor_name):
        # 首先找到因子的分类

        # 1. 检查factor相关文件是否存在
        if not self.factor_exist(factor_name):
            print("Factor " + factor_name + " 文件不存在")
            return

        # 2. 检查factor的输出csv
        # 文件存在，则
        classification = self.factor_classification(factor_name)
        self.factor_output_dirs_check(factor_name, classification)
        if self.factor_net_value_existed(factor_name, classification):

            existed_nv_df = pd.read_csv(
                os.path.join(self.pwd, 'result', classification, factor_name, 'csv', 'factor_value',
                             factor_name + '_factor_value.csv'), index_col=0)
            backtest_started_date = existed_nv_df.index[-1]
            # 如果已经是最新
            # print("".join(backtest_started_date.split('-'))[:8],'|||',self.end_date)
            if "".join(backtest_started_date.split('-'))[:8] == self.end_date:
                print(factor_name + '已经是最新')
                return
            # 如果不是最新
            print('因子' + factor_name + '不是最新，读取后计算')
            factor_df = existed_nv_df.append(
                self.run_factor(factor_name, classification, backtest_started_date, self.end_date).iloc[1:])

        # 文件不存在，则从头开始读
        if not self.factor_net_value_existed(factor_name, classification):
            print('因子' + factor_name + '不存在，从头开始计算')
            factor_df = self.run_factor(factor_name, classification, self.start_date, self.end_date)

        factor_df.to_csv(os.path.join(self.pwd, 'result', classification, factor_name, 'csv', 'factor_value',
                                      factor_name + '_factor_value.csv'))
        print('因子' + factor_name + '已经计算完毕并输出csv文件')

    def factor_exist(self, factor_name):
        factor_py_name = factor_name + '.py'
        for root, dirs, files in os.walk(os.path.join(self.pwd, 'factor')):

            if factor_py_name in files:
                return 1
        return 0

    def factor_net_value_existed(self, factor_name, classification):

        path = os.path.join(self.pwd, 'result', classification, factor_name, 'csv', 'factor_value')
        if factor_name + '_factor_value.csv' not in os.listdir(path):
            return 0
        return 1

    def run_factor(self, factor_name, classification, start_date, end_date):
        # 跑因子值，存成文件
        factor_full_path_module = 'factor.' + classification + '.' + factor_name
        module = importlib.import_module(factor_full_path_module)

        result = getattr(module, 'main')()
        try:
            df = execute_factor(result, self.stock_pool, start_date, end_date)
            return df
        except TypeError:
            print('因子' + factor_name + '返回值为none，若是子定义因子请检查定义，若是调用米筐内置因子请检查是否存在此因子')
            with open(os.path.join(self.pwd, "test.txt"), "w") as f:
                f.write(factor_name + '  , ')

    def factor_classification(self, factor_name):
        factor_py_name = factor_name + '.py'
        for root, dirs, files in os.walk(os.path.join(self.pwd, 'factor')):
            if factor_py_name in files:
                return os.path.basename(root)

    def find_factors_in_folder(self, folder):
        factor_temp_list = []
        folder_path = os.path.join(self.pwd, 'factor', folder)
        if folder not in os.listdir(os.path.join(self.pwd, 'factor/')):
            print(folder + '文件夹不存在')
        else:
            print(folder + '文件夹已经找到，开始扫描')
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    if file.endswith('.py'):
                        factor_temp_list.append(file[:-3])
        return factor_temp_list

    def scan_factor_folder(self):
        return os.listdir(os.path.join(self.pwd, 'factor'))

    def factor_output_dirs_check(self, factor_name, classification):

        os.makedirs(self.pwd + '/result/' + classification, exist_ok=True)
        os.makedirs(self.pwd + '/result/' + classification + '/' + factor_name + '/jpg', exist_ok=True)
        os.makedirs(self.pwd + '/result/' + classification + '/' + factor_name + '/csv', exist_ok=True)
        os.makedirs(self.pwd + '/result/' + classification + '/' + factor_name + '/csv/' + 'ic_analysis', exist_ok=True)
        os.makedirs(self.pwd + '/result/' + classification + '/' + factor_name + '/csv/' + 'quantile_analysis',
                    exist_ok=True)
        os.makedirs(self.pwd + '/result/' + classification + '/' + factor_name + '/csv/' + 'return_analysis',
                    exist_ok=True)
        os.makedirs(self.pwd + '/result/' + classification + '/' + factor_name + '/csv/' + 'factor_value',
                    exist_ok=True)  # 此处dropna会去掉1、ST的股票以及2、米筐中不存在其数据的股票

    def run_analysis(self, factors):
        """
        分析因子列表
        :param factors:
        :return:
        """

        for factor in factors:
            self.unit_factor_analysis(factor)

    def unit_factor_analysis(self, factor):
        file_name = factor + '_factor_value.csv'
        classifcation = self.factor_classification(factor)
        file_path = os.path.join(self.pwd, 'result', classifcation, factor)
        csv_path = os.path.join(self.pwd, 'result', classifcation, factor, 'csv', 'factor_value')
        if file_name not in os.listdir(csv_path):
            print('因子'+factor+'净值文件不存在')
            return
        else:
            print('因子'+factor+'净值文件已找到并读取，开始分析')
            df = pd.read_csv(os.path.join(csv_path, factor + '_factor_value.csv'), index_col=0)
            df.index = pd.to_datetime(df.index)
            lib.OutputResult(file_path, self.pwd).factor_analysis(df, factor)
            print('因子'+factor+'已经完成分析')

    def factor_report(self,factors):
        try:
            shutil.rmtree(os.path.join(self.pwd, 'result', 'temp_dir'))
        except:
            pass
        os.makedirs(os.path.join(self.pwd, 'result', 'temp_dir'))
        for factor in factors:
            self.unit_factor_report(factor)
        lib.OutputResult(factor_result_path = '',pwd = self.pwd).generate_report()
        print('报告已经生成')

    def unit_factor_report(self,factor):
        classifcation = self.factor_classification(factor)
        factor_result_path = os.path.join(self.pwd, 'result',classifcation,factor )
        try:
            lib.OutputResult(factor_result_path = factor_result_path,pwd = self.pwd).temp_generation(factor)
            print('因子'+factor+'分析文件读取并生成完毕')
        except FileNotFoundError:
            print('因子'+ factor+'分析文件不存在，请先使用analysis模式生成分析文件')




start_time = time.time()
print('程序开始执行')
rqdatac.init()
parser = argparse.ArgumentParser()

parser.add_argument('--category', type=str, help='execute_category')
parser.add_argument('--option', type=str, nargs='+', help='execute_option')
parser.add_argument('--input', type=str, nargs='+', help='factors_or_folders_input')
#parser.add_argument('--option', choices={"0", "1", ...})

args = parser.parse_args()

FactorRun().main()
end_time = time.time()
print('程序执行完毕')
print('用时为' + str(end_time - start_time) + 's')
