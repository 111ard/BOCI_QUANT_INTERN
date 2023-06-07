import pandas as pd
import os

def main(classification):
    pwd = os.path.abspath(__file__)[:-len(os.path.basename(__file__))]
    df = []
    temp_path = pwd + '/result/temp_dir'
    temp_list = os.listdir(temp_path)
    for report_unique in temp_list:
        temp = pd.read_excel(temp_path + '/'+report_unique).T
        df.append(temp)
    df = pd.concat(df).T
    df = df.T.drop_duplicates()
    df = df.T.set_index('Unnamed: 0',drop = True)
    df.T.to_excel(pwd + '/result/summary_' + classification + '.xlsx')
