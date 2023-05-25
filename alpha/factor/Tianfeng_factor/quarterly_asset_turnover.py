from rqfactor import *
import rqdatac
rqdatac.init()


def main():
    return Factor('revenue_mrq_0')/Factor('total_assets_mrq_0')

"""factor1 = main()

df = execute_factor(factor1,['000001.XSHE'], start_date = '20230101', end_date = '20230201')"""