from rqfactor import *


def main():
    mean = (Factor('net_profit_mrq_0') / Factor('net_profit_mrq_1') + Factor('net_profit_mrq_1') / Factor(
        'net_profit_mrq_2') + Factor('net_profit_mrq_2') / Factor('net_profit_mrq_3') + Factor(
        'net_profit_mrq_3') / Factor('net_profit_mrq_4') + Factor('net_profit_mrq_4') / Factor(
        'net_profit_mrq_5') + Factor('net_profit_mrq_5') / Factor('net_profit_mrq_6') + Factor(
        'net_profit_mrq_6') / Factor('net_profit_mrq_7') + Factor('net_profit_mrq_7') / Factor(
        'net_profit_mrq_8'))/8
    rolling_0 = Factor('net_profit_mrq_0') / Factor('net_profit_mrq_1')
    rolling_1 = Factor('net_profit_mrq_1') / Factor('net_profit_mrq_2')
    rolling_2 = Factor('net_profit_mrq_2') / Factor('net_profit_mrq_3')
    rolling_3 = Factor('net_profit_mrq_3') / Factor('net_profit_mrq_4')
    rolling_4 = Factor('net_profit_mrq_4') / Factor('net_profit_mrq_5')
    rolling_5 = Factor('net_profit_mrq_5') / Factor('net_profit_mrq_6')
    rolling_6 = Factor('net_profit_mrq_6') / Factor('net_profit_mrq_7')
    rolling_7 = Factor('net_profit_mrq_7') / Factor('net_profit_mrq_8')
    rolling_list = [rolling_0, rolling_1, rolling_2, rolling_3, rolling_4, rolling_5, rolling_6, rolling_7]
    std = sum((x - mean)**2/7 for x in rolling_list)

    return Factor('net_profit_mrq_0') - (Factor('net_profit_mrq_4')+mean)/std