from rqfactor import *


def main():
    mean = (Factor('revenue_mrq_0') / Factor('revenue_mrq_1') + Factor('revenue_mrq_1') / Factor(
        'revenue_mrq_2') + Factor('revenue_mrq_2') / Factor('revenue_mrq_3') + Factor(
        'revenue_mrq_3') / Factor('revenue_mrq_4') + Factor('revenue_mrq_4') / Factor(
        'revenue_mrq_5') + Factor('revenue_mrq_5') / Factor('revenue_mrq_6') + Factor(
        'revenue_mrq_6') / Factor('revenue_mrq_7') + Factor('revenue_mrq_7') / Factor(
        'revenue_mrq_8'))/8
    rolling_0 = Factor('revenue_mrq_0') / Factor('revenue_mrq_1')
    rolling_1 = Factor('revenue_mrq_1') / Factor('revenue_mrq_2')
    rolling_2 = Factor('revenue_mrq_2') / Factor('revenue_mrq_3')
    rolling_3 = Factor('revenue_mrq_3') / Factor('revenue_mrq_4')
    rolling_4 = Factor('revenue_mrq_4') / Factor('revenue_mrq_5')
    rolling_5 = Factor('revenue_mrq_5') / Factor('revenue_mrq_6')
    rolling_6 = Factor('revenue_mrq_6') / Factor('revenue_mrq_7')
    rolling_7 = Factor('revenue_mrq_7') / Factor('revenue_mrq_8')
    rolling_list = [rolling_0, rolling_1, rolling_2, rolling_3, rolling_4, rolling_5, rolling_6, rolling_7]
    std = sum((x - mean)**2/7 for x in rolling_list)

    return Factor('revenue_mrq_0') - (Factor('revenue_mrq_4')+mean)/std