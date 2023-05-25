from rqfactor import *


def main():
    return (Factor('net_profit_mrq_0')/Factor('total_equity_mrq_0') - Factor('net_profit_mrq_4')/Factor('total_equity_mrq_4'))/(Factor('net_profit_mrq_4')/Factor('total_equity_mrq_4'))