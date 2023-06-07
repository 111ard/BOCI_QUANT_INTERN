from rqfactor import *
import rqdatac
from rqfactor.extension import *
import pandas as pd

def main():
    return TS_RANK(Factor('net_profit_mrq_0')/Factor('market_cap_3'),250)




