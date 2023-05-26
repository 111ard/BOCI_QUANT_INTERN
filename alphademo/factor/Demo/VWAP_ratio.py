from rqfactor import *
from rqfactor.extension import *
from rqfactor.notebook import *
import rqdatac
rqdatac.init()

VWAP = Factor('total_turnover') / Factor('volume')
f = (RANK((VWAP - Factor('close'))) / RANK((VWAP + Factor('close'))))

def main():
    return f


#d1='20190101'
#d2='20200101'

#df = execute_factor(main(),rqdatac.index_components('000300.XSHG', d1),d1,d2)
