import rqdatac
import numpy as np
import pandas as pd
from sympy import *
from functools import partial
import datetime as dt
import matplotlib.pyplot as plt

rqdatac.init()


def daily_return(test, size = 50, n = 5):
    x = symbols('x')
    test = test.reset_index()
    for a in range(2,size):
        xu = test.loc[:a, :].index
        yu = test.loc[:a, :].open
        reg = np.polyfit(xu, yu, n)
        f = ''
        for i in range(len(reg)):
            f = f + str(reg[i]) + '*x**' + str(len(reg) - i - 1) + '+'
        f = f[:-6]
        expr = sympify(f)
        f2 = lambdify(x, diff(expr, x), 'numpy')
        test.loc[a, '一阶导数'] = f2(a)
        f3 = lambdify(x, diff(expr, x, 2), 'numpy')
        test.loc[a, '二阶导数'] = f3(a)
    test['return'] = test.open / test.open.shift(1) - 1
    test.loc[(test.一阶导数 >= 0) & (test.二阶导数 >= 0), '信号'] = '多'
    test.loc[(test.一阶导数 <= 0) & (test.二阶导数 <= 0), '信号'] = '空'
    test.loc[(test.一阶导数 <= 0) & (test.二阶导数 >= 0), '信号'] = '平'
    test.loc[(test.一阶导数 >= 0) & (test.二阶导数 <= 0), '信号'] = '平'
    t = 0
    last = '平'
    for i in range(len(test)):

        if pd.isnull(test.信号.iloc[i]):
            break

        now = test.信号.iloc[i]
        if last != now and now != '平':
            t = t + 1
            start_indicator = i
            signal = now

        if last != now and now == '平':
            t = t + 1
            end_indicator = i
        # print('平了')
        last = now
        #  print(t)
        if t == 2:
            # print(last,now)
            # print(start_indicator,i)
            break
    if t == 2:
        try:
            df = test.loc[start_indicator:end_indicator, 'return']
        except:
            df = test.loc[start_indicator:start_indicator+16, 'return']
        return np.cumprod(df + 1).iloc[-1] - 1
    if t == 1 and signal == '多':
        return np.cumprod(test.loc[start_indicator:, 'return'] + 1).iloc[-1] - 1
    if t == 1 and signal == '空':
        return np.cumprod(-test.loc[start_indicator:, 'return'] + 1).iloc[-1] - 1
    if t == 0:
        return 0



#original_data = rqdatac.get_price('000905.XSHG',start_date = '20170101',end_date = '20230101',frequency = '1m')
#original_data = original_data.reset_index()
#original_data['date'] = original_data['datetime'].apply(lambda x: x.strftime(format ='%Y%m%d'))
def calculate_return(start_date,end_date,n,size,stock):
    original_data = rqdatac.get_price(stock, start_date=start_date, end_date=end_date, frequency='1m')
    original_data = original_data.reset_index()
    original_data['date'] = original_data['datetime'].apply(lambda x: x.strftime(format='%Y%m%d'))
    daily_return_partial  = partial(daily_return, n = n,size =size)
    return_list = original_data.groupby('date').apply(daily_return_partial)
    total_return = np.cumprod(1+return_list -0.0001).iloc[-1] -1
    start_date = dt.datetime.strptime(start_date, "%Y%m%d").date()  ##datetime.date(2018, 1, 6)
    end_date = dt.datetime.strptime(end_date, "%Y%m%d").date()  ##datetime.date(2018, 1, 9)
    # （2）计算两个日期date的天数差
    Days = (end_date - start_date).days
    return (total_return+1)**(365/Days) -1

# test = calculate_return(start_date = '20200101',end_date = '20210101',n =5,size =50,stock ='000905.XSHG')

n_list = [2,3,4,5,6,7]
size_list = [30,40,50,60,70,100,150,200,230]
#n_list = [3,4]
#size_list = [30,40]
final_df = pd.DataFrame(columns = size_list,index = n_list)
for n in n_list:
    for size in size_list:
        final_df.loc[n,size] = calculate_return(start_date = '20190101',end_date = '20220101',n =n,size =size,stock ='000905.XSHG')

for n in n_list:
    plt.plot(final_df.columns, final_df.loc[n,:])

plt.legend(n_list)
plt.show()