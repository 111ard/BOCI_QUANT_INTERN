import rqdatac
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


warnings.filterwarnings('ignore')
rqdatac.init()


data = rqdatac.get_price(order_book_ids='000905.XSHG', start_date='20100101', end_date='20230201', frequency='1m')

data1 = data.reset_index().loc[:, ['datetime', 'close']]


def daily_volatility_substration_calculate(daily_df):
    open_price = daily_df.close.iloc[0]
    upward_df = daily_df.loc[daily_df.close>=open_price,:]
    downward_df = daily_df.loc[daily_df.close<open_price,:]
    if len(upward_df) ==1:
        upward_volatility = 0
    else:
        upward_volatility = upward_df.close.pct_change().std()
    if len(downward_df) ==0:
        downward_volatility = 0
    else:
        downward_volatility = downward_df.close.pct_change().std()
    return upward_volatility - downward_volatility


plt.rcParams['font.sans-serif']=['SimHei']
data1['date'] = data1.datetime.apply(lambda x: x.strftime('%Y%m%d'))
daily_volatility = data1.groupby('date').apply(daily_volatility_substration_calculate).apply(pd.Series)
daily_volatility.columns = ['volatility']
daily_return = data1.groupby('date').close.apply(lambda x: x.iloc[-1]).pct_change().apply(pd.Series)
daily_return.columns = ['return']
all_data = daily_volatility.join(daily_return)
all_data['moving_average_10_v'] = all_data.volatility.rolling(11, min_periods = 1).apply(lambda x: x.iloc[:-1].mean())
all_data['moving_average_20_v'] = all_data.volatility.rolling(21, min_periods = 1).apply(lambda x: x.iloc[:-1].mean())
all_data['moving_average_30_v'] = all_data.volatility.rolling(31, min_periods = 1).apply(lambda x: x.iloc[:-1].mean())
print(all_data.corr())

all_data['position_10'] = 0
all_data.loc[all_data.moving_average_10_v >= 0, 'position_10'] = 1
all_data.loc[:, 'strategy_return_ma10'] = all_data['return'] * all_data['position_10']
all_data.loc[:, 'net_value_ma10'] = np.cumprod(all_data.strategy_return_ma10 + 1)

all_data['position_20'] = 0
all_data.loc[all_data.moving_average_20_v >= 0, 'position_20'] = 1
all_data.loc[:, 'strategy_return_ma20'] = all_data['return'] * all_data['position_20']
all_data.loc[:, 'net_value_ma20'] = np.cumprod(all_data.strategy_return_ma20 + 1)

all_data['position_30'] = 0
all_data.loc[all_data.moving_average_30_v >= 0, 'position_30'] = 1
all_data.loc[:, 'strategy_return_ma30'] = all_data['return'] * all_data['position_30']
all_data.loc[:, 'net_value_ma30'] = np.cumprod(all_data.strategy_return_ma30 + 1)

all_data.loc[:, 'csc500_net_value'] = np.cumprod(all_data['return'] + 1)
all_data.index=pd.to_datetime(all_data.index)
plt.plot(all_data.csc500_net_value)
plt.plot(all_data.net_value_ma10)
plt.plot(all_data.net_value_ma20)
plt.plot(all_data.net_value_ma30)
plt.legend(['中证500','剪刀差波动率策略_10日平均','剪刀差波动率策略_20日平均','剪刀差波动率策略_30日平均'])
plt.show()
