
from tools.data import fetch
import pandas as pd
import datetime as dt
from dateutil.parser import parse
import numpy as np

tradeDates = fetch.trade_dates(startTime='2018-08-01', endTime='2018-08-31')

dfOrder \
    = pd.read_excel('D:\\srcdata\\历史指令\\201808.XLS')[
    ['日期', '指令序号', '指令状态','指令类型', '基金名称', '证券代码','委托方向','累计成交金额', '操作级别','指令价格(主币种)']
]

dfOrder.drop(index=len(dfOrder)-1,inplace=True)
dfOrder = dfOrder[((dfOrder['委托方向'] == '买入') | (dfOrder['委托方向'] == '卖出'))
                  & (dfOrder['指令状态'] == '有效指令')
                  & (dfOrder['指令类型'] == '个股')
                  & (dfOrder['操作级别'] == '均价')
                  & (dfOrder['指令价格(主币种)'] == '市价') ]
dfOrder['日期'] = dfOrder['日期'].apply(lambda x: parse(x))

# group1 = dfOrder.groupby(['日期', '证券代码']).get_group((dt.datetime(2018,9,3), '000028'))

def _find_date_code(group1):
    if len(group1['委托方向'].unique()) == 1:
        return pd.Series((group1['委托方向'].iat[0], group1['累计成交金额'].sum()), index=('委托方向', '累计成交金额'))
        # return group1[['委托方向', '累计成交金额']].iloc[0]
    else:
        if group1.loc[group1['委托方向'] == group1['委托方向'].unique()[0], '累计成交金额'].sum() > group1.loc[group1['委托方向'] == group1['委托方向'].unique()[1], '累计成交金额'].sum():
            return pd.Series(
                (group1.loc[group1['委托方向'] == group1['委托方向'].unique()[0], '委托方向'].iat[0],
                 group1.loc[group1['委托方向'] == group1['委托方向'].unique()[0], '累计成交金额'].sum()
                 ),
                index=('委托方向', '累计成交金额')
            )
            # return group1.loc[group1['委托方向'] == group1['委托方向'].unique()[0], ['委托方向', '累计成交金额']].iloc[0]
        else:
            return pd.Series(
                (group1.loc[group1['委托方向'] == group1['委托方向'].unique()[1], '委托方向'].iat[0],
                 group1.loc[group1['委托方向'] == group1['委托方向'].unique()[1], '累计成交金额'].sum()
                 ),
                index=('委托方向', '累计成交金额')
            )
            # return group1.loc[group1['委托方向'] == group1['委托方向'].unique()[1], ['委托方向', '累计成交金额']].iloc[0]


dfStock = dfOrder.groupby(['日期', '证券代码']).apply(_find_date_code)
dfStock['TWAP'] = None
dfStock['VWAP'] = None
dfStock['价差_T-V'] = None
dfStock['spread'] = None



for dcIndex in dfStock.index:
    print(dcIndex)
    df = fetch.stock_tick_windDB(dcIndex[1], dcIndex[0].strftime('%Y-%m-%d')).reset_index()

    df['time'] = df['DATETIME'].apply(lambda x: x.time())

    # 去掉开盘和尾盘集合竞价
    df = df[((df['time'] > dt.time(9, 30, 0)) & (df['time'] <= dt.time(11, 30, 00))) |
            ((df['time'] >= dt.time(13, 0, 3)) & (df['time'] <= dt.time(14, 56, 57)))
            ]

    dfStock.at[dcIndex, 'VWAP'] = df['TOTALVALUETRADE'].max() / df['TOTALVOLUMETRADE'].max()
    dfStock.at[dcIndex, 'TWAP'] = pd.Series(np.where(df['OFFERPX1'] == 0, np.nan, df['OFFERPX1'])).mean() \
                                if dfStock.at[dcIndex, '委托方向'] == '买入' \
                                else pd.Series(np.where(df['BIDPX1'] == 0, np.nan, df['BIDPX1'])).mean()
    dfStock.at[dcIndex, '价差_T-V'] = (dfStock.at[dcIndex, 'TWAP'] - dfStock.at[dcIndex, 'VWAP']) / dfStock.at[dcIndex, 'VWAP']


    dfStock.at[dcIndex, 'spread'] \
        = - pd.Series(
        (np.where(df['OFFERPX1'] == 0, np.nan, df['OFFERPX1']) - np.where(df['BIDPX1'] == 0, np.nan, df['BIDPX1']))
        / (np.where(df['OFFERPX1'] == 0, np.nan, df['OFFERPX1']) + np.where(df['BIDPX1'] == 0, np.nan, df['BIDPX1'])) * 2
          ).mean()

dfStock[['价差_T-V', 'spread']].mean()
(dfStock['价差_T-V'] * dfStock['累计成交金额'] / dfStock['累计成交金额'].sum()).sum()
(dfStock['spread'] * dfStock['累计成交金额'] / dfStock['累计成交金额'].sum()).sum()

rstSer = pd.Series({'价差T-V(bp)':dfStock['价差_T-V'].mean() * 10000,
                    'spread(bp)': dfStock['spread'].mean() * 10000,
                    '价差T-V_weight(bp)': (dfStock['价差_T-V'] * dfStock['累计成交金额'] / dfStock['累计成交金额'].sum()).sum() * 10000,
                    'spread_weight(bp)': (dfStock['spread'] * dfStock['累计成交金额'] / dfStock['累计成交金额'].sum()).sum() * 10000,
                    })

rstSer.to_excel('D:\\rstdata\\价差分析\\201808.xlsx', encoding='gbk')

dfStock.to_csv('D:\\rstdata\\价差分析\\201808.csv',encoding='gbk')

