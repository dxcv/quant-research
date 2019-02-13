
import pandas as pd
from tools.data import fetch
from WindPy import w
from tools.tinytools import pandas_related
import datetime as dt


# dfRatio['占比'] = dfRatio['公司成交金额（元）'] / dfRatio['市场成交金额（元）']

w.start()
dataWind = w.wsd("881001.WI", "amt", "2017-01-01", "2018-12-19", "")

w.stop()

dfmarket = pd.DataFrame(dataWind.Data, index=dataWind.Fields, columns=dataWind.Times).T


# 成交数据
df = pd.read_excel('D:\\srcdata\\公司成交占比\\A股成交量_2017-18.xlsx', parse_dates=[0, ], encoding='gbk')
df.set_index(['日期', '代码'], inplace=True)
# 提出大宗交易
dfblocktrade = pd.read_excel('D:\\srcdata\\公司成交占比\\大宗交易明细.xlsx', parse_dates=[1, ], encoding='gbk')

dfblocktrade1 = dfblocktrade.groupby(['发生日期', '证券代码'])['成交金额'].sum()

df['blocktrade'] = dfblocktrade1
df['blocktrade'].fillna(0, inplace=True)

del dfblocktrade, dfblocktrade1

df['公司成交金额（元）'] = df['公司成交金额（元）'] - df['blocktrade']

# df.loc[df['公司成交金额（元）'] == 0, ['公司成交金额（元）', '市场成交金额（元）']]


dfRatio = df.groupby(['日期'])[['公司成交金额（元）', '市场成交金额（元）']].sum()
dfRatio['市场总金额'] = dfmarket['AMT']
dfRatio['占比'] = dfRatio['公司成交金额（元）'] / dfRatio['市场总金额']

# dfRatio.reset_index(inplace=True)
# dfRatio['year'] = dfRatio['日期'].apply(lambda x: x.year)
# dfRatio.set_index('日期', inplace=True)

#
df2017 = dfRatio[:dt.datetime(2017,12,31)]
df2017.sort_values('公司成交金额（元）', ascending=False, inplace=True)
df2017.iloc[:10]

df2017.sort_values('占比', ascending=False, inplace=True)
df2017.iloc[:10]

#
df2018 = dfRatio[dt.datetime(2017,12,31):]
df2018.sort_values('公司成交金额（元）', ascending=False, inplace=True)
df2018.iloc[:10]

df2018.sort_values('占比', ascending=False, inplace=True)
df2018.iloc[:10]

# 统计个股
# df.set_index('日期', inplace=True)
df2017stock = df[:dt.datetime(2017,12,31)]
df2017stock['个股占比'] = df2017stock['公司成交金额（元）'] / df2017stock['市场成交金额（元）']
df2017stock.sort_values('个股占比', ascending=False, inplace=True)
df2017stock[['公司成交金额（元）', '市场成交金额（元）', '个股占比']].iloc[:23]

#
df2018stock = df[dt.datetime(2017,12,31):]
df2018stock['个股占比'] = df2018stock['公司成交金额（元）'] / df2018stock['市场成交金额（元）']
df2018stock.sort_values('个股占比', ascending=False, inplace=True)
df2018stock[['公司成交金额（元）', '市场成交金额（元）', '个股占比']].iloc[:20]