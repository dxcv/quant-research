

def cal_shock(group1):
    # 重新计算权重，买，统计卖一档的情况
    # group = dfAllStock.groupby('time').get_group(dt.time(9,31,3))
    print(group1.name)
    group = group1.copy()

    # 买的方向，往上打
    group['打穿金额_往上'] = (group['OFFERPX1'] * group['OFFERSIZE1']
                            + group['OFFERPX2'] * group['OFFERSIZE2']
                            + group['OFFERPX3'] * group['OFFERSIZE3']
                            + group['OFFERPX4'] * group['OFFERSIZE4']
                            + group['OFFERPX5'] * group['OFFERSIZE5']) / group['权重(%)'] * 100

    rstDf = pd.DataFrame(index=[0], columns=['金额_1_往上', '股票_1_往上', '该档价差_1_往上',
                                             '对指数的影响_往上',
                                             '金额_1_往下', '股票_1_往下',  '该档价差_1_往下',
                                             '对指数的影响_往下',])

    minIndex_up = group.loc[group['打穿金额_往上'] != 0,'打穿金额_往上'].idxmin()
    minMoney_up = group.loc[group['打穿金额_往上'] != 0, '打穿金额_往上'].min()
    rstDf.at[0, '金额_1_往上'] = minMoney_up
    rstDf.at[0, '股票_1_往上'] = group.at[minIndex_up, 'CODE']
    rstDf.at[0, '该档价差_1_往上'] \
        = (group.at[minIndex_up, 'OFFERPX5'] / (0.5 * group.at[minIndex_up, 'OFFERPX1'] + 0.5 * group.at[minIndex_up, 'BIDPX1']) - 1) * 100

    import numpy as np
    group['档位_往上'] = np.where(group['OFFERPX1'] * group['OFFERSIZE1'] > minMoney_up * group['权重(%)'] / 100, 1,
                         np.where(group['OFFERPX1'] * group['OFFERSIZE1'] + group['OFFERPX2'] * group['OFFERSIZE2'] > minMoney_up * group['权重(%)'] / 100, 2,
                                  np.where(group['OFFERPX1'] * group['OFFERSIZE1'] + group['OFFERPX2'] * group['OFFERSIZE2'] + group['OFFERPX3'] * group['OFFERSIZE3'] > minMoney_up * group['权重(%)'] / 100, 3,
                                            np.where(group['OFFERPX1'] * group['OFFERSIZE1'] + group['OFFERPX2'] * group['OFFERSIZE2'] + group['OFFERPX3'] * group['OFFERSIZE3'] + group['OFFERPX4'] * group['OFFERSIZE4'] > minMoney_up * group['权重(%)'] / 100, 4, 5))))
    group['价差_往上'] = np.where(group['档位_往上'] == 1, (group['OFFERPX1'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                  np.where(group['档位_往上'] == 2, (group['OFFERPX2'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                           np.where(group['档位_往上'] == 3, (group['OFFERPX3'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                                    np.where(group['档位_往上'] == 4, (group['OFFERPX4'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                                             (group['OFFERPX5'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100, ))))

    group.loc[group['价差_往上'] < -99, '价差_往上'] = 0
    group.loc[group['价差_往上'] > 99, '价差_往上'] = 0
    rstDf.at[0, '对指数的影响_往上'] = (group['冲击系数(%/指数点)'] * group['价差_往上']).sum()


    # group.to_csv('D:\\tempdata\\group.csv',encoding='gbk')

    # 卖方向，往下打
    group['打穿金额_往下'] = (group['BIDPX1'] * group['BIDSIZE1']
                            + group['BIDPX2'] * group['BIDSIZE2']
                            + group['BIDPX3'] * group['BIDSIZE3']
                            + group['BIDPX4'] * group['BIDSIZE4']
                            + group['BIDPX5'] * group['BIDSIZE5']) / group['权重(%)'] * 100

    minIndex_down = group.loc[group['打穿金额_往下'] != 0,'打穿金额_往下'].idxmin()
    minMoney_down = group.loc[group['打穿金额_往下'] != 0, '打穿金额_往下'].min()
    rstDf.at[0, '金额_1_往下'] = minMoney_down
    rstDf.at[0, '股票_1_往下'] = group.at[minIndex_down, 'CODE']
    rstDf.at[0, '该档价差_1_往下'] \
        = (group.at[minIndex_down, 'BIDPX5'] / (0.5 * group.at[minIndex_down, 'BIDPX1'] + 0.5 * group.at[minIndex_down, 'BIDPX1']) - 1) * 100


    group['档位_往下'] = np.where(group['BIDPX1'] * group['BIDSIZE1'] > minMoney_down * group['权重(%)'] / 100, 1,
                         np.where(group['BIDPX1'] * group['BIDSIZE1'] + group['BIDPX2'] * group['BIDSIZE2'] > minMoney_down * group['权重(%)'] / 100, 2,
                                  np.where(group['BIDPX1'] * group['BIDSIZE1'] + group['BIDPX2'] * group['BIDSIZE2'] + group['BIDPX3'] * group['BIDSIZE3'] > minMoney_down * group['权重(%)'] / 100, 3,
                                            np.where(group['BIDPX1'] * group['BIDSIZE1'] + group['BIDPX2'] * group['BIDSIZE2'] + group['BIDPX3'] * group['BIDSIZE3'] + group['BIDPX4'] * group['BIDSIZE4'] > minMoney_down * group['权重(%)'] / 100, 4, 5))))
    group['价差_往下'] = np.where(group['档位_往下'] == 1, (group['BIDPX1'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                  np.where(group['档位_往下'] == 2, (group['BIDPX2'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                           np.where(group['档位_往下'] == 3, (group['BIDPX3'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                                    np.where(group['档位_往下'] == 4, (group['BIDPX4'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                                             (group['BIDPX5'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100, ))))

    group.loc[group['价差_往下'] > 99, '价差_往下'] = 0
    group.loc[group['价差_往下'] < -99, '价差_往下'] = 0

    rstDf.at[0, '对指数的影响_往下'] = (group['冲击系数(%/指数点)'] * group['价差_往下']).sum()

    # group.to_csv('D:\\tempdata\\group.csv',encoding='gbk')

    return rstDf


# 评估有没有长度不是4740的
if __name__ == '__main__':
    tradeDates = fetch.trade_dates(startTime='2018-01-01')

    rstDf = pd.DataFrame(columns=['DATETIME', 'CODE','NUM'])
    for tDate in tradeDates['DATETIME']:
        # tDate = tradeDate['DATETIME'][0]
        tDate = dt.datetime(2018,9,13)
        print(tDate)
        for code in weightDf['代码']:
            # code = weightDf['代码'].iat[0]
            print(code)
            df = get_tick_high_sz(code, tDate.strftime('%Y-%m-%d'))
            if df[df['TOTALVOLUMETRADE'] != 0].empty:
                rstDf.set_value(index=len(rstDf),col=['DATETIME', 'CODE','NUM'],value=[tDate, code, None])
                continue
            # 整理成每3s一次的频率，空值向后填充
            df.reset_index(['CODE'],inplace=True)
            df = df.resample('3S').ffill()
            df.reset_index(inplace=True)
            df['time'] = df['DATETIME'].apply(lambda x:x.time())
            df = df[((df['time'] >= dt.time(9, 30, 0)) & (df['time'] <= dt.time(11, 30, 00))) |
                        ((df['time'] >= dt.time(13, 0, 3)) & (df['time'] <= dt.time(14, 56, 57)))
                        ]
            if len(df) != 4740:
                rstDf.set_value(index=len(rstDf), col=['DATETIME', 'CODE', 'NUM'], value=[tDate, code, len(df)])


# 计算对指数的影响
if __name__ == '__main__':
    tradeDates = fetch.trade_dates(startTime='2018-08-01', endTime='2018-09-26')
    # 计算对指数的冲击
    dfindexshock = pd.DataFrame()
    for tDate in tradeDates['DATETIME']:  # 共40个交易日
        dfindexshock1 = pd.read_csv(f'D:\\srcdata\\{tDate.strftime("%Y-%m-%d")}.csv', encoding='gbk')
        dfindexshock1['日期'] = tDate
        dfindexshock = pd.concat([dfindexshock, dfindexshock1])
    del dfindexshock1

    dfindexshock = dfindexshock[dfindexshock['代码'].isin(weightDf['代码'])]  # 只有25个央企调整指数的股票在深成指里面
    dfindexshock.set_index(['日期', '代码'], inplace=True)
    dfindexshock = dfindexshock[['简称', '涨跌幅(%)', '指数贡献点']]
    dfindexshock['涨跌幅(%)'] = dfindexshock['涨跌幅(%)'].apply(lambda x: 0 if x == '--' else float(x))

    # 计算股票的变动（%）对指数的影响（点），没有截距，用OLS模型估计
    import statsmodels.api as sm

    weightDf['冲击系数(%/指数点)'] = None
    for locI, code in enumerate(weightDf['代码']):
        # code = '002415.SZ'
        # locI = 1
        try:
            linmodel = sm.OLS(dfindexshock.loc[(slice(None), code),]['指数贡献点'].values,
                              dfindexshock.loc[(slice(None), code),]['涨跌幅(%)'].values).fit()
        except KeyError:
            continue
        else:
            # linmodel.summary()
            weightDf.iat[locI, weightDf.columns.get_loc('冲击系数(%/指数点)')] = linmodel.params[0]
    weightDf['冲击系数(%/指数点)'].fillna(0, inplace=True)
    del dfindexshock
    # 计算每天的情况
    # tradeDate = fetch.trade_dates(startTime='2018-01-01')
    # tDate = tradeDate['DATETIME'].iat[0]
    # tDate = dt.datetime(2018,8,20)
    # 这里只估计8月份开始
    allMinDf = pd.DataFrame()
    for tDate in tradeDates['DATETIME']:
        print(tDate)
        # 保存所有股票该日tick数据，不含停牌的股票
        dfAllStock = pd.DataFrame()
        for code in weightDf['代码']:
            # code = weightDf['代码'].iat[0]
            print(code)
            df = get_tick_high_sz(code, tDate.strftime('%Y-%m-%d'))
            if df[df['TOTALVOLUMETRADE'] != 0].empty:
                print(code + '停牌')
                continue
            # 整理成每3s一次的频率，空值向后填充
            df.reset_index(['CODE'],inplace=True)
            df = df.resample('3S').ffill()
            df.reset_index(inplace=True)
            df['time'] = df['DATETIME'].apply(lambda x:x.time())
            df = df[((df['time'] >= dt.time(9, 30, 0)) & (df['time'] <= dt.time(11, 30, 00))) |
                        ((df['time'] >= dt.time(13, 0, 3)) & (df['time'] <= dt.time(14, 56, 57)))
                        ]

            # 汇集在一个df中
            dfAllStock = pd.concat([dfAllStock, df])


        # 计算各个项目，停牌的不投入钱，权重重新算
        dfAllStock = pd.merge(dfAllStock, weightDf, how='left', left_on='CODE', right_on='代码')
        dfAllStock.sort_values(by=['time', 'CODE'], ascending=True, inplace=True)

        # group1 = dfAllStock.groupby('time').get_group(dt.time(11,21,39))

        oneDayStat = dfAllStock.groupby('time').apply(cal_shock)
        oneDayStat['date'] = tDate
        oneDayStat.reset_index(level=1, drop=True, inplace=True)
        oneDayStat.reset_index(inplace=True)
        allMinDf = pd.concat([allMinDf, oneDayStat])

        # allMinDf.to_csv('D:\\rstdata\\bidoffer5_index_impact.csv',encoding='gbk')


        allMinDf['对指数的影响_往上'].max() # 3.32
        allMinDf['对指数的影响_往下'].min() # -2.61


# 深圳和上海的tick数据结构是不一样的，深圳的tick数据一定是3s的整数倍，先做深圳的
# 具体计算，买卖一起统计


from tools.tinytools import stock_related
import pyodbc
import pandas as pd
import pandas.io.sql as sql
import datetime as dt
from collections import Counter
from tools.data import fetch


def get_tick_high_sz(stockCode, dateStr):

    # stockCode = stock_related.convert_2_normalcode(stockCode)[:6]
    if stockCode[:2] == '11' or stockCode[:2] == '13':
        mktType = 'SH'
    elif stockCode[:2] == '12':
        mktType = 'SZ'
    else:
        print('请检查输入的代码！')

    dateStr = dateStr.replace('-','')
    if len(dateStr) != 8:
        print('请检查输入的日期，格式为\'20161124\'或者\'2016-11-24\'形式！')
    # select * from dbo.f_getSHL1Market('20181031', '601857')
    fetchCmd = f'SELECT SecurityID,TradeTime,PreClosePx,OpenPx,HighPx,LowPx,LastPx,' \
               f'BidSize1, BidPx1, BidSize2, BidPx2, BidSize3, BidPx3, BidSize4, BidPx4, BidSize5, BidPx5, ' \
               f'OfferSize1,OfferPx1,OfferSize2,OfferPx2,OfferSize3,OfferPx3,OfferSize4,OfferPx4,OfferSize5,OfferPx5,' \
               f'NumTrades,TotalVolumeTrade,TotalValueTrade ' \
               f'FROM dbo.f_get{mktType}L1Market(\'{dateStr}\',\'{stockCode}\')'

    with pyodbc.connect('DSN=HIGH; PWD=password', charset='gbk') as HIGHmssqlConn:
        df = sql.read_sql(fetchCmd,HIGHmssqlConn)

    if df.empty:
        return df

    df.rename(columns=dict(zip(df.columns,[colName.upper() for colName in df.columns])), inplace=True)
    df.rename(columns={'SecurityID'.upper():'CODE'},inplace=True)
    df = df[df['TRADETIME'] != 0.0]

    df['DATETIME'] = df['TradeTime'.upper()].\
        apply(lambda x:dt.datetime.strptime(dateStr+':%06d'%x,'%Y%m%d:%H%M%S'))
    # df['CODE'] = df['CODE'] + '.' + mktType

    df.drop_duplicates(inplace=True)

    # 看去重之后还有没有同一个时点有两根不同的线
    counterDF = pd.DataFrame(pd.Series(dict(Counter(df['TRADETIME']))))
    counterDF.reset_index(inplace=True)
    counterDF.rename(columns={0: 'counter'}, inplace=True)
    rstDF = pd.merge(counterDF, df, how='outer', left_on='index', right_on='TRADETIME')
    del counterDF

    # 将重复的而且交易量不为0的那些单独拿出来，和其他不重复的组合成新的
    if not rstDF[rstDF['counter'] != 1.0].empty:
        raise ValueError('同一个时点有两个不同的线！！')

    # 开盘前值保留最后一根tick
    df.index = range(len(df))
    df = df.iloc[df[df['TRADETIME'] >= 93000.0].index[0] - 1:]

    if df.empty:
        print('%s %s没有数据，将输出空DataFrame！' % (dateStr,stockCode))

    df.drop(labels = ['TRADETIME'],axis = 1,inplace=True)
    df.set_index(['DATETIME','CODE'],inplace=True)
    df.sort_index(level=0,inplace=True)
    return df


def cal_tick_stat_bidoffer5_revised(group1):
    # 重新计算权重，买，统计卖一档的情况
    # group = dfAllStock.groupby('time').get_group(dt.time(9,31,3))
    print(group1.name)
    group = group1.copy()
    group['权重(%)'] = group['权重(%)'] / group['权重(%)'].sum() * 100
    # 买的方向，往上打
    group['打穿金额_往上'] = (group['OFFERPX1'] * group['OFFERSIZE1']
                            + group['OFFERPX2'] * group['OFFERSIZE2']
                            + group['OFFERPX3'] * group['OFFERSIZE3']
                            + group['OFFERPX4'] * group['OFFERSIZE4']
                            + group['OFFERPX5'] * group['OFFERSIZE5']) / group['权重(%)'] * 100

    rstDf = pd.DataFrame(index=[0], columns=['金额_1_往上', '股票_1_往上', '该档挂单_1_往上', '平均挂单_1_往上', '该档价差_1_往上',
                                             '最大价差_往上','最大价差股票_往上','该票到达档位_往上',
                                             '金额_1_往下', '股票_1_往下', '该档挂单_1_往下', '平均挂单_1_往下', '该档价差_1_往下',
                                             '最大价差_往下', '最大价差股票_往下', '该票到达档位_往下',])

    minIndex_up = group.loc[group['打穿金额_往上'] != 0,'打穿金额_往上'].idxmin()
    minMoney_up = group.loc[group['打穿金额_往上'] != 0, '打穿金额_往上'].min()
    rstDf.at[0, '金额_1_往上'] = minMoney_up  # 除零之外的最小金额，零代表涨停了
    rstDf.at[0, '股票_1_往上'] = group.at[minIndex_up, 'CODE']  # 取得最小金额的股票
    rstDf.at[0, '该档挂单_1_往上'] = group.loc[minIndex_up,
                                      ['OFFERSIZE1','OFFERSIZE2','OFFERSIZE3','OFFERSIZE4','OFFERSIZE5']].sum()  # 该股票的五档卖单
    rstDf.at[0, '平均挂单_1_往上'] = group.loc[minIndex_up,
                                      ['OFFERSIZE1', 'OFFERSIZE2', 'OFFERSIZE3', 'OFFERSIZE4', 'OFFERSIZE5',
                                       'BIDSIZE5', 'BIDSIZE4', 'BIDSIZE3', 'BIDSIZE2', 'BIDSIZE1', ]].mean()  # 这个其实没有用到
    rstDf.at[0, '该档价差_1_往上'] \
        = (group.at[minIndex_up, 'OFFERPX5'] / (0.5 * group.at[minIndex_up, 'OFFERPX1'] + 0.5 * group.at[minIndex_up, 'BIDPX1']) - 1) * 100

    # 下面在最小金额下各股票可达档位
    import numpy as np
    group['档位_往上'] = np.where(group['OFFERPX1'] * group['OFFERSIZE1'] > minMoney_up * group['权重(%)'] / 100, 1,
                         np.where(group['OFFERPX1'] * group['OFFERSIZE1'] + group['OFFERPX2'] * group['OFFERSIZE2'] > minMoney_up * group['权重(%)'] / 100, 2,
                                  np.where(group['OFFERPX1'] * group['OFFERSIZE1'] + group['OFFERPX2'] * group['OFFERSIZE2'] + group['OFFERPX3'] * group['OFFERSIZE3'] > minMoney_up * group['权重(%)'] / 100, 3,
                                            np.where(group['OFFERPX1'] * group['OFFERSIZE1'] + group['OFFERPX2'] * group['OFFERSIZE2'] + group['OFFERPX3'] * group['OFFERSIZE3'] + group['OFFERPX4'] * group['OFFERSIZE4'] > minMoney_up * group['权重(%)'] / 100, 4, 5))))
    group['价差_往上'] = np.where(group['档位_往上'] == 1, (group['OFFERPX1'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                  np.where(group['档位_往上'] == 2, (group['OFFERPX2'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                           np.where(group['档位_往上'] == 3, (group['OFFERPX3'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                                    np.where(group['档位_往上'] == 4, (group['OFFERPX4'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                                             (group['OFFERPX5'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100, ))))

    rstDf.at[0, '最大价差_往上'] = group.loc[group['价差_往上']<99, '价差_往上'].max()  # 跌停的情况上面五档
    rstDf.at[0, '最大价差股票_往上'] = group.at[group.loc[group['价差_往上']<99, '价差_往上'].idxmax(), 'CODE']
    rstDf.at[0, '该票到达档位_往上'] = group.at[group.loc[group['价差_往上']<99, '价差_往上'].idxmax(), '档位_往上']

    # group.to_csv('D:\\tempdata\\group.csv',encoding='gbk')

    # 卖方向，往下打
    group['打穿金额_往下'] = (group['BIDPX1'] * group['BIDSIZE1']
                            + group['BIDPX2'] * group['BIDSIZE2']
                            + group['BIDPX3'] * group['BIDSIZE3']
                            + group['BIDPX4'] * group['BIDSIZE4']
                            + group['BIDPX5'] * group['BIDSIZE5']) / group['权重(%)'] * 100

    minIndex_down = group.loc[group['打穿金额_往下'] != 0,'打穿金额_往下'].idxmin()
    minMoney_down = group.loc[group['打穿金额_往下'] != 0, '打穿金额_往下'].min()
    rstDf.at[0, '金额_1_往下'] = minMoney_down
    rstDf.at[0, '股票_1_往下'] = group.at[minIndex_down, 'CODE']
    rstDf.at[0, '该档挂单_1_往下'] = group.loc[minIndex_down,
                                      ['BIDSIZE1','BIDSIZE2','BIDSIZE3','BIDSIZE4','BIDSIZE5']].sum()
    rstDf.at[0, '平均挂单_1_往下'] = group.loc[minIndex_down,
                                      ['OFFERSIZE1', 'OFFERSIZE2', 'OFFERSIZE3', 'OFFERSIZE4', 'OFFERSIZE5',
                                       'BIDSIZE5', 'BIDSIZE4', 'BIDSIZE3', 'BIDSIZE2', 'BIDSIZE1', ]].mean()
    rstDf.at[0, '该档价差_1_往下'] \
        = (group.at[minIndex_down, 'BIDPX5'] / (0.5 * group.at[minIndex_down, 'BIDPX1'] + 0.5 * group.at[minIndex_down, 'BIDPX1']) - 1) * 100


    group['档位_往下'] = np.where(group['BIDPX1'] * group['BIDSIZE1'] > minMoney_down * group['权重(%)'] / 100, 1,
                         np.where(group['BIDPX1'] * group['BIDSIZE1'] + group['BIDPX2'] * group['BIDSIZE2'] > minMoney_down * group['权重(%)'] / 100, 2,
                                  np.where(group['BIDPX1'] * group['BIDSIZE1'] + group['BIDPX2'] * group['BIDSIZE2'] + group['BIDPX3'] * group['BIDSIZE3'] > minMoney_down * group['权重(%)'] / 100, 3,
                                            np.where(group['BIDPX1'] * group['BIDSIZE1'] + group['BIDPX2'] * group['BIDSIZE2'] + group['BIDPX3'] * group['BIDSIZE3'] + group['BIDPX4'] * group['BIDSIZE4'] > minMoney_down * group['权重(%)'] / 100, 4, 5))))
    group['价差_往下'] = np.where(group['档位_往下'] == 1, (group['BIDPX1'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                  np.where(group['档位_往下'] == 2, (group['BIDPX2'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                           np.where(group['档位_往下'] == 3, (group['BIDPX3'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                                    np.where(group['档位_往下'] == 4, (group['BIDPX4'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100,
                                                             (group['BIDPX5'] * 2 / (group['OFFERPX1'] + group['BIDPX1']) - 1) * 100, ))))

    rstDf.at[0, '最大价差_往下'] = group.loc[group['价差_往下']>-99, '价差_往下'].min()
    rstDf.at[0, '最大价差股票_往下'] = group.at[group.loc[group['价差_往下']>-99, '价差_往下'].idxmin(), 'CODE']
    rstDf.at[0, '该票到达档位_往下'] = group.at[group.loc[group['价差_往下']>-99, '价差_往下'].idxmin(), '档位_往下']

    return rstDf


if __name__ == '__main__':

    rstFilepath = 'D:\\rstdata\\冲击评估\\中证可转债及可交换债指数_20190201权重_5档评估.xls'
    weightDf = pd.read_excel('D:\\srcdata\\转债指数\\中证可转债及可交换债指数_20190201权重.xls', encoding='gbk')
    weightDfSz = weightDf[['债券名称\nBond Name',
                           '深市代码\nCode in Shenzhen', '深市名称\nName in Shenzhen',
                           '信用类型\nCredit Type', '权重因子', '权重(%)']]
    weightDfSz = weightDfSz[weightDfSz['深市代码\nCode in Shenzhen'] > 0]
    weightDfSz['深市代码\nCode in Shenzhen'] = weightDfSz['深市代码\nCode in Shenzhen'].apply(lambda x:str(int(x)))
    # 深圳权重27.817
    # weightDfSz['权重(%)'].sum()

    tradeDates = fetch.trade_dates(startTime='2019-01-01')

    allMinDf = pd.DataFrame()
    for tDate in tradeDates['DATETIME']:
        # tDate = tradeDates['DATETIME'].iat[-1]
        # tDate = dt.datetime(2018,3,16)
        print(tDate)
        # 保存所有股票该日tick数据，不含停牌的股票
        dfAllStock = pd.DataFrame()
        for code in weightDfSz['深市代码\nCode in Shenzhen']:
            # code = weightDfSz['深市代码\nCode in Shenzhen'].iat[0]
            print(code)
            df = get_tick_high_sz(code, tDate.strftime('%Y-%m-%d'))

            if df.empty or df[df['TOTALVOLUMETRADE'] != 0].empty:
                print(code + '停牌或者没有上市')
                continue
            # 整理成每3s一次的频率，空值向后填充
            df.reset_index(['CODE'],inplace=True)
            df = df.resample('3S').ffill()
            df.reset_index(inplace=True)
            df['time'] = df['DATETIME'].apply(lambda x:x.time())
            df = df[((df['time'] >= dt.time(9, 30, 0)) & (df['time'] <= dt.time(11, 30, 00))) |
                        ((df['time'] >= dt.time(13, 0, 3)) & (df['time'] <= dt.time(14, 56, 57)))
                        ]

            # 汇集在一个df中
            dfAllStock = pd.concat([dfAllStock, df])


        # 计算各个项目，停牌的不投入钱，权重重新算
        dfAllStock = pd.merge(dfAllStock, weightDfSz, how='left', left_on='CODE', right_on='深市代码\nCode in Shenzhen')
        dfAllStock.sort_values(by=['time', 'CODE'], ascending=True, inplace=True)

        # group1 = dfAllStock.groupby('time').get_group(dt.time(14,31,3))
        oneDayStat = dfAllStock.groupby('time').apply(cal_tick_stat_bidoffer5_revised)
        oneDayStat['date'] = tDate
        oneDayStat.reset_index(level=1, drop=True, inplace=True)
        oneDayStat.reset_index(inplace=True)
        allMinDf = pd.concat([allMinDf, oneDayStat])

    # 保存数据
    allMinDf.to_csv('D:\\rstdata\\冲击评估\\中证可转债及可交换债指数_20190201权重_5档评估.csv', encoding='gbk', index=False)

    # 分析数据
    rstDataDf = pd.DataFrame(columns=['95%不打穿金额（元）', '平均不打穿金额（元）',
                                              '95%价差（%）', '平均价差（%）', '最大价差（%）',
                                              '打穿和冲击重合的概率（%）'], index=['买入', '卖出'])
    # allMinDf = pd.read_csv('D:\\rstdata\\冲击评估\\中证可转债及可交换债指数_20190201权重_5档评估.csv', encoding='gbk',)
    # 往上的情形
    # 95%情况的都不打穿的金额
    allMinDf.sort_values('金额_1_往上',inplace=True)
    allMinDf['rankRatio'] = range(1, len(allMinDf)+1)
    allMinDf['rankRatio'] = allMinDf['rankRatio'] / (len(allMinDf)+1) * 100
    rstDataDf.at['买入', '95%不打穿金额（元）'] \
        = allMinDf[allMinDf['rankRatio'] > 5].iat[0, allMinDf.columns.get_loc('金额_1_往上')]
    rstDataDf.at['买入', '平均不打穿金额（元）'] \
        = allMinDf['金额_1_往上'].mean()

    # 打穿情况出现较多的股票，2179，2025，2415，1979，2916，0423
    from collections import Counter
    upcountCode = pd.Series(Counter(allMinDf['股票_1_往上']))
    upcountCode.sort_values(inplace=True,ascending=False)
    upcountCode = pd.DataFrame(upcountCode)
    upcountCode.rename(columns={0:'股票_1_往上'},inplace=True)
    upcountCode['频次比例'] = upcountCode['股票_1_往上'] / upcountCode['股票_1_往上'].sum() * 100

    # 打穿情况下最大的价差，5%
    rstDataDf_jiacha = pd.DataFrame(columns=['95%不打穿金额（元）', '平均不打穿金额（元）'], index=['买入', '卖出'])
    allMinDf.sort_values('最大价差_往上',inplace=True,ascending=False)
    allMinDf['rankRatio'] = range(1, len(allMinDf)+1)
    allMinDf['rankRatio'] = allMinDf['rankRatio'] / (len(allMinDf)+1) * 100
    rstDataDf.at['买入', '95%价差（%）'] \
        = allMinDf[allMinDf['rankRatio'] > 5].iat[0, allMinDf.columns.get_loc('最大价差_往上')]
    rstDataDf.at['买入', '平均价差（%）'] \
        = allMinDf['最大价差_往上'].mean()
    rstDataDf.at['买入', '最大价差（%）'] \
        = allMinDf['最大价差_往上'].max()

    # 打穿情况下最大价差的股票，出现的最大价差和打穿经常重合，2179，2025，2106，2013，2268，2140
    upSpreadcountCode = pd.Series(Counter(allMinDf['最大价差股票_往上']))
    upSpreadcountCode.sort_values(inplace=True,ascending=False)
    upSpreadcountCode = pd.DataFrame(upSpreadcountCode)
    upSpreadcountCode.rename(columns={0:'最大价差股票_往上'},inplace=True)
    upSpreadcountCode['频次比例'] = upSpreadcountCode['最大价差股票_往上'] / upSpreadcountCode['最大价差股票_往上'].sum() * 100

    # 打穿和冲击重合的概率
    rstDataDf.at['买入', '打穿和冲击重合的概率（%）'] \
        = len(allMinDf[allMinDf['最大价差股票_往上'] == allMinDf['股票_1_往上']]) / len(allMinDf) * 100

    # 向下的情形
    # 95%情况的都不打穿的金额
    allMinDf.sort_values('金额_1_往下', inplace=True)
    allMinDf['rankRatio'] = range(1, len(allMinDf) + 1)
    allMinDf['rankRatio'] = allMinDf['rankRatio'] / (len(allMinDf) + 1) * 100
    rstDataDf.at['卖出', '95%不打穿金额（元）'] \
        = allMinDf[allMinDf['rankRatio'] > 5].iat[0, allMinDf.columns.get_loc('金额_1_往下')]
    rstDataDf.at['卖出', '平均不打穿金额（元）'] \
        = allMinDf['金额_1_往下'].mean()


    # 打穿情况出现较多的股票，2179，2025，2415，2916，1979，0423
    downcountCode = pd.Series(Counter(allMinDf['股票_1_往下']))
    downcountCode.sort_values(inplace=True, ascending=False)
    downcountCode = pd.DataFrame(downcountCode)
    downcountCode.rename(columns={0: '股票_1_往下'}, inplace=True)
    downcountCode['频次比例'] = downcountCode['股票_1_往下'] / downcountCode['股票_1_往下'].sum() * 100

    # 打穿情况下最大的价差，5%
    allMinDf.sort_values('最大价差_往下',inplace=True,ascending=True)
    allMinDf['rankRatio'] = range(1, len(allMinDf)+1)
    allMinDf['rankRatio'] = allMinDf['rankRatio'] / (len(allMinDf)+1) * 100
    rstDataDf.at['卖出', '95%价差（%）'] \
        = allMinDf[allMinDf['rankRatio'] > 5].iat[0, allMinDf.columns.get_loc('最大价差_往下')]
    rstDataDf.at['卖出', '平均价差（%）'] \
        = allMinDf['最大价差_往下'].mean()
    rstDataDf.at['卖出', '最大价差（%）'] \
        = allMinDf['最大价差_往下'].min()


    # 打穿情况下最大价差的股票，出现的最大价差和打穿经常重合，2179，2025，2106，2013，2314，2140
    downSpreadcountCode = pd.Series(Counter(allMinDf['最大价差股票_往下']))
    downSpreadcountCode.sort_values(inplace=True,ascending=False)
    downSpreadcountCode = pd.DataFrame(downSpreadcountCode)
    downSpreadcountCode.rename(columns={0:'最大价差股票_往下'},inplace=True)
    downSpreadcountCode['频次比例'] = downSpreadcountCode['最大价差股票_往下'] / downSpreadcountCode['最大价差股票_往下'].sum() * 100

    # 打穿和冲击重合的概率
    rstDataDf.at['卖出', '打穿和冲击重合的概率（%）'] \
        = len(allMinDf[allMinDf['最大价差股票_往下'] == allMinDf['股票_1_往下']]) / len(allMinDf) * 100

    writer = pd.ExcelWriter(rstFilepath)
    (rstDataDf.T).to_excel(writer, sheet_name='冲击和价差')
    upcountCode.to_excel(writer, sheet_name='往上打')
    upSpreadcountCode.to_excel(writer, sheet_name='往上打', startcol=4)
    downcountCode.to_excel(writer, sheet_name='往下打')
    downSpreadcountCode.to_excel(writer, sheet_name='往下打', startcol=4)
    writer.save()
