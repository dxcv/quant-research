
# 第一个划分牛熊的想法
if __name__ == '__main__':
    from tools.data import fetch
    import datetime as dt
    import pandas as pd
    from tools.tinytools import pandas_related, group_method

    df = fetch.index_one('000001.SH', startTime='2000-01-01')

    df['6_month_peak'] = df['HIGH'].rolling(130).max()
    df['1_month_peak'] = df['HIGH'].rolling(22).max()

    df['3_month_bottom'] = df['LOW'].rolling(65).min()
    df['12_month_bottom'] = df['LOW'].rolling(250).min()
    df['24_month_bottom'] = df['LOW'].rolling(510).min()
    df['3_month_std'] = df['CLOSE'].rolling(65).std()

    df['bb_tag'] = None

    pandas_related.cal_intraperiod_return(df)

    # 分牛熊
    for index in df.index[1:]:
        preIndex = pandas_related.previous_index(df, index)
        # if index[0] == dt.datetime(2009, 4, 24):
        #     pass
        if df.at[index, 'monthly_return'] > 0.1 \
                and df.at[index, 'CLOSE'] / df.at[index, '3_month_bottom'] > 1.4 \
                and df.at[preIndex, 'bb_tag'] is None:
            df.at[index, 'bb_tag'] = '牛市'
            _base = df.at[index, '3_month_bottom']
            # _maxzhangfu = df.at[index, 'CLOSE'] / _base - 1
        elif df.at[preIndex, 'bb_tag'] == '牛市' \
                and (df.at[index, 'CLOSE'] - df.at[index, '6_month_peak']) / (df.at[index, '6_month_peak'] - _base) < -0.3 \
                and df.at[index, 'monthly_return'] < -0.1:
            df.at[index, 'bb_tag'] = '熊市'
            _peak = df.at[index, '6_month_peak']
        elif df.at[preIndex, 'bb_tag'] == '熊市' \
                and (df.at[index, '3_month_bottom'] - _peak) / (_peak - _base) < -0.6 \
                and df.at[index, 'CLOSE'] / df.at[index, '3_month_bottom'] > 1.3:
            df.at[index, 'bb_tag'] = '牛尾'
            _base = df.at[index, '3_month_bottom']
        elif df.at[preIndex, 'bb_tag'] == '牛尾' \
                and df.at[index, 'CLOSE'] / df.at[index, '1_month_peak'] - 1 < -0.15:
            pass
        elif df.at[preIndex, 'bb_tag'] == '牛市':
            df.at[index, 'bb_tag'] = '牛市'
        elif df.at[index, 'bb_tag'] == '熊市' and df.at[index, '3_month_std'] < 100:
            pass
        elif df.at[preIndex, 'bb_tag'] == '熊市' and df.at[index, '3_month_std'] > 100:
            df.at[index, 'bb_tag'] = '熊市'
        elif df.at[preIndex, 'bb_tag'] == '牛尾':
            df.at[index, 'bb_tag'] = '牛尾'

    df.drop(['6_month_peak', '1_month_peak', '3_month_bottom', '12_month_bottom', '24_month_bottom', '3_month_std'],
            axis=1, inplace=True)
    df.fillna('震荡市', inplace=True)

    pandas_related.gen_groupkey(df, groupkeyName='fenduan_flag', sourceCol='bb_tag')

    # 取所有中信一级指数
    dfIndustry = fetch.index_industry_list('all', type='citics', citicslevel=1, startTime='2005-01-01')
    dfIndustry['S_INFO_NAME'] = dfIndustry['S_INFO_NAME'].apply(lambda x: x[:-4])
    dfIndustry.reset_index(level=1,inplace=True,drop=True)
    dfIndustry.rename(columns={'S_INFO_NAME':'CODE'},inplace=True)
    dfIndustry.reset_index(inplace=True)
    dfIndustry.set_index(['DATETIME', 'CODE'], inplace=True)


# 第一个牛熊市的情况，行业的周涨跌幅
if __name__ == '__main__':
    from tools.data import fetch
    import datetime as dt
    import pandas as pd
    from tools.tinytools import pandas_related, group_method

    df = fetch.index_one('000001.SH', startTime='2000-01-01')
    # 取所有中信一级指数
    dfIndustry = fetch.index_industry_list('all', type='citics', citicslevel=1, startTime='2005-01-01')
    dfIndustry['S_INFO_NAME'] = dfIndustry['S_INFO_NAME'].apply(lambda x: x[:-4])
    dfIndustry.reset_index(level=1,inplace=True,drop=True)
    dfIndustry.rename(columns={'S_INFO_NAME':'CODE'},inplace=True)
    dfIndustry.reset_index(inplace=True)
    dfIndustry.set_index(['DATETIME', 'CODE'], inplace=True)

    # 第一个牛市期间
    dfBB1 = df['2006-01-04':'2010-02-12']

    dfInduBB1 = dfIndustry[dfBB1.index[0][0].replace(day=1): dfBB1.index[-1][0]]
    pandas_related.gen_date_flag(dfInduBB1, period='week')

    dfindex = fetch.index_list(['000001.SH', '000016.SH', '399001.SZ'],
                               startTime='2006-01-01', endTime='2010-02-12')
    pandas_related.gen_date_flag(dfindex, period='week')

    dfweeklyReturn = dfInduBB1.groupby(by=['year_week', 'CODE']).apply(group_method.cal_pct_change)
    dfweeklyReturn = dfweeklyReturn.unstack()

    dfindexWeeklyReturn = dfindex.groupby(by=['year_week', 'CODE']).apply(group_method.cal_pct_change)
    dfindexWeeklyReturn = dfindexWeeklyReturn.unstack()


    dfindexWeeklyReturn['000001.SH_close'] \
        = dfindex.loc[(slice(None), '000001.SH'), slice(None)].groupby(by=['year_week']).apply(group_method.cal_last)
    dfindexWeeklyReturn['DATETIME'] \
        = dfindex.loc[(slice(None), '000001.SH'), slice(None)].reset_index().groupby(by=['year_week']).apply(group_method.cal_last, col='DATETIME')

    dfweeklyReturnRank = dfweeklyReturn.rank(axis=1)

    rankCol = [f'第{i}' for i in range(1, len(dfweeklyReturnRank.columns) + 1)]
    chgRankCol = [f'第{i}涨幅%' for i in range(1, len(dfweeklyReturnRank.columns) + 1)]
    _rstCol = []
    for i, j in zip(rankCol, chgRankCol):
        _rstCol.append(i)
        _rstCol.append(j)
    dfRankRst = pd.DataFrame(index=dfweeklyReturnRank.index,
                             columns=['000001.SH', '000001.SH_close', 'DATETIME','000016.SH', '399001.SZ'] + _rstCol)
    dfRankRst[['000001.SH', '000001.SH_close', 'DATETIME','000016.SH', '399001.SZ']] \
        = dfindexWeeklyReturn[['000001.SH', '000001.SH_close', 'DATETIME','000016.SH', '399001.SZ']]
    dfRankRst[['000001.SH', '000016.SH', '399001.SZ']] \
        = dfRankRst[['000001.SH', '000016.SH', '399001.SZ']] * 100

    for weekIndex in dfweeklyReturnRank.index:
        # weekIndex = dfweeklyReturnRank.index[0]
        dfRankRst.loc[weekIndex,rankCol] = dfweeklyReturnRank.loc[weekIndex].sort_values(ascending=False).index.values
        dfRankRst.loc[weekIndex, chgRankCol] = \
            dfweeklyReturn.loc[
                  weekIndex, dfweeklyReturnRank.loc[weekIndex].sort_values(ascending=False).index.values
            ].values * 100
    del rankCol, chgRankCol, _rstCol

    # dfRankRst.to_csv('D:\\tempdata\\dfRankRst.csv', encoding='gbk')


# 第一个牛熊市的情况，行业的月涨跌幅
if __name__ == '__main__':
    from tools.data import fetch
    import pandas as pd
    from tools.tinytools import pandas_related, group_method

    df = fetch.index_one('000001.SH', startTime='2000-01-01')
    # 取所有中信一级指数
    dfIndustry = fetch.index_industry_list('all', type='citics', citicslevel=1, startTime='2005-01-01')
    dfIndustry['S_INFO_NAME'] = dfIndustry['S_INFO_NAME'].apply(lambda x: x[:-4])
    dfIndustry.reset_index(level=1,inplace=True,drop=True)
    dfIndustry.rename(columns={'S_INFO_NAME':'CODE'},inplace=True)
    dfIndustry.reset_index(inplace=True)
    dfIndustry.set_index(['DATETIME', 'CODE'], inplace=True)

    # 第一个牛市期间
    dfBB1 = df['2006-01-04':'2010-02-12']

    dfInduBB1 = dfIndustry[dfBB1.index[0][0].replace(day=1): dfBB1.index[-1][0]]
    pandas_related.gen_date_flag(dfInduBB1, period='month')

    dfindex = fetch.index_list(['000001.SH', '000016.SH', '399001.SZ'],
                               startTime='2006-01-01', endTime='2010-02-12')
    pandas_related.gen_date_flag(dfindex, period='month')

    dfmonthlyReturn = dfInduBB1.groupby(by=['year_month', 'CODE']).apply(group_method.cal_pct_change)
    dfmonthlyReturn = dfmonthlyReturn.unstack()

    dfindexMonthlyReturn = dfindex.groupby(by=['year_month', 'CODE']).apply(group_method.cal_pct_change)
    dfindexMonthlyReturn = dfindexMonthlyReturn.unstack()

    dfindexMonthlyReturn['000001.SH_close'] \
        = dfindex.loc[(slice(None), '000001.SH'), slice(None)].groupby(by=['year_month']).apply(group_method.cal_last)
    dfindexMonthlyReturn['DATETIME'] \
        = dfindex.loc[(slice(None), '000001.SH'), slice(None)].reset_index().groupby(by=['year_month']).apply(group_method.cal_last, col='DATETIME')

    dfmonthlyReturnRank = dfmonthlyReturn.rank(axis=1)

    rankCol = [f'第{i}' for i in range(1, len(dfmonthlyReturnRank.columns) + 1)]
    chgRankCol = [f'第{i}涨幅%' for i in range(1, len(dfmonthlyReturnRank.columns) + 1)]
    _rstCol = []
    for i, j in zip(rankCol, chgRankCol):
        _rstCol.append(i)
        _rstCol.append(j)
    dfRankRst = pd.DataFrame(index=dfmonthlyReturnRank.index,
                             columns=['000001.SH', '000001.SH_close', 'DATETIME','000016.SH', '399001.SZ'] + _rstCol)
    dfRankRst[['000001.SH', '000001.SH_close', 'DATETIME','000016.SH', '399001.SZ']] \
        = dfindexMonthlyReturn[['000001.SH', '000001.SH_close', 'DATETIME','000016.SH', '399001.SZ']]
    dfRankRst[['000001.SH', '000016.SH', '399001.SZ']] \
        = dfRankRst[['000001.SH', '000016.SH', '399001.SZ']] * 100

    for monthIndex in dfmonthlyReturnRank.index:
        # monthIndex = dfmonthlyReturnRank.index[0]
        dfRankRst.loc[monthIndex,rankCol] = dfmonthlyReturnRank.loc[monthIndex].sort_values(ascending=False).index.values
        dfRankRst.loc[monthIndex, chgRankCol] = \
            dfmonthlyReturn.loc[
                monthIndex, dfmonthlyReturnRank.loc[monthIndex].sort_values(ascending=False).index.values
            ].values * 100
    del rankCol, chgRankCol, _rstCol

    # dfRankRst.to_csv('D:\\tempdata\\dfRankRstMonth.csv', encoding='gbk')

# 第一个牛熊市的情况，行业的季度涨跌幅
if __name__ == '__main__':
    from tools.data import fetch
    import pandas as pd
    from tools.tinytools import pandas_related, group_method

    df = fetch.index_one('000001.SH', startTime='2000-01-01')
    # 取所有中信一级指数
    dfIndustry = fetch.index_industry_list('all', type='citics', citicslevel=1, startTime='2005-01-01')
    dfIndustry['S_INFO_NAME'] = dfIndustry['S_INFO_NAME'].apply(lambda x: x[:-4])
    dfIndustry.reset_index(level=1,inplace=True,drop=True)
    dfIndustry.rename(columns={'S_INFO_NAME':'CODE'},inplace=True)
    dfIndustry.reset_index(inplace=True)
    dfIndustry.set_index(['DATETIME', 'CODE'], inplace=True)

    # 第一个牛市期间
    dfBB1 = df['2006-01-04':'2010-02-12']

    dfInduBB1 = dfIndustry[dfBB1.index[0][0].replace(day=1): dfBB1.index[-1][0]]
    pandas_related.gen_date_flag(dfInduBB1, period='quarter')

    dfindex = fetch.index_list(['000001.SH', '000016.SH', '399001.SZ'],
                               startTime='2006-01-01', endTime='2010-02-12')
    pandas_related.gen_date_flag(dfindex, period='quarter')

    dfquarterlyReturn = dfInduBB1.groupby(by=['year_quarter', 'CODE']).apply(group_method.cal_pct_change)
    dfquarterlyReturn = dfquarterlyReturn.unstack()

    dfindexquarterlyReturn = dfindex.groupby(by=['year_quarter', 'CODE']).apply(group_method.cal_pct_change)
    dfindexquarterlyReturn = dfindexquarterlyReturn.unstack()

    dfindexquarterlyReturn['000001.SH_close'] \
        = dfindex.loc[(slice(None), '000001.SH'), slice(None)].groupby(by=['year_quarter']).apply(group_method.cal_last)
    dfindexquarterlyReturn['DATETIME'] \
        = dfindex.loc[(slice(None), '000001.SH'), slice(None)].reset_index().groupby(by=['year_quarter']).apply(group_method.cal_last, col='DATETIME')

    dfquarterlyReturnRank = dfquarterlyReturn.rank(axis=1)

    rankCol = [f'第{i}' for i in range(1, len(dfquarterlyReturnRank.columns) + 1)]
    chgRankCol = [f'第{i}涨幅%' for i in range(1, len(dfquarterlyReturnRank.columns) + 1)]
    _rstCol = []
    for i, j in zip(rankCol, chgRankCol):
        _rstCol.append(i)
        _rstCol.append(j)
    dfRankRst = pd.DataFrame(index=dfquarterlyReturnRank.index,
                             columns=['000001.SH', '000001.SH_close', 'DATETIME','000016.SH', '399001.SZ'] + _rstCol)
    dfRankRst[['000001.SH', '000001.SH_close', 'DATETIME','000016.SH', '399001.SZ']] \
        = dfindexquarterlyReturn[['000001.SH', '000001.SH_close', 'DATETIME','000016.SH', '399001.SZ']]
    dfRankRst[['000001.SH', '000016.SH', '399001.SZ']] \
        = dfRankRst[['000001.SH', '000016.SH', '399001.SZ']] * 100

    for quarterIndex in dfquarterlyReturnRank.index:
        # quarterIndex = dfquarterlyReturnRank.index[0]
        dfRankRst.loc[quarterIndex,rankCol] = dfquarterlyReturnRank.loc[quarterIndex].sort_values(ascending=False).index.values
        dfRankRst.loc[quarterIndex, chgRankCol] = \
            dfquarterlyReturn.loc[
                quarterIndex, dfquarterlyReturnRank.loc[quarterIndex].sort_values(ascending=False).index.values
            ].values * 100
    del rankCol, chgRankCol, _rstCol

    # dfRankRst.to_csv('D:\\tempdata\\dfRankRstquarter.csv', encoding='gbk')


dir(a)