

def merge_day2min(dfLongNeed, dfShort, colNameAfter,fillna=0):
    '''
    这个函数的目的是将长周期的某一列merge到短周期上，方便显示对照。两个df的index应该是一样，而且都必须含有DATETIME
    :param dfLongNeed: 长周期的需要merge的df，应该只有一列
    :param dfShort: 短周期的df
    :param colNameAfter: 归入短周期后的名字
    :return: 在短周期df上加了一列
    '''
    if 'DATETIME' in dfLongNeed.index.names \
            and dfLongNeed.index.names == dfShort.index.names:

        dfTemp = dfLongNeed.copy()
        dfTemp.reset_index(inplace=True)
        dfTemp['DATETIME'] = dfTemp['DATETIME'].apply(lambda x: x.replace(hour=15))

        dfShort[colNameAfter] = dfTemp.set_index(list(dfLongNeed.index.names))[dfLongNeed.columns[0]]
        if fillna == None:
            pass
        else:
            dfShort[colNameAfter].fillna(fillna, inplace=True)
    else:
        raise ValueError('两个df的index必须一样，且DATETIME必须在index里面！')


def day_downSample(dfInput, rule='W'):
    '''
    将df# 降采样生成周线等，df可以用DATETIME和CODE两列或者只有DATETIME一列，没有进行过多的检查。
    :param df: 需要降采样的df
    :param rule: 降采样的规则，"W"是周，'M'是月，目前只提供这两个选择。
    :return:
    '''
    if 'CODE' in dfInput.index.names and 'DATETIME' in dfInput.index.names:
        import numpy as np
        df = dfInput.copy()
        df.reset_index(inplace=True)

        if rule == 'W':
            df['chgSign'] = (
                    df['DATETIME'].apply(lambda x: x.weekday()) - df['DATETIME'].apply(lambda x: x.weekday()).shift(-1)). \
                apply(lambda x: True if x > 0.0 else np.nan)
        elif rule == 'M':
            df['chgSign'] = (
                    df['DATETIME'].apply(lambda x: x.month) - df['DATETIME'].apply(lambda x: x.month).shift(-1)). \
                apply(lambda x: True if x > 0.0 else np.nan)

        df.loc[df['chgSign'] == True, 'chgSign'] = range(len(df.loc[df['chgSign'] == True, 'chgSign']))
        df['chgSign'].fillna(method='bfill', inplace=True)
        df['chgSign'].fillna(df['chgSign'].max() + 1, inplace=True)

        downSampleDict = {'PRECLOSE': 'first', 'OPEN': 'first', 'HIGH': 'max', 'LOW': 'min', 'CLOSE': 'last',
                          'ADJPRECLOSE': 'first', 'ADJOPEN': 'first', 'ADJHIGH': 'max', 'ADJLOW': 'min',
                          'ADJCLOSE': 'last',
                          'BADJPRECLOSE': 'first', 'BADJOPEN': 'first', 'BADJHIGH': 'max', 'BADJLOW': 'min',
                          'BADJCLOSE': 'last',
                          'PCTCHANGE': 'sum', 'VOLUME': 'sum', 'AMOUNT': 'sum'}
        acturalDict = {}
        for col in df.columns:
            if col in downSampleDict:
                acturalDict[col] = downSampleDict[col]
            else:
                # 如果不在上面的dict中，那么统一取最后一个
                acturalDict[col] = 'last'

        dfWeek = df.groupby('chgSign').agg(acturalDict)
        dfWeek.set_index(['DATETIME', 'CODE'], inplace=True)
        return dfWeek
    elif 'DATETIME' in dfInput.index.names:
        import numpy as np
        df = dfInput.copy()
        df.reset_index(inplace=True)

        if rule == 'W':
            df['chgSign'] = (
                    df['DATETIME'].apply(lambda x: x.weekday()) - df['DATETIME'].apply(lambda x: x.weekday()).shift(
                -1)). \
                apply(lambda x: True if x > 0.0 else np.nan)
        elif rule == 'M':
            df['chgSign'] = (
                    df['DATETIME'].apply(lambda x: x.month) - df['DATETIME'].apply(lambda x: x.month).shift(-1)). \
                apply(lambda x: True if x > 0.0 else np.nan)

        df.loc[df['chgSign'] == True, 'chgSign'] = range(len(df.loc[df['chgSign'] == True, 'chgSign']))
        df['chgSign'].fillna(method='bfill', inplace=True)
        df['chgSign'].fillna(df['chgSign'].max() + 1, inplace=True)

        downSampleDict = {'PRECLOSE': 'first', 'OPEN': 'first', 'HIGH': 'max', 'LOW': 'min', 'CLOSE': 'last',
                          'ADJPRECLOSE': 'first', 'ADJOPEN': 'first', 'ADJHIGH': 'max', 'ADJLOW': 'min',
                          'ADJCLOSE': 'last',
                          'BADJPRECLOSE': 'first', 'BADJOPEN': 'first', 'BADJHIGH': 'max', 'BADJLOW': 'min',
                          'BADJCLOSE': 'last',
                          'PCTCHANGE': 'sum', 'VOLUME': 'sum', 'AMOUNT': 'sum'}
        acturalDict = {}
        for col in df.columns:
            if col in downSampleDict:
                acturalDict[col] = downSampleDict[col]
            else:
                # 如果不在上面的dict中，那么统一取最后一个
                acturalDict[col] = 'last'

        dfWeek = df.groupby('chgSign').agg(acturalDict)
        dfWeek.set_index('DATETIME', inplace=True)
        return dfWeek


def gen_wave_group_key(df, waveGroupKeyName='waveGroupKey', groupSign='sign'):
    '''
    这个函数的目的是根据groupSign分段，groupSign的标记是非零，生成一列waveGroupKeyName
    :param df:
    :param waveGroupKeyName:
    :param groupSign:
    :return:
    '''
    df[waveGroupKeyName] = None
    df.loc[df[groupSign] != 0, waveGroupKeyName] = range(1, len(df.loc[df[groupSign] != 0, waveGroupKeyName]) + 1)
    df[waveGroupKeyName].fillna(method='ffill', inplace=True)
    df[waveGroupKeyName].fillna(0, inplace=True)


def freq_stat(group, freqCol='总段数'):
    '''
    这个函数的目的是对列freqCol的值的出现进行统计，返回三列，出现次数，出现频率和累计频率
    :param group:
    :param freqCol:
    :return:
    '''
    import pandas as pd
    _sum = group[freqCol].count()
    rstdf = pd.DataFrame(group.groupby(freqCol)[freqCol].count())
    rstdf.rename(columns={freqCol:'出现次数'},inplace=True)
    rstdf['出现频率'] = group.groupby(freqCol)[freqCol].count() / _sum
    rstdf['累计频率'] = rstdf['出现频率'].cumsum()
    return rstdf


def cal_intraperiod_return(dfDay, period='month'):
    '''
    这个函数用的作用是：例如，生成每个月月内每天相对于上月末的收益率
    :param dfDay:
    :param period:
    :return:
    '''
    if period == 'month':
        # dfDay = df.copy()
        dfDay.reset_index(inplace=True)
        dfDay['year_month'] = dfDay['DATETIME'].apply(lambda x: x.replace(day=1))
        dfDay.set_index(['DATETIME', 'CODE'], inplace=True)
        def _find_monthly_return(group1):
            return group1['CLOSE'] / group1.iat[0, group1.columns.get_loc('PRECLOSE')] - 1

        dfDay[f'{period}ly_return'] \
            = dfDay.groupby('year_month').apply(_find_monthly_return).reset_index(level=0, drop=True)
        dfDay.drop('year_month', axis=1, inplace=True)
        # return dfDay


def gen_date_flag(dfDay, period='month'):
    if period == 'month':
        # dfDay = df.copy()
        dfDay.reset_index(inplace=True)
        dfDay['year_month'] = dfDay['DATETIME'].apply(lambda x: "{year}-m{month:0>2}".format(year=x.year, month=x.month))
        dfDay.set_index(['DATETIME', 'CODE'], inplace=True)
    elif period == 'week':
        dfDay.reset_index(inplace=True)
        dfDay['year_week'] = dfDay['DATETIME'].apply(lambda x:"{year}-w{week:0>2}".format(year=x.year, week=x.week)
                                                              if (x.week != 1) or x.dayofyear < 300
                                                              else "{year}-w{week:0>2}".format(year=x.year, week=x.week+52) )
        dfDay.set_index(['DATETIME', 'CODE'], inplace=True)
    elif period == 'quarter':
        dfDay.reset_index(inplace=True)
        dfDay['year_quarter'] = dfDay['DATETIME'].apply(lambda x: "{year}-q{quarter}".format(year=x.year, quarter=x.quarter))
        dfDay.set_index(['DATETIME', 'CODE'], inplace=True)


def previous_index(df, curIndex):
    return df.index[df.index.get_loc(curIndex) - 1]


def gen_groupkey(df, groupkeyName='groupKey', sourceCol='tag', type='string_flag'):
    if type == 'string_flag':
        df[groupkeyName] = None
        i = 1
        for index in df.index[1:]:
            preIndex = previous_index(df, index)
            if df.at[index, sourceCol] == df.at[preIndex, sourceCol]:
                pass
            else:
                df.at[index, groupkeyName] = i
                i += 1

        df[groupkeyName].fillna(method='ffill',inplace=True)
        df[groupkeyName].fillna(0, inplace=True)
    elif type == 'zero_updown':
        df['_temp'] = (df[sourceCol] * df[sourceCol].shift(-1)) < 0
        df[groupkeyName] = None
        df.loc[df['_temp'], groupkeyName] = range(len(df[groupkeyName][df['_temp']]))
        df[groupkeyName].fillna(method='bfill', inplace=True)
        df[groupkeyName].fillna(df[groupkeyName].max() + 1, inplace=True)
        df.drop('_temp', axis=1, inplace=True)


