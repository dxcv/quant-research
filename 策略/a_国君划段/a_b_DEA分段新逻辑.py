
import pandas as pd
import numpy as np
from tools.data import fetch
from 策略.a_国君划段 import a_a_DEA分段


def gen_DEA_segment(df):
    '''
    对df根据DEA的水上水下进行分组，
    :param df:需要有DEA这一列
    :return: 生成groupKey列，不同DEA不同分组，会将最后不完整的DEA段删掉，updown显示水上水下，水上是True，水下是False
    '''
    dfNeed = df.copy()
    dfNeed['chgPoint'] = (dfNeed['DEA'] * dfNeed['DEA'].shift(-1)) < 0
    dfNeed['groupKey'] = np.nan
    dfNeed.loc[dfNeed['chgPoint'], 'groupKey'] = range(len(dfNeed['groupKey'][dfNeed['chgPoint']]))
    dfNeed['groupKey'].fillna(method='bfill', inplace=True)
    dfNeed.dropna(inplace=True)
    dfNeed['updown'] = dfNeed['DEA'].apply(lambda x:True if x >= 0 else False) # DEA水上是正，DEA水下是负
    dfNeed.drop('chgPoint', axis=1, inplace=True)
    return dfNeed


def fenduan_new_logic(df1day):
    '''
    这个函数的功能是在df1day中得到新逻辑的分段标记和买卖标记，
    :param df1day: 必须有groupKey，updown两列
    :return: 多了两列:'确认点'和'波段拐点'
    '''
    df1day['确认点'] = 0
    df1day['波段拐点'] = 0

    i = 0.0
    preGroup = df1day[df1day['groupKey'] == i]
    if preGroup.iat[0, preGroup.columns.get_loc('updown')] == True:
        df1day.at[preGroup['CLOSE'].idxmax(),'波段拐点'] = -1.0
        preGroup.at[preGroup['CLOSE'].idxmax(), '波段拐点'] = -1.0
    else:
        df1day.at[preGroup['CLOSE'].idxmin(), '波段拐点'] = 1.0
        preGroup.at[preGroup['CLOSE'].idxmin(), '波段拐点'] = 1.0

    i += 1
    while i <= df1day['groupKey'].max():
        curGroup = df1day[df1day['groupKey'] == i]

        # 找确认点
        if preGroup.iat[0, preGroup.columns.get_loc('updown')] == True:
            for index in curGroup.index:
                preIndex = df1day.index[df1day.index.get_loc(index) - 1]
                # print(index, curGroup.at[index, 'CLOSE'], df1day.loc[preGroup['CLOSE'].idxmax():preIndex, 'CLOSE'].min())
                if curGroup.at[index, 'CLOSE'] < df1day.loc[preGroup['CLOSE'].idxmax():preIndex,'CLOSE'].min():
                    print('卖出',index)
                    df1day.at[index, '确认点'] = -1
                    curGroup.at[index, '确认点'] = -1
                    i += 1
                    break
        else:
            for index in curGroup.index:
                preIndex = df1day.index[df1day.index.get_loc(index) - 1]
                # print(index, curGroup.at[index, 'CLOSE'], df1day.loc[preGroup['CLOSE'].idxmax():preIndex, 'CLOSE'].max())
                if curGroup.at[index, 'CLOSE'] > df1day.loc[preGroup['CLOSE'].idxmin():preIndex,'CLOSE'].max():
                    print('买入',index)
                    df1day.at[index, '确认点'] = 1
                    curGroup.at[index, '确认点'] = 1
                    i += 1
                    break

        # 如果找到确认点了
        if not curGroup[curGroup['确认点'] != 0].empty:
            # 当前group变成之前group
            preGroup = curGroup.copy()
            if preGroup.iat[0, preGroup.columns.get_loc('updown')] == True:
                df1day.at[preGroup['CLOSE'].idxmax(), '波段拐点'] = -1.0
                preGroup.at[preGroup['CLOSE'].idxmax(), '波段拐点'] = -1.0
            else:
                df1day.at[preGroup['CLOSE'].idxmin(), '波段拐点'] = 1.0
                preGroup.at[preGroup['CLOSE'].idxmin(), '波段拐点'] = 1.0
        else:
            # 先将之前找到的波段拐点弄掉
            df1day.at[preGroup.loc[preGroup['波段拐点'] != 0, '波段拐点'].index[0],'波段拐点'] = 0
            preGroup.loc[preGroup['波段拐点'] != 0, '波段拐点'] = 0
            # 这里需要得到pregroup
            preGroup = pd.concat([preGroup, df1day[df1day['groupKey'] == i + 1]])
            # 然后找到新的波段拐点
            if preGroup.iat[0, preGroup.columns.get_loc('updown')] == True:
                df1day.at[preGroup['CLOSE'].idxmax(), '波段拐点'] = -1.0
                preGroup.at[preGroup['CLOSE'].idxmax(), '波段拐点'] = -1.0
            else:
                df1day.at[preGroup['CLOSE'].idxmin(), '波段拐点'] = 1.0
                preGroup.at[preGroup['CLOSE'].idxmin(), '波段拐点'] = 1.0
            i += 2


def long_attach_2_short(dfSmallPeriod, largeSignCol, smallSignCol, largeSign_after, mismatchThres):
        dfSmallPeriod['order'] = range(len(dfSmallPeriod.index))
        dfSmallPeriod[largeSign_after] = 0
        for largeIndex in dfSmallPeriod[dfSmallPeriod[largeSignCol] != 0].index:
            dfSmallPeriod['distance'] = 0
            if dfSmallPeriod.at[largeIndex, largeSignCol] == 1:
                dfSmallPeriod['distance'] = (
                        dfSmallPeriod.loc[dfSmallPeriod[smallSignCol] == 1, 'order'] - dfSmallPeriod.at[largeIndex, 'order']).abs()
                if dfSmallPeriod['distance'].min() < mismatchThres:
                    dfSmallPeriod.at[dfSmallPeriod['distance'].idxmin(), largeSign_after] = 1
                else:
                    closestPoint = dfSmallPeriod['distance'].idxmin()

                    # 找到最近的两个点
                    # 最近的小级别点在右边
                    if dfSmallPeriod.at[closestPoint, 'order'] - dfSmallPeriod.at[largeIndex, 'order'] > 0:
                        secondClosestPoint = dfSmallPeriod.loc[:largeIndex,'distance'].idxmin()
                    # 最近的小级别点在右边
                    else:
                        secondClosestPoint = dfSmallPeriod.loc[largeIndex:, 'distance'].idxmin()

                    # 如果比较靠边上，那个就不归到最低点那里去
                    if dfSmallPeriod.at[closestPoint, 'distance'] \
                            / (dfSmallPeriod.at[closestPoint, 'distance']
                               + dfSmallPeriod.at[secondClosestPoint, 'distance']
                              ) < 0.3:
                        dfSmallPeriod.at[closestPoint, largeSign_after] = 1
                    # 如果比较靠中间，那么就归到最低点那里去
                    else:
                        if dfSmallPeriod.at[closestPoint, 'CLOSE'] > dfSmallPeriod.at[secondClosestPoint, 'CLOSE']:
                            dfSmallPeriod.at[secondClosestPoint, largeSign_after] = 1
                        else:
                            dfSmallPeriod.at[closestPoint, largeSign_after] = 1
            elif dfSmallPeriod.at[largeIndex, largeSignCol] == -1:
                dfSmallPeriod['distance'] = (
                        dfSmallPeriod.loc[dfSmallPeriod[smallSignCol] == -1, 'order'] - dfSmallPeriod.at[largeIndex, 'order']).abs()
                if dfSmallPeriod['distance'].min() < mismatchThres:
                    dfSmallPeriod.at[dfSmallPeriod['distance'].idxmin(), largeSign_after] = -1
                else:
                    closestPoint = dfSmallPeriod['distance'].idxmin()

                    # 找到最近的两个点
                    # 最近的小级别点在右边
                    if dfSmallPeriod.at[closestPoint, 'order'] - dfSmallPeriod.at[largeIndex, 'order'] > 0:
                        secondClosestPoint = dfSmallPeriod.loc[:largeIndex, 'distance'].idxmin()
                    # 最近的小级别点在右边
                    else:
                        secondClosestPoint = dfSmallPeriod.loc[largeIndex:, 'distance'].idxmin()

                    # 如果比较靠边上，那个就不归到最高点那里去
                    if dfSmallPeriod.at[closestPoint, 'distance'] \
                            / (dfSmallPeriod.at[closestPoint, 'distance']
                               + dfSmallPeriod.at[secondClosestPoint, 'distance']
                    ) < 0.3:
                        dfSmallPeriod.at[closestPoint, largeSign_after] = -1
                        # 如果比较靠中间，那么就归到最高点那里去
                    else:
                        if dfSmallPeriod.at[closestPoint, 'CLOSE'] > dfSmallPeriod.at[secondClosestPoint, 'CLOSE']:
                            dfSmallPeriod.at[closestPoint, largeSign_after] = -1
                        else:
                            dfSmallPeriod.at[secondClosestPoint, largeSign_after] = -1
        dfSmallPeriod.drop(['order', 'distance'], axis=1, inplace=True)


# 下面是对波段的简单分析统计
def _find_minMax(group):
    '''
    找到段内的最大值，最小值，以及其中的位置。
    :param group:
    :return:
    '''
    return pd.DataFrame([group.index[0][0],
                         group.iat[0, group.columns.get_loc('波段拐点')], group.iat[0, group.columns.get_loc('CLOSE')],
                         group['CLOSE'].max(), group['CLOSE'].idxmax()[0],
                         group['CLOSE'].min(), group['CLOSE'].idxmin()[0]],
                        index=['point',
                               '波段拐点', 'CLOSE',
                               'maxValue', 'maxPoint',
                               'minValue', 'minPoint']).T


def _gen_desc(ser1):
    a = pd.DataFrame(index=['均值','标准差','最大值','最小值','样本数'],columns=['desc'])
    a.at['均值','desc'] = ser1.mean()
    a.at['标准差', 'desc'] = ser1.std()
    a.at['最大值', 'desc'] = ser1.max()
    a.at['最小值', 'desc'] = ser1.min()
    a.at['样本数', 'desc'] = ser1.count()
    return a.T


def gen_stat_rst(df):
    dfStat = df.groupby('waveGroupKey').apply(_find_minMax)
    dfStat.reset_index(level=1, drop=True, inplace=True)
    dfStat.drop(['maxValue', 'maxPoint', 'minValue', 'minPoint'], axis=1, inplace=True)
    dfStat['收益率'] = dfStat['CLOSE'].pct_change()
    dfStat['收益率'] = dfStat['收益率'] * dfStat['波段拐点'] * -1
    dfStat['波段拐点'] = dfStat['波段拐点'].map({-1: '上涨波段', 1: '下降波段'})
    dfStat.dropna(inplace=True)
    rst = dfStat.groupby('波段拐点')['收益率'].apply(_gen_desc)
    rst.reset_index(level=1,drop=True,inplace=True)
    return rst


def sub_wave_analysis(group):
    print(group.iat[0,group.columns.get_loc('waveGroupKey')], group.index[0])
    rst = pd.DataFrame(columns=['涨跌','总段数','合并后总段数',
                                '上涨段数','最大涨幅','最小涨幅','平均涨幅','涨幅标准差',
                                '合并后上涨段数','合并后最大涨幅','合并后最小涨幅','合并后平均涨幅','合并后涨幅标准差',
                                '下跌段数','最大跌幅','最小跌幅','平均跌幅','跌幅标准差',
                                '合并后下跌段数','合并后最大跌幅','合并后最小跌幅','合并后平均跌幅','合并后跌幅标准差',
                                '日线确认后延续幅度','日线确认后延续段数','日线确认后延续时长',
                                '日线确认前延续幅度','日线确认前延续段数','日线确认前延续时长',],index=[0,])
    rst.at[0,'涨跌'] = '上涨' if group.iat[0, group.columns.get_loc('波段拐点_大_合并')] == 1 else '下跌'

    # 将字波段的端点拿出来，并且由于group是含头不含尾，也在将最后一点当成尾，头去掉
    subWave = group.loc[group['波段拐点'] != 0,]
    subWave.set_value(index=group.index[-1],col=group.columns,value=group.iloc[-1])
    if subWave.iat[0,subWave.columns.get_loc('波段拐点')] == 1:
        subWave.iat[-1,subWave.columns.get_loc('波段拐点')] = -1
        subWave.iat[0, subWave.columns.get_loc('波段拐点')] = 0
    else:
        subWave.iat[-1,subWave.columns.get_loc('波段拐点')] = 1
        subWave.iat[0, subWave.columns.get_loc('波段拐点')] = 0
    subWave['涨跌幅度'] = subWave['CLOSE'].pct_change()

    rst.at[0, '上涨段数'] = len(subWave[subWave['波段拐点'] == -1])
    rst.at[0, '最大涨幅'] = subWave.loc[subWave['涨跌幅度'] > 0, '涨跌幅度'].max()
    rst.at[0, '最小涨幅'] = subWave.loc[subWave['涨跌幅度'] > 0, '涨跌幅度'].min()
    rst.at[0, '平均涨幅'] = subWave.loc[subWave['涨跌幅度'] > 0, '涨跌幅度'].mean()
    rst.at[0, '涨幅标准差'] = subWave.loc[subWave['涨跌幅度'] > 0, '涨跌幅度'].std()

    rst.at[0, '下跌段数'] = len(subWave[subWave['波段拐点'] == 1])
    rst.at[0, '最大跌幅'] = subWave.loc[subWave['涨跌幅度'] < 0, '涨跌幅度'].min()
    rst.at[0, '最小跌幅'] = subWave.loc[subWave['涨跌幅度'] < 0, '涨跌幅度'].max()
    rst.at[0, '平均跌幅'] = subWave.loc[subWave['涨跌幅度'] < 0, '涨跌幅度'].mean()
    rst.at[0, '跌幅标准差'] = subWave.loc[subWave['涨跌幅度'] < 0, '涨跌幅度'].std()

    rst.at[0, '总段数'] = rst.at[0, '上涨段数'] + rst.at[0, '下跌段数']

    # 计算与大周期确认点相关的指标
    if rst.at[0, '涨跌'] == '上涨':
        # 大周期内没有确认点，一般由于跌得快而且起来的也快，确认点在靠右的地方，被纳入到子周期那里了
        if group.loc[group['确认点_大'] == 1, 'CLOSE'].empty:
            rst.at[0, '日线确认后延续幅度'] = None
            rst.at[0, '日线确认后延续时长'] = None
            rst.at[0, '日线确认前延续幅度'] \
                = group.iat[-1, group.columns.get_loc('CLOSE')] / group.iat[0, group.columns.get_loc('CLOSE')] -1
            rst.at[0, '日线确认前延续时长'] = len(group)
            rst.at[0, '日线确认后延续段数'] = None
            rst.at[0, '日线确认前延续段数'] = rst.at[0,'上涨段数']
        else:
            rst.at[0, '日线确认后延续幅度'] \
                = group.iat[-1, group.columns.get_loc('CLOSE')] / group.loc[group['确认点_大'] == 1, 'CLOSE'][0] - 1
            rst.at[0, '日线确认后延续时长'] \
                = len(group.index) - len(group[:group.loc[group['确认点_大'] == 1, 'CLOSE'].index[-1]].index)

            rst.at[0, '日线确认前延续幅度'] \
                = group.loc[group['确认点_大'] == 1, 'CLOSE'][0] / group.iat[0, group.columns.get_loc('CLOSE')] - 1
            rst.at[0, '日线确认前延续时长'] \
                = len(group[:group.loc[group['确认点_大'] == 1, 'CLOSE'].index[-1]].index)-1

            # 大级别确认点右边没有日期小级别的拐点了
            try:
                rightIndex = group[group.loc[group['确认点_大'] == 1, 'CLOSE'].index[-1]:][
                    group.loc[group.loc[group['确认点_大'] == 1, 'CLOSE'].index[-1]:,'波段拐点'] !=0
                                                                                          ].index[0]
            except IndexError:
                rst.at[0, '日线确认后延续段数'] = 0
                rst.at[0, '日线确认前延续段数'] = rst.at[0,'上涨段数']
            else:
                rst.at[0, '日线确认后延续段数'] = len(subWave[rightIndex:][subWave.loc[rightIndex:,'波段拐点'] == -1])
                rst.at[0, '日线确认前延续段数'] = len(subWave[:rightIndex][subWave.loc[:rightIndex,'波段拐点'] == -1])
    else:
        if group.loc[group['确认点_大'] == -1, 'CLOSE'].empty:
            rst.at[0, '日线确认后延续幅度'] = None
            rst.at[0, '日线确认后延续时长'] = None
            rst.at[0, '日线确认前延续幅度'] \
                = group.iat[-1, group.columns.get_loc('CLOSE')] / group.iat[0, group.columns.get_loc('CLOSE')] -1
            rst.at[0, '日线确认前延续时长'] = len(group)
            rst.at[0, '日线确认后延续段数'] = None
            rst.at[0, '日线确认前延续段数'] = rst.at[0,'下跌段数']
        else:
            rst.at[0, '日线确认后延续幅度'] \
                = group.iat[-1, group.columns.get_loc('CLOSE')] / group.loc[group['确认点_大'] == -1, 'CLOSE'][0] - 1
            rst.at[0, '日线确认后延续时长'] \
                = len(group.index) - len(group[:group.loc[group['确认点_大'] == -1, 'CLOSE'].index[-1]].index)

            rst.at[0, '日线确认前延续幅度'] \
                = group.loc[group['确认点_大'] == -1, 'CLOSE'][0] / group.iat[0, group.columns.get_loc('CLOSE')] - 1
            rst.at[0, '日线确认前延续时长'] \
                = len(group[:group.loc[group['确认点_大'] == -1, 'CLOSE'].index[-1]].index) - 1

            # 大级别确认点右边没有日期小级别的拐点了
            try:
                rightIndex = group[group.loc[group['确认点_大'] == -1, 'CLOSE'].index[-1]:][
                    group.loc[group.loc[group['确认点_大'] == -1, 'CLOSE'].index[-1]:, '波段拐点'] != 0
                    ].index[0]
            except IndexError:
                rst.at[0, '日线确认后延续段数'] = 0
                rst.at[0, '日线确认前延续段数'] = rst.at[0, '下跌段数']
            else:
                rst.at[0, '日线确认后延续段数'] = len(subWave[rightIndex:][subWave.loc[rightIndex:, '波段拐点'] == 1])
                rst.at[0, '日线确认前延续段数'] = len(subWave[:rightIndex][subWave.loc[:rightIndex, '波段拐点'] == 1])

    if rst.at[0, '涨跌'] == '上涨':
        maxPoint = 0
        for curIndex in subWave[subWave['波段拐点'] < 0].index[:-1]:
            # curIndex = subWave[subWave['波段拐点'] < 0].index[0]
            if subWave.at[curIndex, 'CLOSE'] > maxPoint:
                maxPoint = subWave.at[curIndex, 'CLOSE']
            else:
                previousIndex = subWave.index[subWave.index.get_loc(curIndex) - 1]
                subWave.at[curIndex, '波段拐点'] = 0
                subWave.at[previousIndex, '波段拐点'] = 0
    else:
        minPoint = 49999
        for curIndex in subWave[subWave['波段拐点'] > 0].index:
            # curIndex = subWave[subWave['波段拐点'] < 0].index[0]
            if subWave.at[curIndex, 'CLOSE'] < minPoint:
                minPoint = subWave.at[curIndex, 'CLOSE']
            else:
                previousIndex = subWave.index[subWave.index.get_loc(curIndex) - 1]
                subWave.at[curIndex, '波段拐点'] = 0
                subWave.at[previousIndex, '波段拐点'] = 0

    rst.at[0, '合并后上涨段数'] = len(subWave[subWave['波段拐点'] == -1])
    rst.at[0, '合并后下跌段数'] = len(subWave[subWave['波段拐点'] == 1])
    rst.at[0, '合并后总段数'] = rst.at[0, '合并后上涨段数'] + rst.at[0, '合并后下跌段数']

    if subWave.iat[-1,subWave.columns.get_loc('波段拐点')] == 1:
        subWave.iat[0, subWave.columns.get_loc('波段拐点')] = -1
    else:
        subWave.iat[0, subWave.columns.get_loc('波段拐点')] = 1

    subWave = subWave[subWave['波段拐点'] != 0]
    subWave['涨跌幅度'] = subWave['CLOSE'].pct_change()

    rst.at[0, '合并后最大涨幅'] = subWave.loc[subWave['涨跌幅度'] > 0, '涨跌幅度'].max()
    rst.at[0, '合并后最小涨幅'] = subWave.loc[subWave['涨跌幅度'] > 0, '涨跌幅度'].min()
    rst.at[0, '合并后平均涨幅'] = subWave.loc[subWave['涨跌幅度'] > 0, '涨跌幅度'].mean()
    rst.at[0, '合并后涨幅标准差'] = subWave.loc[subWave['涨跌幅度'] > 0, '涨跌幅度'].std()

    rst.at[0, '合并后最大跌幅'] = subWave.loc[subWave['涨跌幅度'] < 0, '涨跌幅度'].min()
    rst.at[0, '合并后最小跌幅'] = subWave.loc[subWave['涨跌幅度'] < 0, '涨跌幅度'].max()
    rst.at[0, '合并后平均跌幅'] = subWave.loc[subWave['涨跌幅度'] < 0, '涨跌幅度'].mean()
    rst.at[0, '合并后跌幅标准差'] = subWave.loc[subWave['涨跌幅度'] < 0, '涨跌幅度'].std()

    return rst


def after_confirm_stat(group):
    a = pd.DataFrame(group.count())
    a.reset_index(inplace=True)
    a.rename(columns={'index':'项目',0:'值'},inplace=True)
    a['统计量'] = '有效数量'
    a.set_index(['项目','统计量'],inplace=True)

    b = pd.DataFrame(group.mean())
    b.reset_index(inplace=True)
    b.rename(columns={'index':'项目',0:'值'},inplace=True)
    b['统计量'] = '均值'
    b.set_index(['项目','统计量'],inplace=True)

    c = pd.DataFrame(group.std())
    c.reset_index(inplace=True)
    c.rename(columns={'index':'项目',0:'值'},inplace=True)
    c['统计量'] = '标准差'
    c.set_index(['项目','统计量'],inplace=True)

    d = pd.DataFrame(group.max())
    d.reset_index(inplace=True)
    d.rename(columns={'index':'项目',0:'值'},inplace=True)
    d['统计量'] = '最大值'
    d.set_index(['项目','统计量'],inplace=True)

    e = pd.DataFrame(group.min())
    e.reset_index(inplace=True)
    e.rename(columns={'index':'项目',0:'值'},inplace=True)
    e['统计量'] = '最小值'
    e.set_index(['项目','统计量'],inplace=True)

    f = a.copy()
    f['值'] = group[group.columns[0]].size - a['值']
    f.reset_index(inplace=True)
    f['统计量'] = '无效个数'
    f.set_index(['项目', '统计量'], inplace=True)

    rst = pd.concat([a,f,b,c,d,e],axis=0)
    rst.sort_index(level=0,inplace=True)
    return rst.T.stack(0)[['有效数量','无效个数','均值','标准差','最大值','最小值']]


# ######################################################################################################################
# 显示000001.SH的分段情况，日线
if __name__ == '__main__':
    from tools.mplot import sub, fig
    from tools.backtest import signal, timing
    df1day = fetch.index_one('000001.SH')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)

    fenduan_new_logic(df1dayAfter)

    # 回测新逻辑及显示equityCurve
    signal.signal2position_single(df1dayAfter, signalCol='确认点', pxCol='CLOSE',
                                  mode='long')
    btest = timing.Single_Proportion(df1dayAfter, originSignal='确认点',mode='long',)

    # 显示画段的结果
    szFig = fig.Fig()
    close1day = sub.Lines(szFig.ax1, szFig.ax1Top,
                          df1dayAfter[['CLOSE']],
                          lenuplt=1000, step=50,
                          majorGrid='year', majorFormat='%Y',
                          minorGrid='month')
    tech1day = sub.Lines(szFig.ax2, szFig.ax2Top,df1dayAfter[['DEA']],baseLine=0,
                         lenuplt=1000, step=50,
                         majorGrid='year', majorFormat='%Y',
                         minorGrid='month')
    close1day.attach_wave_sign(df1dayAfter[['CLOSE','波段拐点']], align='left')
    close1day.attach_buysell_sign(df1dayAfter[['CLOSE','poChg']], align='left', mode='long')
    tech1day.attach_buysell_sign(df1dayAfter[['DEA', 'poChg']], align='left', mode='long')
    close1day.add_synchron(tech1day)
    tech1day.add_synchron(close1day)

    szFig.show()

    del df1day, df1dayAfter, btest, szFig, close1day, tech1day


# 显示000001.SH的分段情况，30min
if __name__ == '__main__':
    from tools.mplot import sub, fig
    from tools.backtest import signal, timing
    df30min = fetch.n_min('000001.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)

    fenduan_new_logic(df30minAfter)

    # 回测新逻辑及显示equityCurve
    signal.signal2position_single(df30minAfter, signalCol='确认点', pxCol='CLOSE',
                                  mode='long')
    btest = timing.Single_Proportion(df30minAfter, originSignal='确认点',mode='long',period='30min',
                                     contrast='000001.SH',labels=('new logic 30min',))


    # 显示画段的结果
    szFig = fig.Fig()
    close30min = sub.Lines(szFig.ax1, szFig.ax1Top,df30minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500, step=50,
                           majorGrid='week', majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech30min = sub.Lines(szFig.ax2, szFig.ax2Top,df30minAfter[['DEA']],baseLine=0,
                          length=150,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    close30min.attach_wave_sign(df30minAfter[['CLOSE','波段拐点']],align='left')
    close30min.attach_buysell_sign(df30minAfter[['CLOSE','poChg']],align='left',mode='long')
    tech30min.attach_buysell_sign(df30minAfter[['DEA', 'poChg']], align='left', mode='long')
    close30min.add_synchron(tech30min)
    tech30min.add_synchron(close30min)

    szFig.show()

    del df30min, df30minAfter, btest, szFig, close30min, tech30min


# 显示000001.SH的分段情况，5min
if __name__ == '__main__':
    from tools.mplot import sub, fig
    from tools.backtest import signal, timing
    df5min = fetch.n_min('000001.SH',n=5)
    a_a_DEA分段.cal_macd(df5min)
    df5min = df5min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_DEA_segment(df5min)

    fenduan_new_logic(df5minAfter)

    # 回测新逻辑及显示equityCurve
    signal.signal2position_single(df5minAfter, signalCol='确认点', pxCol='CLOSE',
                                  mode='long',)
    btest = timing.Single_Proportion(df5minAfter, originSignal='确认点',mode='long',period='5min',
                                     contrast='000001.SH',labels=('new logic 5min',))

    # 显示画段的结果
    szFig = fig.Fig()
    close5min = sub.Lines(szFig.ax1, szFig.ax1Top,df5minAfter[['CLOSE']],
                          length=150,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    tech5min = sub.Lines(szFig.ax2, szFig.ax2Top,df5minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close5min.attach_wave_sign(df5minAfter[['CLOSE','波段拐点']],align='left')
    close5min.attach_buysell_sign(df5minAfter[['CLOSE','poChg']], align='left', mode='long')
    tech5min.attach_buysell_sign(df5minAfter[['DEA', 'poChg']], align='left', mode='long')
    close5min.add_synchron(tech5min)
    tech5min.add_synchron(close5min)

    szFig.show()

    del df5min, df5minAfter, btest, szFig, close5min, tech5min


# 显示399006.SZ的分段情况，日线
if __name__ == '__main__':
    from tools.mplot import sub, fig
    from tools.backtest import signal, timing
    df1day = fetch.index_one('399006.SZ')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)

    fenduan_new_logic(df1dayAfter)

    # 回测新逻辑及显示equityCurve
    signal.signal2position_single(df1dayAfter, signalCol='确认点', pxCol='CLOSE',
                                  mode='long', )
    btest = timing.Single_Proportion(df1dayAfter, originSignal='确认点', mode='long',
                                     contrast='000001.SH',)


    # 显示画段的结果
    szFig = fig.Fig()
    close1day = sub.Lines(szFig.ax1, szFig.ax1Top,df1dayAfter[['CLOSE']],
                          lenuplt=1000, step=50,
                          majorGrid='year', majorFormat='%Y',
                          minorGrid='month')
    tech1day = sub.Lines(szFig.ax2, szFig.ax2Top,df1dayAfter[['DEA']],baseLine=0,
                         lenuplt=1000, step=50,
                         majorGrid='year', majorFormat='%Y',
                         minorGrid='month')
    close1day.attach_wave_sign(df1dayAfter[['CLOSE','波段拐点']], align='left')
    close1day.attach_buysell_sign(df1dayAfter[['CLOSE','poChg']], align='left', mode='long')
    tech1day.attach_buysell_sign(df1dayAfter[['DEA', 'poChg']], align='left', mode='long')
    close1day.add_synchron(tech1day)
    tech1day.add_synchron(close1day)

    szFig.show()

    del df1day, df1dayAfter, btest, szFig, close1day, tech1day


# 显示399006.SZ的分段情况，30min
if __name__ == '__main__':
    from tools.mplot import sub, fig
    from tools.backtest import signal, timing
    df30min = fetch.n_min('399006.SZ',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)

    fenduan_new_logic(df30minAfter)

    # 回测新逻辑及显示equityCurve
    signal.signal2position_single(df30minAfter, signalCol='确认点', pxCol='CLOSE',
                                  mode='long', )
    btest = timing.Single_Proportion(df30minAfter, originSignal='确认点',mode='long',period='30min',
                                     contrast='000001.SH',labels=('new logic 30min',))

    # 显示画段的结果
    szFig = fig.Fig()
    close30min = sub.Lines(szFig.ax1, szFig.ax1Top,df30minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500, step=50,
                           majorGrid='week', majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech30min = sub.Lines(szFig.ax2, szFig.ax2Top,df30minAfter[['DEA']],baseLine=0,
                          length=150,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    close30min.attach_wave_sign(df30minAfter[['CLOSE','波段拐点']],align='left')
    close30min.attach_buysell_sign(df30minAfter[['CLOSE','poChg']],align='left',mode='long')
    tech30min.attach_buysell_sign(df30minAfter[['DEA', 'poChg']], align='left', mode='long')
    close30min.add_synchron(tech30min)
    tech30min.add_synchron(close30min)

    szFig.show()

    del df30min, df30minAfter, btest, szFig, close30min, tech30min


# 显示399006.SZ的分段情况，5min
if __name__ == '__main__':
    from tools.mplot import sub, fig
    from tools.backtest import signal, timing
    df5min = fetch.n_min('399006.SZ',n=5)
    a_a_DEA分段.cal_macd(df5min)
    df5min = df5min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_DEA_segment(df5min)

    fenduan_new_logic(df5minAfter)

    # 回测新逻辑及显示equityCurve
    signal.signal2position_single(df5minAfter, signalCol='确认点', pxCol='CLOSE',
                                  mode='long', )
    btest = timing.Single_Proportion(df5minAfter, originSignal='确认点',mode='long',period='5min',
                                     contrast='000001.SH',labels=('new logic 5min',))

    # 显示画段的结果
    szFig = fig.Fig()
    close5min = sub.Lines(szFig.ax1, szFig.ax1Top,df5minAfter[['CLOSE']],
                          length=150,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    tech5min = sub.Lines(szFig.ax2, szFig.ax2Top,df5minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close5min.attach_wave_sign(df5minAfter[['CLOSE','波段拐点']],align='left')
    close5min.attach_buysell_sign(df5minAfter[['CLOSE','poChg']], align='left', mode='long')
    tech5min.attach_buysell_sign(df5minAfter[['DEA', 'poChg']], align='left', mode='long')
    close5min.add_synchron(tech5min)
    tech5min.add_synchron(close5min)

    szFig.show()

    del df5min, df5minAfter, btest, szFig, close5min, tech5min


# 显示000016.SH的分段情况，日线
if __name__ == '__main__':
    from tools.mplot import sub, fig
    from tools.backtest import signal, timing
    df1day = fetch.index_one('000016.SH')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)

    fenduan_new_logic(df1dayAfter)

    # 回测新逻辑及显示equityCurve
    signal.signal2position_single(df1dayAfter, signalCol='确认点', pxCol='CLOSE',
                                  mode='long', )
    btest = timing.Single_Proportion(df1dayAfter, originSignal='确认点', mode='long',
                                     contrast='000001.SH',)

    # 显示画段的结果
    szFig = fig.Fig()
    close1day = sub.Lines(szFig.ax1, szFig.ax1Top,df1dayAfter[['CLOSE']],
                          lenuplt=1000, step=50,
                          majorGrid='year', majorFormat='%Y',
                          minorGrid='month')
    tech1day = sub.Lines(szFig.ax2, szFig.ax2Top,df1dayAfter[['DEA']],baseLine=0,
                         lenuplt=1000, step=50,
                         majorGrid='year', majorFormat='%Y',
                         minorGrid='month')
    close1day.attach_wave_sign(df1dayAfter[['CLOSE','波段拐点']], align='left')
    close1day.attach_buysell_sign(df1dayAfter[['CLOSE','poChg']], align='left', mode='long')
    tech1day.attach_buysell_sign(df1dayAfter[['DEA', 'poChg']], align='left', mode='long')
    close1day.add_synchron(tech1day)
    tech1day.add_synchron(close1day)

    szFig.show()

    del df1day, df1dayAfter, btest, szFig, close1day, tech1day


# 显示000016.SH的分段情况，30min
if __name__ == '__main__':
    from tools.mplot import sub, fig
    from tools.backtest import signal, timing
    df30min = fetch.n_min('000016.SH',n=30,startTime='2014-2-10')
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)

    len(df30minAfter)

    fenduan_new_logic(df30minAfter)

    # 回测新逻辑及显示equityCurve
    signal.signal2position_single(df30minAfter, signalCol='确认点', pxCol='CLOSE',
                                  mode='long', )
    btest = timing.Single_Proportion(df30minAfter, originSignal='确认点',mode='long',period='30min',
                                     contrast='000001.SH',labels=('new logic 30min',))

    # 显示画段的结果
    szFig = fig.Fig()
    close30min = sub.Lines(szFig.ax1, szFig.ax1Top,df30minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500, step=50,
                           majorGrid='week', majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech30min = sub.Lines(szFig.ax2, szFig.ax2Top,df30minAfter[['DEA']],baseLine=0,
                          length=150,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    close30min.attach_wave_sign(df30minAfter[['CLOSE','波段拐点']],align='left')
    close30min.attach_buysell_sign(df30minAfter[['CLOSE','poChg']],align='left',mode='long')
    tech30min.attach_buysell_sign(df30minAfter[['DEA', 'poChg']], align='left', mode='long')
    close30min.add_synchron(tech30min)
    tech30min.add_synchron(close30min)

    szFig.show()

    del df30min, df30minAfter, btest, szFig, close30min, tech30min


# 显示000016.SH的分段情况，5min
if __name__ == '__main__':
    from tools.mplot import sub, fig
    from tools.backtest import signal, timing
    df5min = fetch.n_min('000016.SH',n=5,startTime='2014-2-10')
    a_a_DEA分段.cal_macd(df5min)
    df5min = df5min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_DEA_segment(df5min)

    fenduan_new_logic(df5minAfter)

    # 回测新逻辑及显示equityCurve
    signal.signal2position_single(df5minAfter, signalCol='确认点', pxCol='CLOSE',
                                  mode='long', )
    btest = timing.Single_Proportion(df5minAfter, originSignal='确认点',mode='long',period='5min',
                                     contrast='000001.SH',labels=('new logic 5min',))

    # 显示画段的结果
    szFig = fig.Fig()
    close5min = sub.Lines(szFig.ax1, szFig.ax1Top,df5minAfter[['CLOSE']],
                          length=150,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    tech5min = sub.Lines(szFig.ax2, szFig.ax2Top,df5minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close5min.attach_wave_sign(df5minAfter[['CLOSE','波段拐点']],align='left')
    close5min.attach_buysell_sign(df5minAfter[['CLOSE','poChg']], align='left', mode='long')
    tech5min.attach_buysell_sign(df5minAfter[['DEA', 'poChg']], align='left', mode='long')
    close5min.add_synchron(tech5min)
    tech5min.add_synchron(close5min)

    szFig.show()

    del df5min, df5minAfter, btest, szFig, close5min, tech5min


# 显示000300.SH的分段情况，日线
if __name__ == '__main__':
    from tools.mplot import sub, fig
    from tools.backtest import signal, timing
    df1day = fetch.index_one('000300.SH')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)

    fenduan_new_logic(df1dayAfter)

    # 回测新逻辑及显示equityCurve
    signal.signal2position_single(df1dayAfter, signalCol='确认点', pxCol='CLOSE',
                                  mode='long', )
    btest = timing.Single_Proportion(df1dayAfter, originSignal='确认点', mode='long',
                                     contrast='000001.SH',)

    # 显示画段的结果
    szFig = fig.Fig()
    close1day = sub.Lines(szFig.ax1, szFig.ax1Top,df1dayAfter[['CLOSE']],
                          lenuplt=1000, step=50,
                          majorGrid='year', majorFormat='%Y',
                          minorGrid='month')
    tech1day = sub.Lines(szFig.ax2, szFig.ax2Top,df1dayAfter[['DEA']],baseLine=0,
                         lenuplt=1000, step=50,
                         majorGrid='year', majorFormat='%Y',
                         minorGrid='month')
    close1day.attach_wave_sign(df1dayAfter[['CLOSE','波段拐点']], align='left')
    close1day.attach_buysell_sign(df1dayAfter[['CLOSE','poChg']], align='left', mode='long')
    tech1day.attach_buysell_sign(df1dayAfter[['DEA', 'poChg']], align='left', mode='long')
    close1day.add_synchron(tech1day)
    tech1day.add_synchron(close1day)

    szFig.show()

    del df1day, df1dayAfter, btest, szFig, close1day, tech1day


# 显示000300.SH的分段情况，30min
if __name__ == '__main__':
    from tools.mplot import sub, fig
    from tools.backtest import signal, timing
    df30min = fetch.n_min('000300.SH',n=30,startTime='2014-2-10')
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)

    fenduan_new_logic(df30minAfter)

    # 回测新逻辑及显示equityCurve
    signal.signal2position_single(df30minAfter, signalCol='确认点', pxCol='CLOSE',
                                  mode='long', )
    btest = timing.Single_Proportion(df30minAfter, originSignal='确认点',mode='long',period='30min',
                                     contrast='000001.SH',labels=('new logic 30min',))

    # 显示画段的结果
    szFig = fig.Fig()
    close30min = sub.Lines(szFig.ax1, szFig.ax1Top,df30minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500, step=50,
                           majorGrid='week', majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech30min = sub.Lines(szFig.ax2, szFig.ax2Top,df30minAfter[['DEA']],baseLine=0,
                          length=150,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    close30min.attach_wave_sign(df30minAfter[['CLOSE','波段拐点']],align='left')
    close30min.attach_buysell_sign(df30minAfter[['CLOSE','poChg']],align='left',mode='long')
    tech30min.attach_buysell_sign(df30minAfter[['DEA', 'poChg']], align='left', mode='long')
    close30min.add_synchron(tech30min)
    tech30min.add_synchron(close30min)

    szFig.show()

    del df30min, df30minAfter, btest, szFig, close30min, tech30min


# 显示000300.SH的分段情况，5min
if __name__ == '__main__':
    from tools.mplot import sub, fig
    from tools.backtest import signal, timing
    df5min = fetch.n_min('000300.SH',n=5,startTime='2014-2-10')
    a_a_DEA分段.cal_macd(df5min)
    df5min = df5min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_DEA_segment(df5min)

    fenduan_new_logic(df5minAfter)

    # 回测新逻辑及显示equityCurve
    signal.signal2position_single(df5minAfter, signalCol='确认点', pxCol='CLOSE',
                                  mode='long', )
    btest = timing.Single_Proportion(df5minAfter, originSignal='确认点',mode='long',period='5min',
                                     contrast='000001.SH',labels=('new logic 5min',))

    # 显示画段的结果
    szFig = fig.Fig()
    close5min = sub.Lines(szFig.ax1, szFig.ax1Top,df5minAfter[['CLOSE']],
                          length=150,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    tech5min = sub.Lines(szFig.ax2, szFig.ax2Top,df5minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close5min.attach_wave_sign(df5minAfter[['CLOSE','波段拐点']],align='left')
    close5min.attach_buysell_sign(df5minAfter[['CLOSE','poChg']], align='left', mode='long')
    tech5min.attach_buysell_sign(df5minAfter[['DEA', 'poChg']], align='left', mode='long')
    close5min.add_synchron(tech5min)
    tech5min.add_synchron(close5min)

    szFig.show()

    del df5min, df5minAfter, btest, szFig, close5min, tech5min

# 总结：总体胜率很低，20%以下，5分钟的收益较好，

########################################################################################################################
# 新画段逻辑日线和30min联动，显示到最近
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related,pandas_related

    # 1天的线
    df1day = fetch.index_one('000001.SH')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)
    fenduan_new_logic(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('000001.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    # 由于两个After都截断过，用最长的df30min
    df30min['波段拐点'] = df30minAfter['波段拐点']
    df30min['波段拐点'].fillna(0,inplace=True)
    # 将day的归入30min
    pandas_related.merge_day2min(df1dayAfter[['波段拐点']], df30min, '波段拐点_大')
    pandas_related.merge_day2min(df1day[['DEA']], df30min, 'DEA_大',fillna=None)

    curFig = fig.Fig(subplotdict={'axClose':[0.05,0.43,0.9,0.55],
                                  'axTechShort':[0.05,0.2,0.9,0.18],
                                  'axTechLong':[0.05,0.00,0.9,0.18]},
                     if_top_need=(True,True,True))

    close = sub.Lines(curFig.axClose, curFig.axCloseTop, df30min[['CLOSE']],
                      endNum=-1,length=200,
                      lenuplt=1500, step=50,
                      majorGrid='week', majorFormat='%Y-%m-%d',
                      minorGrid='day',
                      dtFormat='%Y-%m-%d %H:%M:%S')
    techShort = sub.Lines(curFig.axTechShort,curFig.axTechShortTop,df30min[['DEA']],baseLine=0,
                          endNum=-1, length=200,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df30min[['DEA_大']],baseLine=0,
                         endNum=-1, length=200,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')

    close.attach_wave_sign(df30min[['CLOSE','波段拐点']])
    close.attach_wave_sign(df30min[['CLOSE','波段拐点_大']],color='cyan')

    close.add_synchron(techShort, techLong)
    techShort.add_synchron(close, techLong)
    techLong.add_synchron(close, techShort)

    curFig.show()

    del close, curFig, df1day, df1dayAfter, df30min, df30minAfter, techLong, techShort


########################################################################################################################
# 新画段逻辑日线和30min的小级别合并，000001.SH
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related,pandas_related

    # 1天的线
    df1day = fetch.index_one('000001.SH')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)
    fenduan_new_logic(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('000001.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    # 由于两个After都截断过，用最长的df30min
    df30min['波段拐点'] = df30minAfter['波段拐点']
    df30min['波段拐点'].fillna(0,inplace=True)
    # 将day的归入30min
    pandas_related.merge_day2min(df1dayAfter[['波段拐点']], df30min, '波段拐点_大')
    pandas_related.merge_day2min(df1day[['DEA']], df30min, 'DEA_大',fillna=None)

    long_attach_2_short(df30min, '波段拐点_大', '波段拐点', '波段拐点_大_合并',mismatchThres=8*5)

    curFig = fig.Fig(subplotdict={'axClose':[0.05,0.43,0.9,0.55],
                                  'axTechShort':[0.05,0.2,0.9,0.18],
                                  'axTechLong':[0.05,0.00,0.9,0.18]},
                     if_top_need=(True,True,True))
    close = sub.Lines(curFig.axClose, curFig.axCloseTop, df30min[['CLOSE']],
                      length=200,
                      lenuplt=1500, step=50,
                      majorGrid='week', majorFormat='%Y-%m-%d',
                      minorGrid='day',
                      dtFormat='%Y-%m-%d %H:%M:%S')
    techShort = sub.Lines(curFig.axTechShort,curFig.axTechShortTop,df30min[['DEA']],baseLine=0,
                          length=200,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df30min[['DEA_大']],baseLine=0,
                         length=200,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')

    close.attach_wave_sign(df30min[['CLOSE','波段拐点']],)
    close.attach_wave_sign(df30min[['CLOSE','波段拐点_大']],color='cyan')
    close.attach_wave_sign(df30min[['CLOSE','波段拐点_大_合并']],color='red')

    close.add_synchron(techShort,techLong)
    techShort.add_synchron(close,techLong)
    techLong.add_synchron(close,techShort)

    curFig.show()

    del df1dayAfter, df1day, df30min, df30minAfter, curFig, close, techLong, techShort


# 新画段逻辑30min和5min的小级别合并，000001.SH
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig

    # 30分钟的线
    df30min = fetch.n_min('000001.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    # 5分钟的线
    df5min = fetch.n_min('000001.SH',n=5)
    a_a_DEA分段.cal_macd(df5min)
    df5min = df5min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_DEA_segment(df5min)
    fenduan_new_logic(df5minAfter)

    # 由于两个After都截断过，用最长的df5min
    df5min['波段拐点'] = df5minAfter['波段拐点']
    df5min['波段拐点'].fillna(0,inplace=True)
    # 将30min的归入5min
    df5min['波段拐点_大'] = df30minAfter['波段拐点']
    df5min['波段拐点_大'].fillna(0.0,inplace=True)
    df5min['DEA_大'] = df30minAfter['DEA']

    long_attach_2_short(df5min, '波段拐点_大', '波段拐点', '波段拐点_大_合并',mismatchThres=6*8)

    curFig = fig.Fig(subplotdict={'axClose':[0.05,0.43,0.9,0.55],
                                  'axTechShort':[0.05,0.2,0.9,0.18],
                                  'axTechLong':[0.05,0.00,0.9,0.18]},
                     if_top_need=(True,True,True))
    close = sub.Lines(curFig.axClose, curFig.axCloseTop, df5min[['CLOSE']],
                      length=200,
                      lenuplt=1500, step=50,
                      majorGrid='week', majorFormat='%Y-%m-%d',
                      minorGrid='day',
                      dtFormat='%Y-%m-%d %H:%M:%S')
    techShort = sub.Lines(curFig.axTechShort,curFig.axTechShortTop,df5min[['DEA']], baseLine=0,
                          length=200,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df5min[['DEA_大']], baseLine=0,
                         length=200,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')

    close.attach_wave_sign(df5min[['CLOSE','波段拐点']],)
    close.attach_wave_sign(df5min[['CLOSE','波段拐点_大']],color='cyan')
    close.attach_wave_sign(df5min[['CLOSE','波段拐点_大_合并']],color='red')

    close.add_synchron(techShort,techLong)
    techShort.add_synchron(close,techLong)
    techLong.add_synchron(close,techShort)

    curFig.show()

    del df30minAfter, df30min, df5min, df5minAfter, curFig, close, techLong, techShort


# 新画段逻辑日线和30min的小级别合并，000016.SH
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related,pandas_related

    # 1天的线
    df1day = fetch.index_one('000016.SH')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)
    fenduan_new_logic(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('000016.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    # 由于两个After都截断过，用最长的df30min
    df30min['波段拐点'] = df30minAfter['波段拐点']
    df30min['波段拐点'].fillna(0,inplace=True)
    # 将day的归入30min
    pandas_related.merge_day2min(df1dayAfter[['波段拐点']], df30min, '波段拐点_大')
    pandas_related.merge_day2min(df1day[['DEA']], df30min, 'DEA_大',fillna=None)

    long_attach_2_short(df30min, '波段拐点_大', '波段拐点', '波段拐点_大_合并',mismatchThres=8*5)

    curFig = fig.Fig(subplotdict={'axClose':[0.05,0.43,0.9,0.55],
                                  'axTechShort':[0.05,0.2,0.9,0.18],
                                  'axTechLong':[0.05,0.00,0.9,0.18]},
                     if_top_need=(True,True,True))
    close = sub.Lines(curFig.axClose, curFig.axCloseTop, df30min[['CLOSE']],
                      length=200,
                      lenuplt=1500, step=50,
                      majorGrid='week', majorFormat='%Y-%m-%d',
                      minorGrid='day',
                      dtFormat='%Y-%m-%d %H:%M:%S')
    techShort = sub.Lines(curFig.axTechShort,curFig.axTechShortTop,df30min[['DEA']],baseLine=0,
                          length=200,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df30min[['DEA_大']],baseLine=0,
                         length=200,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')

    close.attach_wave_sign(df30min[['CLOSE','波段拐点']],)
    close.attach_wave_sign(df30min[['CLOSE','波段拐点_大']],color='cyan')
    close.attach_wave_sign(df30min[['CLOSE','波段拐点_大_合并']],color='red')

    close.add_synchron(techShort,techLong)
    techShort.add_synchron(close,techLong)
    techLong.add_synchron(close,techShort)

    curFig.show()

    del df1dayAfter, df1day, df30min, df30minAfter, curFig, close, techLong, techShort


# 新画段逻辑30min和5min的小级别合并，000016.SH
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related,pandas_related

    # 30分钟的线
    df30min = fetch.n_min('000016.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    # 5分钟的线
    df5min = fetch.n_min('000016.SH',n=5)
    a_a_DEA分段.cal_macd(df5min)
    df5min = df5min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_DEA_segment(df5min)
    fenduan_new_logic(df5minAfter)

    # 由于两个After都截断过，用最长的df5min
    df5min['波段拐点'] = df5minAfter['波段拐点']
    df5min['波段拐点'].fillna(0,inplace=True)
    # 将30min的归入5min
    df5min['波段拐点_大'] = df30minAfter['波段拐点']
    df5min['波段拐点_大'].fillna(0.0,inplace=True)
    df5min['DEA_大'] = df30minAfter['DEA']

    long_attach_2_short(df5min, '波段拐点_大', '波段拐点', '波段拐点_大_合并',mismatchThres=6*8)

    curFig = fig.Fig(subplotdict={'axClose':[0.05,0.43,0.9,0.55],
                                  'axTechShort':[0.05,0.2,0.9,0.18],
                                  'axTechLong':[0.05,0.00,0.9,0.18]},
                     if_top_need=(True,True,True))
    close = sub.Lines(curFig.axClose, curFig.axCloseTop, df5min[['CLOSE']],
                      length=200,
                      lenuplt=1500, step=50,
                      majorGrid='week', majorFormat='%Y-%m-%d',
                      minorGrid='day',
                      dtFormat='%Y-%m-%d %H:%M:%S')
    techShort = sub.Lines(curFig.axTechShort,curFig.axTechShortTop,df5min[['DEA']], baseLine=0,
                          length=200,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df5min[['DEA_大']], baseLine=0,
                         length=200,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')

    close.attach_wave_sign(df5min[['CLOSE','波段拐点']],)
    close.attach_wave_sign(df5min[['CLOSE','波段拐点_大']],color='cyan')
    close.attach_wave_sign(df5min[['CLOSE','波段拐点_大_合并']],color='red')

    close.add_synchron(techShort,techLong)
    techShort.add_synchron(close,techLong)
    techLong.add_synchron(close,techShort)

    curFig.show()

    close.select_date('2016-08-04 10:30:00')

    del df30minAfter, df30min, df5min, df5minAfter, curFig, close, techLong, techShort


# 新画段逻辑日线和30min的小级别合并，000300.SH
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related,pandas_related

    # 1天的线
    df1day = fetch.index_one('000300.SH')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)
    fenduan_new_logic(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('000300.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    # 由于两个After都截断过，用最长的df30min
    df30min['波段拐点'] = df30minAfter['波段拐点']
    df30min['波段拐点'].fillna(0,inplace=True)
    # 将day的归入30min
    pandas_related.merge_day2min(df1dayAfter[['波段拐点']], df30min, '波段拐点_大')
    pandas_related.merge_day2min(df1day[['DEA']], df30min, 'DEA_大',fillna=None)

    long_attach_2_short(df30min, '波段拐点_大', '波段拐点', '波段拐点_大_合并',mismatchThres=8*5)

    curFig = fig.Fig(subplotdict={'axClose': [0.05,0.43,0.9,0.55],
                                  'axTechShort': [0.05,0.2,0.9,0.18],
                                  'axTechLong': [0.05,0.00,0.9,0.18]},
                     if_top_need=(True,True,True))
    close = sub.Lines(curFig.axClose, curFig.axCloseTop, df30min[['CLOSE']],
                      length=200,
                      lenuplt=1500, step=50,
                      majorGrid='week', majorFormat='%Y-%m-%d',
                      minorGrid='day',
                      dtFormat='%Y-%m-%d %H:%M:%S')
    techShort = sub.Lines(curFig.axTechShort,curFig.axTechShortTop,df30min[['DEA']],baseLine=0,
                          length=200,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df30min[['DEA_大']],baseLine=0,
                         length=200,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')

    close.attach_wave_sign(df30min[['CLOSE','波段拐点']],)
    close.attach_wave_sign(df30min[['CLOSE','波段拐点_大']], color='cyan')
    close.attach_wave_sign(df30min[['CLOSE','波段拐点_大_合并']], color='red')

    close.add_synchron(techShort,techLong)
    techShort.add_synchron(close,techLong)
    techLong.add_synchron(close,techShort)

    curFig.show()


    del df1dayAfter, df1day, df30min, df30minAfter, curFig, close, techLong, techShort


# 新画段逻辑30min和5min的小级别合并，000300.SH
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig

    # 30分钟的线
    df30min = fetch.n_min('000300.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    # 5分钟的线
    df5min = fetch.n_min('000300.SH',n=5)
    a_a_DEA分段.cal_macd(df5min)
    df5min = df5min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_DEA_segment(df5min)
    fenduan_new_logic(df5minAfter)

    # 由于两个After都截断过，用最长的df5min
    df5min['波段拐点'] = df5minAfter['波段拐点']
    df5min['波段拐点'].fillna(0,inplace=True)
    # 将30min的归入5min
    df5min['波段拐点_大'] = df30minAfter['波段拐点']
    df5min['波段拐点_大'].fillna(0.0,inplace=True)
    df5min['DEA_大'] = df30minAfter['DEA']

    long_attach_2_short(df5min, '波段拐点_大', '波段拐点', '波段拐点_大_合并',mismatchThres=6*8)

    curFig = fig.Fig(subplotdict={'axClose':[0.05,0.43,0.9,0.55],
                                  'axTechShort':[0.05,0.2,0.9,0.18],
                                  'axTechLong':[0.05,0.00,0.9,0.18]},
                     if_top_need=(True,True,True))
    close = sub.Lines(curFig.axClose, curFig.axCloseTop, df5min[['CLOSE']],
                      length=200,
                      lenuplt=1500, step=50,
                      majorGrid='week', majorFormat='%Y-%m-%d',
                      minorGrid='day',
                      dtFormat='%Y-%m-%d %H:%M:%S')
    techShort = sub.Lines(curFig.axTechShort,curFig.axTechShortTop,df5min[['DEA']], baseLine=0,
                          length=200,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df5min[['DEA_大']], baseLine=0,
                         length=200,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')

    close.attach_wave_sign(df5min[['CLOSE','波段拐点']],)
    close.attach_wave_sign(df5min[['CLOSE','波段拐点_大']],color='cyan')
    close.attach_wave_sign(df5min[['CLOSE','波段拐点_大_合并']],color='red')

    close.add_synchron(techShort,techLong)
    techShort.add_synchron(close,techLong)
    techLong.add_synchron(close,techShort)

    curFig.show()

    del df30minAfter, df30min, df5min, df5minAfter, curFig, close, techLong, techShort


# 新画段逻辑日线和30min的小级别合并，399006.SZ
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related,pandas_related

    # 1天的线
    df1day = fetch.index_one('399006.SZ')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)
    fenduan_new_logic(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('399006.SZ',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    # 由于两个After都截断过，用最长的df30min
    df30min['波段拐点'] = df30minAfter['波段拐点']
    df30min['波段拐点'].fillna(0,inplace=True)
    # 将day的归入30min
    pandas_related.merge_day2min(df1dayAfter[['波段拐点']], df30min, '波段拐点_大')
    pandas_related.merge_day2min(df1day[['DEA']], df30min, 'DEA_大',fillna=None)

    long_attach_2_short(df30min, '波段拐点_大', '波段拐点', '波段拐点_大_合并',mismatchThres=8*5)

    curFig = fig.Fig(subplotdict={'axClose': [0.05,0.43,0.9,0.55],
                                  'axTechShort': [0.05,0.2,0.9,0.18],
                                  'axTechLong': [0.05,0.00,0.9,0.18]},
                     if_top_need=(True,True,True))
    close = sub.Lines(curFig.axClose, curFig.axCloseTop, df30min[['CLOSE']],
                      length=200,
                      lenuplt=1500, step=50,
                      majorGrid='week', majorFormat='%Y-%m-%d',
                      minorGrid='day',
                      dtFormat='%Y-%m-%d %H:%M:%S')
    techShort = sub.Lines(curFig.axTechShort,curFig.axTechShortTop,df30min[['DEA']],baseLine=0,
                          length=200,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df30min[['DEA_大']],baseLine=0,
                         length=200,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')

    close.attach_wave_sign(df30min[['CLOSE','波段拐点']],)
    close.attach_wave_sign(df30min[['CLOSE','波段拐点_大']], color='cyan')
    close.attach_wave_sign(df30min[['CLOSE','波段拐点_大_合并']], color='red')

    close.add_synchron(techShort,techLong)
    techShort.add_synchron(close,techLong)
    techLong.add_synchron(close,techShort)

    curFig.show()

    del df1dayAfter, df1day, df30min, df30minAfter, curFig, close, techLong, techShort


# 新画段逻辑30min和5min的小级别合并，399006.SZ
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related,pandas_related

    # 30分钟的线
    df30min = fetch.n_min('399006.SZ',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    # 5分钟的线
    df5min = fetch.n_min('399006.SZ',n=5)
    a_a_DEA分段.cal_macd(df5min)
    df5min = df5min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_DEA_segment(df5min)
    fenduan_new_logic(df5minAfter)

    # 由于两个After都截断过，用最长的df5min
    df5min['波段拐点'] = df5minAfter['波段拐点']
    df5min['波段拐点'].fillna(0,inplace=True)
    # 将30min的归入5min
    df5min['波段拐点_大'] = df30minAfter['波段拐点']
    df5min['波段拐点_大'].fillna(0.0,inplace=True)
    df5min['DEA_大'] = df30minAfter['DEA']

    long_attach_2_short(df5min, '波段拐点_大', '波段拐点', '波段拐点_大_合并',mismatchThres=6*8)

    curFig = fig.Fig(subplotdict={'axClose':[0.05,0.43,0.9,0.55],
                                  'axTechShort':[0.05,0.2,0.9,0.18],
                                  'axTechLong':[0.05,0.00,0.9,0.18]},
                     if_top_need=(True,True,True))
    close = sub.Lines(curFig.axClose, curFig.axCloseTop, df5min[['CLOSE']],
                      length=200,
                      lenuplt=1500, step=50,
                      majorGrid='week', majorFormat='%Y-%m-%d',
                      minorGrid='day',
                      dtFormat='%Y-%m-%d %H:%M:%S')
    techShort = sub.Lines(curFig.axTechShort,curFig.axTechShortTop,df5min[['DEA']], baseLine=0,
                          length=200,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df5min[['DEA_大']], baseLine=0,
                         length=200,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')

    close.attach_wave_sign(df5min[['CLOSE','波段拐点']],)
    close.attach_wave_sign(df5min[['CLOSE','波段拐点_大']],color='cyan')
    close.attach_wave_sign(df5min[['CLOSE','波段拐点_大_合并']],color='red')

    close.add_synchron(techShort,techLong)
    techShort.add_synchron(close,techLong)
    techLong.add_synchron(close,techShort)

    curFig.show()

    del df30minAfter, df30min, df5min, df5minAfter, curFig, close, techLong, techShort


########################################################################################################################
# 新逻辑的波段简单统计分析，000001.SH
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related, pandas_related

    # 1天的线
    df1day = fetch.index_one('000001.SH')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)
    fenduan_new_logic(df1dayAfter)

    pandas_related.gen_wave_group_key(df1dayAfter,waveGroupKeyName='waveGroupKey',groupSign='波段拐点')
    detailDf1day = df1dayAfter.groupby('waveGroupKey').apply(_find_minMax)
    stat1day = gen_stat_rst(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('000001.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    pandas_related.gen_wave_group_key(df30minAfter,waveGroupKeyName='waveGroupKey',groupSign='波段拐点')
    detailDf30min = df30minAfter.groupby('waveGroupKey').apply(_find_minMax)
    stat30min = gen_stat_rst(df30minAfter)

    # 5分钟的线
    df5min = fetch.n_min('000001.SH',n=5)
    a_a_DEA分段.cal_macd(df5min)
    df5min = df5min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_DEA_segment(df5min)
    fenduan_new_logic(df5minAfter)

    pandas_related.gen_wave_group_key(df5minAfter,waveGroupKeyName='waveGroupKey',groupSign='波段拐点')
    detailDf5min = df5minAfter.groupby('waveGroupKey').apply(_find_minMax)
    stat5min = gen_stat_rst(df5minAfter)

    del df1dayAfter, df1day,detailDf1day,stat1day, \
        df30min, df30minAfter,detailDf30min,stat30min, \
        df5min, df5minAfter,detailDf5min,stat5min


# 新逻辑的波段简单统计分析，000016.SH
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related, pandas_related

    # 1天的线
    df1day = fetch.index_one('000016.SH')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)
    fenduan_new_logic(df1dayAfter)

    pandas_related.gen_wave_group_key(df1dayAfter,waveGroupKeyName='waveGroupKey',groupSign='波段拐点')
    detailDf1day = df1dayAfter.groupby('waveGroupKey').apply(_find_minMax)
    stat1day = gen_stat_rst(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('000016.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    pandas_related.gen_wave_group_key(df30minAfter,waveGroupKeyName='waveGroupKey',groupSign='波段拐点')
    detailDf30min = df30minAfter.groupby('waveGroupKey').apply(_find_minMax)
    stat30min = gen_stat_rst(df30minAfter)

    # 5分钟的线
    df5min = fetch.n_min('000016.SH',n=5)
    a_a_DEA分段.cal_macd(df5min)
    df5min = df5min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_DEA_segment(df5min)
    fenduan_new_logic(df5minAfter)

    pandas_related.gen_wave_group_key(df5minAfter,waveGroupKeyName='waveGroupKey',groupSign='波段拐点')
    detailDf5min = df5minAfter.groupby('waveGroupKey').apply(_find_minMax)
    stat5min = gen_stat_rst(df5minAfter)

    del df1dayAfter, df1day,detailDf1day,stat1day, \
        df30min, df30minAfter,detailDf30min,stat30min, \
        df5min, df5minAfter,detailDf5min,stat5min


# 新逻辑的波段简单统计分析，399006.SZ
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related, pandas_related

    # 1天的线
    df1day = fetch.index_one('399006.SZ')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)
    fenduan_new_logic(df1dayAfter)

    pandas_related.gen_wave_group_key(df1dayAfter,waveGroupKeyName='waveGroupKey',groupSign='波段拐点')
    detailDf1day = df1dayAfter.groupby('waveGroupKey').apply(_find_minMax)
    stat1day = gen_stat_rst(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('399006.SZ',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    pandas_related.gen_wave_group_key(df30minAfter,waveGroupKeyName='waveGroupKey',groupSign='波段拐点')
    detailDf30min = df30minAfter.groupby('waveGroupKey').apply(_find_minMax)
    stat30min = gen_stat_rst(df30minAfter)

    # 5分钟的线
    df5min = fetch.n_min('399006.SZ',n=5)
    a_a_DEA分段.cal_macd(df5min)
    df5min = df5min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_DEA_segment(df5min)
    fenduan_new_logic(df5minAfter)

    pandas_related.gen_wave_group_key(df5minAfter,waveGroupKeyName='waveGroupKey',groupSign='波段拐点')
    detailDf5min = df5minAfter.groupby('waveGroupKey').apply(_find_minMax)
    stat5min = gen_stat_rst(df5minAfter)

    del df1dayAfter, df1day,detailDf1day,stat1day, \
        df30min, df30minAfter,detailDf30min,stat30min, \
        df5min, df5minAfter,detailDf5min,stat5min


# 新逻辑的波段简单统计分析，000300.SH
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related, pandas_related

    # 1天的线
    df1day = fetch.index_one('000300.SH')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)
    fenduan_new_logic(df1dayAfter)

    pandas_related.gen_wave_group_key(df1dayAfter,waveGroupKeyName='waveGroupKey',groupSign='波段拐点')
    detailDf1day = df1dayAfter.groupby('waveGroupKey').apply(_find_minMax)
    stat1day = gen_stat_rst(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('000300.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    pandas_related.gen_wave_group_key(df30minAfter,waveGroupKeyName='waveGroupKey',groupSign='波段拐点')
    detailDf30min = df30minAfter.groupby('waveGroupKey').apply(_find_minMax)
    stat30min = gen_stat_rst(df30minAfter)

    # 5分钟的线
    df5min = fetch.n_min('000300.SH',n=5)
    a_a_DEA分段.cal_macd(df5min)
    df5min = df5min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_DEA_segment(df5min)
    fenduan_new_logic(df5minAfter)

    pandas_related.gen_wave_group_key(df5minAfter,waveGroupKeyName='waveGroupKey',groupSign='波段拐点')
    detailDf5min = df5minAfter.groupby('waveGroupKey').apply(_find_minMax)
    stat5min = gen_stat_rst(df5minAfter)

    del df1dayAfter, df1day,detailDf1day,stat1day, \
        df30min, df30minAfter,detailDf30min,stat30min, \
        df5min, df5minAfter,detailDf5min,stat5min


########################################################################################################################
# 新逻辑的波段联动分析，日线和30min，000001.SH
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import pandas_related

    # 1天的线
    df1day = fetch.index_one('000001.SH')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)
    fenduan_new_logic(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('000001.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    # 由于两个After都截断过，用最长的df30min
    df30min['波段拐点'] = df30minAfter['波段拐点']
    df30min['波段拐点'].fillna(0,inplace=True)
    # 将day的归入30min
    pandas_related.merge_day2min(df1dayAfter[['波段拐点']], df30min, '波段拐点_大')
    pandas_related.merge_day2min(df1dayAfter[['确认点']], df30min, '确认点_大')
    pandas_related.merge_day2min(df1day[['DEA']], df30min, 'DEA_大',fillna=None)

    # 将大级别的波段记号附到小级别的记号上面，差距在8个大周期之内的直接附，大于8期的附到高低点那里
    long_attach_2_short(df30min, '波段拐点_大', '波段拐点', '波段拐点_大_合并',mismatchThres=8*5)

    pandas_related.gen_wave_group_key(df30min, waveGroupKeyName='waveGroupKey', groupSign='波段拐点_大_合并')


    df30minCut = df30min[df30min[df30min['波段拐点_大_合并'] != 0.0].index[0]:
                         df30min[df30min['波段拐点_大_合并'] != 0.0].index[-1]]
    # group = df30minCut.groupby('waveGroupKey').get_group(1)

    liandongStat = df30minCut.groupby('waveGroupKey').apply(sub_wave_analysis)
    liandongStat.reset_index(level=1,drop=True,inplace=True)

    # 大级别波段对应的小级别波段数
    freqStatDf = liandongStat.groupby('涨跌').apply(pandas_related.freq_stat,freqCol='总段数')
    hebinghoufreqStatDf = liandongStat.groupby('涨跌').apply(pandas_related.freq_stat,freqCol='合并后总段数')

    # 下面统计的是合并前的段的情况
    afterConfirmStat = liandongStat.groupby('涨跌')[
        ['日线确认后延续幅度','日线确认后延续时长','日线确认前延续幅度','日线确认前延续时长']
                                                   ].apply(after_confirm_stat)

    del df1dayAfter, df1day, df30min, df30minAfter, df30minCut, \
        liandongStat, freqStatDf, hebinghoufreqStatDf, afterConfirmStat


# 新逻辑的波段联动统计分析，30min和5min，000001.SH
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import pandas_related


    # 30分钟的线
    df30min = fetch.n_min('000001.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    # 5分钟的线
    df5min = fetch.n_min('000001.SH',n=5)
    a_a_DEA分段.cal_macd(df5min)
    df5min = df5min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_DEA_segment(df5min)
    fenduan_new_logic(df5minAfter)

    # 由于两个After都截断过，用最长的df5min
    df5min['波段拐点'] = df5minAfter['波段拐点']
    df5min['波段拐点'].fillna(0,inplace=True)
    # 将30min的归入5min
    df5min['波段拐点_大'] = df30minAfter['波段拐点']
    df5min['波段拐点_大'].fillna(0.0,inplace=True)
    df5min['确认点_大'] = df30minAfter['确认点']
    df5min['确认点_大'].fillna(0.0,inplace=True)
    df5min['DEA_大'] = df30minAfter['DEA']
    # 将大级别的波段记号附到小级别的记号上面，差距在8个大周期之内的直接附，大于6期的附到高低点那里
    long_attach_2_short(df5min, '波段拐点_大', '波段拐点', '波段拐点_大_合并',mismatchThres=6*8)

    pandas_related.gen_wave_group_key(df5min, waveGroupKeyName='waveGroupKey', groupSign='波段拐点_大_合并')
    df5minCut = df5min[df5min[df5min['波段拐点_大_合并'] != 0.0].index[0]:
                         df5min[df5min['波段拐点_大_合并'] != 0.0].index[-1]]

    liandongStat = df5minCut.groupby('waveGroupKey').apply(sub_wave_analysis)
    liandongStat.reset_index(level=1,drop=True,inplace=True)

    # 大级别波段对应的小级别波段数
    freqStatDf = liandongStat.groupby('涨跌').apply(pandas_related.freq_stat,freqCol='总段数')
    hebinghoufreqStatDf = liandongStat.groupby('涨跌').apply(pandas_related.freq_stat,freqCol='合并后总段数')

    # 下面统计的是合并前的段的情况
    afterConfirmStat = liandongStat.groupby('涨跌')[
        ['日线确认后延续幅度','日线确认后延续时长','日线确认前延续幅度','日线确认前延续时长']
                                                   ].apply(after_confirm_stat)

    del df30minAfter, df30min, df5min, df5minAfter, df5minCut, \
        liandongStat, freqStatDf, hebinghoufreqStatDf, afterConfirmStat


########################################################################################################################
# 新逻辑的联动策略，日线和30min，000001.SH
# 30分钟的策略如果联立了日线策略的话胜率降低，收益略微提升，但是还是很低。
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import pandas_related
    from tools.backtest import signal, timing

    # 1天的线
    df1day = fetch.index_one('000001.SH')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)
    fenduan_new_logic(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('000001.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    # 由于两个After都截断过，用最长的df30min
    df30min['波段拐点'] = df30minAfter['波段拐点']
    df30min['波段拐点'].fillna(0,inplace=True)
    df30min['确认点'] = df30minAfter['确认点']
    df30min['确认点'].fillna(0,inplace=True)
    # 将day的归入30min
    pandas_related.merge_day2min(df1dayAfter[['波段拐点']], df30min, '波段拐点_大')
    pandas_related.merge_day2min(df1dayAfter[['确认点']], df30min, '确认点_大')
    pandas_related.merge_day2min(df1day[['DEA']], df30min, 'DEA_大',fillna=None)
    # 将大级别确认点做成开仓条件
    df30min.loc[df30min['确认点_大'] == 0, '确认点_大'] = None
    df30min['确认点_大'].fillna(method='ffill',inplace=True)
    df30min['确认点_大'].fillna(0,inplace=True)
    df30min['确认点_大'] = df30min['确认点_大'].map({1:True, 0:False,-1:False})

    signal.daxiao_liandong(df30min, '确认点', '确认点_大',)
    signal.signal2position_single(df30min, mode='long')

    timing.single_proportion_backtest(df30min,)
    detailDf = timing.single_trade_details(df30min)
    evalSer = timing.overall_eval(df30min, detailDf)
    yearDf = timing.year_month_analysis(detailDf)

    equityFig = fig.Equity_Fig()
    sub.draw_series_on_axes(equityFig.axDesc, evalSer, color='white')
    sub.draw_month_analysis(equityFig.axYear, yearDf)
    equityCurve = sub.Equity_Curve(equityFig.axEquity, df30min[['equityCurve']], show_x_tick=False, period='30min')

    equityFig.show()


    curFig = fig.Fig(subplotdict={'axClose':[0.05,0.43,0.9,0.55],
                                  'axTechShort':[0.05,0.2,0.9,0.18],
                                  'axTechLong':[0.05,0.00,0.9,0.18]},
                     if_top_need=(True,True,True))
    close = sub.Lines(curFig.axClose, curFig.axCloseTop, df30min[['CLOSE']],
                      length=200,
                      lenuplt=1500, step=50,
                      majorGrid='week', majorFormat='%Y-%m-%d',
                      minorGrid='day',
                      dtFormat='%Y-%m-%d %H:%M:%S')
    techShort = sub.Lines(curFig.axTechShort,curFig.axTechShortTop,df30min[['DEA']],baseLine=0,
                          length=200,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df30min[['DEA_大']],baseLine=0,
                         length=200,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')

    close.attach_wave_sign(df30min[['CLOSE','波段拐点']],)
    close.attach_wave_sign(df30min[['CLOSE','波段拐点_大']],color='cyan')
    close.attach_buysell_sign(df30min[['CLOSE','poChg']],mode='long')

    close.add_synchron(techShort,techLong)
    techShort.add_synchron(close,techLong)
    techLong.add_synchron(close,techShort)

    curFig.show()


    del df1dayAfter, df1day, df30min, df30minAfter, \
        detailDf, evalSer, yearDf, \
        equityFig, equityCurve, curFig, close, techShort, techLong


########################################################################################################################
# 大级别确认点后不创新高新低离场可行性分析
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import pandas_related
    from tools.backtest import signal, timing

    # 1天的线
    df1day = fetch.index_one('000001.SH')
    a_a_DEA分段.cal_macd(df1day)
    df1day = df1day[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_DEA_segment(df1day)
    fenduan_new_logic(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('000001.SH',n=30)
    a_a_DEA分段.cal_macd(df30min)
    df30min = df30min[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_DEA_segment(df30min)
    fenduan_new_logic(df30minAfter)

    # 由于两个After都截断过，用最长的df30min
    df30min['波段拐点'] = df30minAfter['波段拐点']
    df30min['波段拐点'].fillna(0,inplace=True)
    df30min['确认点'] = df30minAfter['确认点']
    df30min['确认点'].fillna(0,inplace=True)
    # 将day的归入30min
    pandas_related.merge_day2min(df1dayAfter[['波段拐点']], df30min, '波段拐点_大')
    pandas_related.merge_day2min(df1dayAfter[['确认点']], df30min, '确认点_大')
    pandas_related.merge_day2min(df1day[['DEA']], df30min, 'DEA_大',fillna=None)

    # 策略：日线确认点开仓，不创新高新低的确认点平仓，否则在日线另外一个确认点平仓
    df30min['signal'] = df30min['确认点_大']
    # 调第一个点，第一个点不要，因为第一段DEA不完整
    largeIndex = df30min[df30min['确认点_大'] != 0].index[0]
    df30min.at[largeIndex,'signal'] = 0
    # 调多空，用signal里面的函数来条
    signal.signal2position_single(df30min, mode='long')
    df30min['signal'] = df30min['poChg']
    df30min.drop('poChg',inplace=True,axis=1)
    for largeIndex in df30min[df30min['signal'] != 0].index:
        # largeIndex = df30min[df30min['signal'] != 0].index[0]
        if df30min.at[largeIndex, 'signal'] == 1:
            nextLargeIndex \
                = df30min[df30min['signal'] != 0].index[
                df30min[df30min['signal'] != 0].index.get_loc(largeIndex) + 1
                                                         ]
            df30min.at[largeIndex, 'signal'] = 1

            upWave = df30min.loc[largeIndex:nextLargeIndex,]
            highestSmallTop = 0
            for smallIndex in upWave.loc[upWave['波段拐点'] != 0].index:
                # smallIndex = upWave.loc[upWave['波段拐点'] != 0].index[0]
                if upWave.at[smallIndex, '波段拐点'] == -1:
                    if upWave.at[smallIndex,'CLOSE'] > highestSmallTop:
                        highestSmallTop = upWave.at[smallIndex,'CLOSE']
                    # 不创新高了
                    else:
                        df30min.at[upWave[smallIndex:][upWave.loc[smallIndex:, '确认点'] == -1].index[0], 'signal'] = -1
                        df30min.at[nextLargeIndex, 'signal'] = 0
                        break
                else:
                    pass
        else:
            pass

    # 回测这个策略
    signal.signal2position_single(df30min, mode='long')
    timing.single_proportion_backtest(df30min)
    detailDf = timing.single_trade_details(df30min)
    evalSer = timing.overall_eval(df30min, detailDf)
    yearDf = timing.year_month_analysis(detailDf)

    equityFig = fig.Equity_Fig()
    sub.draw_series_on_axes(equityFig.axDesc, evalSer, color='white')
    sub.draw_month_analysis(equityFig.axYear, yearDf)
    equityCurve = sub.Equity_Curve(equityFig.axEquity, df30min[['equityCurve']], show_x_tick=False, period='30min')
    equityFig.show()

    # df30min.to_csv('D:\\tempdata\\df30min.csv',encoding='gbk')

    curFig = fig.Fig(subplotdict={'axClose':[0.05,0.43,0.9,0.55],
                                  'axTechShort':[0.05,0.2,0.9,0.18],
                                  'axTechLong':[0.05,0.00,0.9,0.18]},
                     if_top_need=(True,True,True))
    close = sub.Lines(curFig.axClose, curFig.axCloseTop, df30min[['CLOSE']],
                      length=200,
                      lenuplt=1500, step=50,
                      majorGrid='week', majorFormat='%Y-%m-%d',
                      minorGrid='day',
                      dtFormat='%Y-%m-%d %H:%M:%S')
    techShort = sub.Lines(curFig.axTechShort,curFig.axTechShortTop,df30min[['DEA']],baseLine=0,
                          length=200,
                          lenuplt=1500, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d',
                          minorGrid='day',
                          dtFormat='%Y-%m-%d %H:%M:%S')
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df30min[['DEA_大']],baseLine=0,
                         length=200,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')

    close.attach_wave_sign(df30min[['CLOSE','波段拐点']],)
    close.attach_wave_sign(df30min[['CLOSE','波段拐点_大']],color='cyan')
    close.attach_buysell_sign(df30min[['CLOSE', '确认点_大']], mode='longshort')
    close.attach_buysell_sign(df30min[['CLOSE', 'signal']], mode='long')

    close.add_synchron(techShort,techLong)
    techShort.add_synchron(close,techLong)
    techLong.add_synchron(close,techShort)

    curFig.show()

    # 成功率懒得统计了，如果卖点比正常的高，则成功，成功率很高，75%，但是总体胜率很低。而且识别不了趋势