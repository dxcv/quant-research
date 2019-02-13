
import pandas as pd
import numpy as np
import talib as ta
import datetime as dt


def find_suspect_point(group):
    '''
    分完段之后水上取最大值，水下取最小值
    :param group:
    :return:
    '''
    if group.iloc[0, group.columns.get_loc('updown')] == True:
        rst = pd.DataFrame(group.xs(group['CLOSE'].idxmax())).T
        rst.index.names = ['DATETIME', 'CODE']
    else:
        rst = pd.DataFrame(group.xs(group['CLOSE'].idxmin())).T
        rst.index.names = ['DATETIME', 'CODE']
    return rst


def _find_minMax(group):
    '''
    找到段内的最大值，最小值，以及其中的位置。
    :param group:
    :return:
    '''
    return pd.DataFrame([group.index[0][0],
                         group.iat[0, group.columns.get_loc('sign')], group.iat[0, group.columns.get_loc('CLOSE')],
                         group['CLOSE'].max(), group['CLOSE'].idxmax()[0],
                         group['CLOSE'].min(), group['CLOSE'].idxmin()[0]],
                        index=['point',
                               'sign', 'CLOSE',
                               'maxValue', 'maxPoint',
                               'minValue', 'minPoint']).T


def del_suspect_point(dfNeed):
    '''
    可疑点删除算法，跟均sign来分段，dfNeed生成新的列waveGroupKey是本次删除前的波段。
    新的列signOld保留上次的sign来备查。
    :param dfNeed:
    :return:
    '''
    # 按照可疑点分为不同的段，每段都是含头不含尾
    # 总的的分段中第0段是开头不完整的DEA的分出的段
    dfNeed['waveGroupKey'] = None
    dfNeed.loc[dfNeed['sign'] != 0, 'waveGroupKey'] = range(1, len(dfNeed.loc[dfNeed['sign'] != 0, 'waveGroupKey']) + 1)
    dfNeed['waveGroupKey'].fillna(method='ffill', inplace=True)
    dfNeed['waveGroupKey'].fillna(0, inplace=True)

    dfMinMax = dfNeed.groupby('waveGroupKey').apply(_find_minMax, )
    dfMinMax.reset_index(level=1, drop=True, inplace=True)
    dfNeed['signOld'] = dfNeed['sign']

    delPointDict = {}

    waveI = 1
    while waveI <= len(dfMinMax.index) - 2:
        # if dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')] == dt.datetime(2018,7,24,13,20,0):
        #     print('found')
        # else:
        #     pass
        # 该点是顶点，下降波段
        if dfMinMax.iat[waveI, dfMinMax.columns.get_loc('sign')] == -1:
            # 特殊情况一：顶点比下一个底点还要低
            if dfMinMax.iat[waveI, dfMinMax.columns.get_loc('CLOSE')] \
                    < dfMinMax.iat[waveI+1, dfMinMax.columns.get_loc('CLOSE')]:
                print('特殊一：{date}'.format(date=dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]))
                if dfMinMax.iat[waveI, dfMinMax.columns.get_loc('sign')] \
                        * dfMinMax.iat[waveI+1, dfMinMax.columns.get_loc('sign')] > 0:
                    dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0
                    dfMinMax.iat[waveI - 1, dfMinMax.columns.get_loc('maxValue')] \
                        = dfMinMax.iloc[waveI - 1:waveI+1, dfMinMax.columns.get_loc('maxValue')].max()
                    dfMinMax.iat[waveI - 1, dfMinMax.columns.get_loc('minValue')] \
                        = dfMinMax.iloc[waveI - 1:waveI+1, dfMinMax.columns.get_loc('minValue')].min()
                    dfMinMax.drop(dfMinMax.iloc[waveI].name,inplace=True)
                    waveI -= 1
                else:
                    if waveI == 1:
                        dfNeed.loc[
                            (dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0
                        dfMinMax.drop(dfMinMax.iloc[waveI].name, inplace=True)
                    else:
                        # 删除这两个点
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI].name, dfMinMax.iloc[waveI+1].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+1].name), 'sign'] = 0
                        dfMinMax.iat[waveI - 1, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI - 1:waveI + 2, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI - 1, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI - 1:waveI + 2, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI].name, dfMinMax.iloc[waveI+1].name],inplace=True)
                        waveI -= 1
            # 异常波段一：下降的顶点不是最大
            elif dfMinMax.iat[waveI, dfMinMax.columns.get_loc('CLOSE')] \
                    < dfMinMax.iat[waveI, dfMinMax.columns.get_loc('maxValue')]:
                print('异常一：{date}'.format(date=dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]))
                # g是第一个点
                if waveI == 1:
                    delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] = (dfMinMax.iloc[waveI].name,)
                    # 在dfNeed和dfMinMax中删去g点
                    dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除g即可
                    dfMinMax.drop(dfMinMax.iloc[waveI].name, inplace=True)
                # g是倒数第二个点
                elif waveI == len(dfMinMax.index)-2:
                    d0 = dfMinMax.iat[waveI-1, dfMinMax.columns.get_loc('CLOSE')]
                    d = dfMinMax.iat[waveI+1, dfMinMax.columns.get_loc('CLOSE')]
                    if d0 < d:  # 去掉较高的点
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI].name, dfMinMax.iloc[waveI+1].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除g
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+1].name), 'sign'] = 0  # 删除d
                        dfMinMax.drop([dfMinMax.iloc[waveI].name, dfMinMax.iloc[waveI+1].name],inplace=True)
                    else:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI-1].name,dfMinMax.iloc[waveI].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除g
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI-1].name), 'sign'] = 0  # 删除d0
                        dfMinMax.iat[waveI-2, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI-2:waveI+1, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI-2, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI-2:waveI+1, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI-1].name,dfMinMax.iloc[waveI].name],inplace=True)
                        waveI -= 2
                # g是中间的点
                else:
                    d0 = dfMinMax.iat[waveI-1, dfMinMax.columns.get_loc('CLOSE')]
                    g = dfMinMax.iat[waveI, dfMinMax.columns.get_loc('CLOSE')]
                    d = dfMinMax.iat[waveI+1, dfMinMax.columns.get_loc('CLOSE')]
                    g1 = dfMinMax.iat[waveI+2, dfMinMax.columns.get_loc('CLOSE')]
                    if g < g1 and d0 < d:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI].name, dfMinMax.iloc[waveI+1].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除g
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+1].name), 'sign'] = 0  # 删除d
                        dfMinMax.iat[waveI - 1, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI - 1:waveI + 2, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI - 1, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI - 1:waveI + 2, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI].name, dfMinMax.iloc[waveI+1].name],inplace=True)
                        waveI -= 1
                    else:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI-1].name, dfMinMax.iloc[waveI].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除g
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI-1].name), 'sign'] = 0  # 删除d0
                        dfMinMax.iat[waveI - 2, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI - 2:waveI+1, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI - 2, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI - 2:waveI+1, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI-1].name, dfMinMax.iloc[waveI].name], inplace=True)
                        waveI -= 2
            # 异常波段二，下跌的底点不是最低
            elif dfMinMax.iat[waveI+1, dfMinMax.columns.get_loc('CLOSE')] \
                    > dfMinMax.iat[waveI, dfMinMax.columns.get_loc('minValue')]:
                print('异常二：{date}'.format(date=dfMinMax.iat[waveI,dfMinMax.columns.get_loc('point')]))
                # g是倒数第二个点，d是最后一个点
                if waveI == len(dfMinMax.index)-2:
                    delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                        = (dfMinMax.iloc[waveI + 1].name,)
                    dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+1].name), 'sign'] = 0  # 删除d
                    dfMinMax.drop(dfMinMax.iloc[waveI + 1].name, inplace=True)
                # g是第一个点，保留较高的点
                elif waveI == 1:
                    g = dfMinMax.iat[waveI, dfMinMax.columns.get_loc('CLOSE')]
                    g1 = dfMinMax.iat[waveI+2, dfMinMax.columns.get_loc('CLOSE')]
                    if g > g1:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI + 1].name,dfMinMax.iloc[waveI + 2].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+1].name), 'sign'] = 0  # 删除d
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+2].name), 'sign'] = 0  # 删除g1
                        dfMinMax.iat[waveI, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI:waveI + 3, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI:waveI + 3, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI + 1].name,dfMinMax.iloc[waveI + 2].name], inplace=True)
                    else:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI].name,dfMinMax.iloc[waveI + 1].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+1].name), 'sign'] = 0  # 删除d
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除g
                        dfMinMax.drop([dfMinMax.iloc[waveI].name, dfMinMax.iloc[waveI + 1].name], inplace=True)
                # g是中间的点
                else:
                    d0 = dfMinMax.iat[waveI-1, dfMinMax.columns.get_loc('CLOSE')]
                    g = dfMinMax.iat[waveI, dfMinMax.columns.get_loc('CLOSE')]
                    d = dfMinMax.iat[waveI+1, dfMinMax.columns.get_loc('CLOSE')]
                    g1 = dfMinMax.iat[waveI+2, dfMinMax.columns.get_loc('CLOSE')]
                    if d0 < d and g < g1:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (0,1)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除g
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+1].name), 'sign'] = 0  # 删除d
                        dfMinMax.iat[waveI - 1, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI-1:waveI + 2, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI - 1, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI-1:waveI + 2, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI].name,dfMinMax.iloc[waveI + 1].name], inplace=True)
                        waveI -= 1
                    else:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI+1].name,dfMinMax.iloc[waveI + 2].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] ==dfMinMax.iloc[waveI+1].name), 'sign'] = 0  # 删除d
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+2].name), 'sign'] = 0  # 删除g1
                        dfMinMax.iat[waveI, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI:waveI+3, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI:waveI+3, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI+1].name, dfMinMax.iloc[waveI + 2].name], inplace=True)
            # 不是异常波段
            else:
                waveI += 1

        # 该点是底点，上升波段
        elif dfMinMax.iat[waveI, dfMinMax.columns.get_loc('sign')] == 1:
            # 特殊情况二：底点比下一个顶点还要高
            if dfMinMax.iat[waveI, dfMinMax.columns.get_loc('CLOSE')] \
                    > dfMinMax.iat[waveI+1, dfMinMax.columns.get_loc('CLOSE')]:
                print('特殊二：{date}'.format(date=dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]))
                if dfMinMax.iat[waveI, dfMinMax.columns.get_loc('sign')] \
                        * dfMinMax.iat[waveI+1, dfMinMax.columns.get_loc('sign')] > 0:
                    dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0
                    dfMinMax.iat[waveI - 1, dfMinMax.columns.get_loc('maxValue')] \
                        = dfMinMax.iloc[waveI - 1:waveI+1, dfMinMax.columns.get_loc('maxValue')].max()
                    dfMinMax.iat[waveI - 1, dfMinMax.columns.get_loc('minValue')] \
                        = dfMinMax.iloc[waveI - 1:waveI+1, dfMinMax.columns.get_loc('minValue')].min()
                    dfMinMax.drop(dfMinMax.iloc[waveI].name,inplace=True)
                    waveI -= 1
                else:
                    if waveI == 1:
                        dfNeed.loc[
                            (dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0
                        dfMinMax.drop(dfMinMax.iloc[waveI].name, inplace=True)
                    else:
                        # 删除这两个点
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI].name, dfMinMax.iloc[waveI+1].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+1].name), 'sign'] = 0
                        dfMinMax.iat[waveI - 1, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI - 1:waveI + 2, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI - 1, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI - 1:waveI + 2, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI].name, dfMinMax.iloc[waveI+1].name],inplace=True)
                        waveI -= 1
            # 异常波段三：上升波段的底点不是最小
            elif dfMinMax.iat[waveI, dfMinMax.columns.get_loc('CLOSE')] \
                    > dfMinMax.iat[waveI, dfMinMax.columns.get_loc('minValue')]:
                print('异常三：{date}'.format(date=dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]))
                # 第一个点
                if waveI == 1:
                    delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] = (dfMinMax.iloc[waveI].name,)
                    dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除d即可
                    dfMinMax.drop(dfMinMax.iloc[waveI].name, inplace=True)
                # d是倒数第二个点
                elif waveI == len(dfMinMax.index)-2:
                    g0 = dfMinMax.iat[waveI-1, dfMinMax.columns.get_loc('CLOSE')]
                    g = dfMinMax.iat[waveI+1, dfMinMax.columns.get_loc('CLOSE')]
                    if g0 < g: # 去掉较低的点
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI-1].name,dfMinMax.iloc[waveI].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除d
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI-1].name), 'sign'] = 0  #删除g0
                        dfMinMax.iat[waveI-2, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI-2:waveI+1, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI-2, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI-2:waveI+1, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI-1].name,dfMinMax.iloc[waveI].name],inplace=True)
                        waveI -= 2
                    else:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI].name,dfMinMax.iloc[waveI+1].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除d
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+1].name), 'sign'] = 0  # 删除g
                        dfMinMax.drop([dfMinMax.iloc[waveI].name, dfMinMax.iloc[waveI+1].name], inplace=True)
                # d是中间的点
                else:
                    g0 = dfMinMax.iat[waveI-1, dfMinMax.columns.get_loc('CLOSE')]
                    d = dfMinMax.iat[waveI, dfMinMax.columns.get_loc('CLOSE')]
                    g = dfMinMax.iat[waveI+1, dfMinMax.columns.get_loc('CLOSE')]
                    d1 = dfMinMax.iat[waveI+2, dfMinMax.columns.get_loc('CLOSE')]
                    if g0 > g and d > d1:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI].name,dfMinMax.iloc[waveI+1].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除d
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+1].name), 'sign'] = 0  # 删除g
                        dfMinMax.iat[waveI-1, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI-1:waveI+2, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI-1, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI-1:waveI+2, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI].name,dfMinMax.iloc[waveI+1].name],inplace=True)
                        waveI -= 1
                    else:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI-1].name,dfMinMax.iloc[waveI].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除d
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI-1].name), 'sign'] = 0  # 删除g0
                        dfMinMax.iat[waveI-2, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI-2:waveI+1, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI-2, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI-2:waveI+1, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI-1].name,dfMinMax.iloc[waveI].name],inplace=True)
                        waveI -= 2
            # 异常波段四，上升的顶点不是最高
            elif dfMinMax.iat[waveI+1,dfMinMax.columns.get_loc('CLOSE')] \
                    < dfMinMax.iat[waveI, dfMinMax.columns.get_loc('maxValue')]:
                print('异常四：{date}'.format(date=dfMinMax.iat[waveI,dfMinMax.columns.get_loc('point')]))
                # d是倒数第二个点，g是最后一个点，直接删去g
                if waveI == len(dfMinMax.index)-2:
                    delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                        = (dfMinMax.iloc[waveI + 1].name)
                    dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+1].name), 'sign'] = 0  # 删除g
                    dfMinMax.drop(dfMinMax.iloc[waveI+1].name, inplace=True)
                # d是第一个点，保留较低的点
                elif waveI == 1:
                    d = dfMinMax.iat[waveI, dfMinMax.columns.get_loc('CLOSE')]
                    d1 = dfMinMax.iat[waveI+2, dfMinMax.columns.get_loc('CLOSE')]
                    if d < d1:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI + 1].name,dfMinMax.iloc[waveI + 2].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI + 1].name), 'sign'] = 0  # 删除g
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI + 2].name), 'sign'] = 0  # 删除d1
                        dfMinMax.iat[waveI, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI:waveI + 3, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI:waveI + 3, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI + 1].name,dfMinMax.iloc[waveI + 2].name], inplace=True)
                    else:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI].name,dfMinMax.iloc[waveI + 1].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI+1].name), 'sign'] = 0  # 删除g
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除d
                        dfMinMax.drop([dfMinMax.iloc[waveI].name, dfMinMax.iloc[waveI + 1].name], inplace=True)
                # d是中间的点
                else:
                    g0 = dfMinMax.iat[waveI-1, dfMinMax.columns.get_loc('CLOSE')]
                    d = dfMinMax.iat[waveI, dfMinMax.columns.get_loc('CLOSE')]
                    g = dfMinMax.iat[waveI+1, dfMinMax.columns.get_loc('CLOSE')]
                    d1 = dfMinMax.iat[waveI+2, dfMinMax.columns.get_loc('CLOSE')]
                    if g0 > g and d > d1:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI].name,dfMinMax.iloc[waveI + 1].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI].name), 'sign'] = 0  # 删除d
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI + 1].name), 'sign'] = 0  # 删除g
                        dfMinMax.iat[waveI-1, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI-1:waveI+2, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI-1, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI-1:waveI+2, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI].name, dfMinMax.iloc[waveI + 1].name], inplace=True)
                        waveI -= 1
                    else:
                        delPointDict[dfMinMax.iat[waveI, dfMinMax.columns.get_loc('point')]] \
                            = (dfMinMax.iloc[waveI+1].name,dfMinMax.iloc[waveI + 2].name)
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI + 1].name), 'sign'] = 0  # 删除g
                        dfNeed.loc[(dfNeed['sign'] != 0) & (dfNeed['waveGroupKey'] == dfMinMax.iloc[waveI + 2].name), 'sign'] = 0  # 删除d1
                        dfMinMax.iat[waveI, dfMinMax.columns.get_loc('maxValue')] \
                            = dfMinMax.iloc[waveI:waveI+3, dfMinMax.columns.get_loc('maxValue')].max()
                        dfMinMax.iat[waveI, dfMinMax.columns.get_loc('minValue')] \
                            = dfMinMax.iloc[waveI:waveI+3, dfMinMax.columns.get_loc('minValue')].min()
                        dfMinMax.drop([dfMinMax.iloc[waveI+1].name, dfMinMax.iloc[waveI + 2].name], inplace=True)
            # 不是异常波段
            else:
                waveI += 1

        # 该点是第〇点，
        else:
            waveI += 1
    return delPointDict


def cal_macd(df):
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2


def gen_suspect_point(df):
    '''
    这个函数是根据DEA的正负，上求最大值，水下求最小值
    分段的最高最低符号包含开头和最后不完整的DEA段
    :param df:
    :return:
    '''
    dfNeed = df.copy()
    dfNeed['chgPoint'] = (dfNeed['DEA'] * dfNeed['DEA'].shift(-1)) < 0
    dfNeed['groupKey'] = np.nan
    dfNeed.loc[dfNeed['chgPoint'], 'groupKey'] = range(len(dfNeed['groupKey'][dfNeed['chgPoint']]))
    dfNeed['groupKey'].fillna(method='bfill', inplace=True)
    dfNeed.dropna(inplace=True) # 这句将后面的去掉了，因为没有确定下来
    dfNeed['updown'] = dfNeed['DEA'].apply(lambda x:True if x >= 0 else False) # DEA水上是正，DEA水下是负
    # 第一次的可疑点序列
    dfSuspect = dfNeed.groupby('groupKey', as_index=False).apply(find_suspect_point)
    dfSuspect.reset_index(level=0,drop=True,inplace=True)
    # 显示这次的可疑点
    dfNeed['sign'] = dfSuspect['updown']
    dfNeed['sign'] = dfNeed['sign'].map({True:-1,False:1,np.nan:0})
    dfNeed.drop(['groupKey','updown','chgPoint'],axis=1,inplace=True)
    del dfSuspect
    return dfNeed


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
    dfStat['收益率'] = dfStat['收益率'] * dfStat['sign'] * -1
    dfStat['sign'] = dfStat['sign'].map({-1: '上涨波段', 1: '下降波段'})
    dfStat.dropna(inplace=True)
    rst = dfStat.groupby('sign')['收益率'].apply(_gen_desc)
    rst.reset_index(level=1,drop=True,inplace=True)
    return rst


########################################################################################################################
# 这是显示删除过程000001.SH，日线
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df1day = fetch.index_one('000001.SH')
    cal_macd(df1day)
    df1day = df1day[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_suspect_point(df1day)
    # 删除之前
    szFig = fig.Fig()
    close1day = sub.Lines(szFig.ax1, szFig.ax1Top,df1dayAfter[['CLOSE']],
                          lenuplt=1000,step=50,majorGrid='year',majorFormat='%Y',minorGrid='month')
    tech1day = sub.Lines(szFig.ax2, szFig.ax2Top,df1dayAfter[['DEA']],baseLine=0,
                         lenuplt=1000, step=50, majorGrid='year', majorFormat='%Y', minorGrid='month')
    close1day.attach_wave_sign(df1dayAfter[['CLOSE','sign']])
    close1day.add_synchron(tech1day)
    tech1day.add_synchron(close1day)
    szFig.show()

    delPointDict = del_suspect_point(df1dayAfter)
    close1day.attach_wave_sign(df1dayAfter[['CLOSE', 'sign']], color='green')

    del szFig, df1dayAfter,df1day,close1day,tech1day,delPointDict


# 这是显示删除过程000001.SH，30min @ todo 出现致命缺陷，最大值出现在DEA小于0的时候，出现在快速拉升的时候，直接废了上证指数的相关策略
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df30min = fetch.n_min('000001.SH', n=30)
    cal_macd(df30min)
    df30min = df30min[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_suspect_point(df30min)
    # 删除之前
    szFig = fig.Fig()
    close30min = sub.Lines(szFig.ax1, szFig.ax1Top,df30minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500,step=50,
                           majorGrid='week',majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech30min = sub.Lines(szFig.ax2, szFig.ax2Top,df30minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close30min.attach_wave_sign(df30minAfter[['CLOSE','sign']])
    close30min.add_synchron(tech30min)
    tech30min.add_synchron(close30min)
    szFig.show()

    delPointDict = del_suspect_point(df30minAfter)
    close30min.attach_wave_sign(df30minAfter[['CLOSE', 'sign']], color='green')

    del szFig, df30minAfter,df30min,close30min,tech30min,delPointDict


# 这是显示删除过程000001.SH，5min
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df5min = fetch.n_min('000001.SH', n=5)
    cal_macd(df5min)
    df5min = df5min[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_suspect_point(df5min)
    # 删除之前
    szFig = fig.Fig()
    close5min = sub.Lines(szFig.ax1, szFig.ax1Top,df5minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500,step=50,
                           majorGrid='week',majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech5min = sub.Lines(szFig.ax2, szFig.ax2Top,df5minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close5min.attach_wave_sign(df5minAfter[['CLOSE','sign']])
    close5min.add_synchron(tech5min)
    tech5min.add_synchron(close5min)
    szFig.show()

    delPointDict = del_suspect_point(df5minAfter)
    close5min.attach_wave_sign(df5minAfter[['CLOSE', 'sign']], color='green')
    del szFig, df5minAfter,df5min,close5min,tech5min,delPointDict


# 这是显示删除过程399006.SZ，日线
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df1day = fetch.index_one('399006.SZ')
    cal_macd(df1day)
    df1day = df1day[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_suspect_point(df1day)
    # 删除之前
    szFig = fig.Fig()
    close1day = sub.Lines(szFig.ax1, szFig.ax1Top,df1dayAfter[['CLOSE']],
                          lenuplt=1000,step=50,majorGrid='year',majorFormat='%Y',minorGrid='month')
    tech1day = sub.Lines(szFig.ax2, szFig.ax2Top,df1dayAfter[['DEA']],baseLine=0,
                         lenuplt=1000, step=50, majorGrid='year', majorFormat='%Y', minorGrid='month')
    close1day.attach_wave_sign(df1dayAfter[['CLOSE','sign']])
    close1day.add_synchron(tech1day)
    tech1day.add_synchron(close1day)
    szFig.show()

    delPointDict = del_suspect_point(df1dayAfter)
    close1day.attach_wave_sign(df1dayAfter[['CLOSE', 'sign']], color='green')

    del szFig, df1dayAfter,df1day,close1day,tech1day,delPointDict


# 这是显示删除过程399006.SZ，30min
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df30min = fetch.n_min('399006.SZ', n=30)
    cal_macd(df30min)
    df30min = df30min[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_suspect_point(df30min)
    # 删除之前
    szFig = fig.Fig()
    close30min = sub.Lines(szFig.ax1, szFig.ax1Top,df30minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500,step=50,
                           majorGrid='week',majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech30min = sub.Lines(szFig.ax2, szFig.ax2Top,df30minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close30min.attach_wave_sign(df30minAfter[['CLOSE','sign']])
    close30min.add_synchron(tech30min)
    tech30min.add_synchron(close30min)
    szFig.show()

    delPointDict = del_suspect_point(df30minAfter)
    close30min.attach_wave_sign(df30minAfter[['CLOSE', 'sign']], color='green')

    del szFig, df30minAfter,df30min,close30min,tech30min,delPointDict


# 这是显示删除过程399006.SZ，5min
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df5min = fetch.n_min('399006.SZ', n=5)
    cal_macd(df5min)
    df5min = df5min[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_suspect_point(df5min)
    # 删除之前
    szFig = fig.Fig()
    close5min = sub.Lines(szFig.ax1, szFig.ax1Top,df5minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500,step=50,
                           majorGrid='week',majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech5min = sub.Lines(szFig.ax2, szFig.ax2Top,df5minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close5min.attach_wave_sign(df5minAfter[['CLOSE','sign']])
    close5min.add_synchron(tech5min)
    tech5min.add_synchron(close5min)
    szFig.show()

    delPointDict = del_suspect_point(df5minAfter)
    close5min.attach_wave_sign(df5minAfter[['CLOSE', 'sign']], color='green')
    del szFig, df5minAfter,df5min,close5min,tech5min,delPointDict


# 这是显示删除过程000016.SH，日线
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df1day = fetch.index_one('000016.SH')
    cal_macd(df1day)
    df1day = df1day[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_suspect_point(df1day)
    # 删除之前
    szFig = fig.Fig()
    close1day = sub.Lines(szFig.ax1, szFig.ax1Top,df1dayAfter[['CLOSE']],
                          lenuplt=1000,step=50,majorGrid='year',majorFormat='%Y',minorGrid='month')
    tech1day = sub.Lines(szFig.ax2, szFig.ax2Top,df1dayAfter[['DEA']],baseLine=0,
                         lenuplt=1000, step=50, majorGrid='year', majorFormat='%Y', minorGrid='month')
    close1day.attach_wave_sign(df1dayAfter[['CLOSE','sign']])
    close1day.add_synchron(tech1day)
    tech1day.add_synchron(close1day)
    szFig.show()

    delPointDict = del_suspect_point(df1dayAfter)
    close1day.attach_wave_sign(df1dayAfter[['CLOSE', 'sign']], color='green')

    del szFig, df1dayAfter,df1day,close1day,tech1day,delPointDict


# 这是显示删除过程000016.SH，30min
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df30min = fetch.n_min('000016.SH', n=30)
    cal_macd(df30min)
    df30min = df30min[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_suspect_point(df30min)
    # 删除之前
    szFig = fig.Fig()
    close30min = sub.Lines(szFig.ax1, szFig.ax1Top,df30minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500,step=50,
                           majorGrid='week',majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech30min = sub.Lines(szFig.ax2, szFig.ax2Top,df30minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close30min.attach_wave_sign(df30minAfter[['CLOSE','sign']])
    close30min.add_synchron(tech30min)
    tech30min.add_synchron(close30min)
    szFig.show()

    delPointDict = del_suspect_point(df30minAfter)
    close30min.attach_wave_sign(df30minAfter[['CLOSE', 'sign']], color='green')

    del szFig, df30minAfter,df30min,close30min,tech30min,delPointDict


# 这是显示删除过程000016.SH，5min
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df5min = fetch.n_min('000016.SH', n=5)
    cal_macd(df5min)
    df5min = df5min[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_suspect_point(df5min)
    # 删除之前
    szFig = fig.Fig()
    close5min = sub.Lines(szFig.ax1, szFig.ax1Top,df5minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500,step=50,
                           majorGrid='week',majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech5min = sub.Lines(szFig.ax2, szFig.ax2Top,df5minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close5min.attach_wave_sign(df5minAfter[['CLOSE','sign']])
    close5min.add_synchron(tech5min)
    tech5min.add_synchron(close5min)
    szFig.show()

    delPointDict = del_suspect_point(df5minAfter)
    close5min.attach_wave_sign(df5minAfter[['CLOSE', 'sign']], color='green')
    del szFig, df5minAfter,df5min,close5min,tech5min,delPointDict


# 这是显示删除过程000300.SH，日线
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df1day = fetch.index_one('000300.SH')
    cal_macd(df1day)
    df1day = df1day[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_suspect_point(df1day)
    # 删除之前
    szFig = fig.Fig()
    close1day = sub.Lines(szFig.ax1, szFig.ax1Top,df1dayAfter[['CLOSE']],
                          lenuplt=1000,step=50,majorGrid='year',majorFormat='%Y',minorGrid='month')
    tech1day = sub.Lines(szFig.ax2, szFig.ax2Top,df1dayAfter[['DEA']],baseLine=0,
                         lenuplt=1000, step=50, majorGrid='year', majorFormat='%Y', minorGrid='month')
    close1day.attach_wave_sign(df1dayAfter[['CLOSE','sign']])
    close1day.add_synchron(tech1day)
    tech1day.add_synchron(close1day)
    szFig.show()

    delPointDict = del_suspect_point(df1dayAfter)
    close1day.attach_wave_sign(df1dayAfter[['CLOSE', 'sign']], color='green')

    del szFig, df1dayAfter,df1day,close1day,tech1day,delPointDict


# 这是显示删除过程000300.SH，30min
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df30min = fetch.n_min('000300.SH', n=30)
    cal_macd(df30min)
    df30min = df30min[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_suspect_point(df30min)
    # 删除之前
    szFig = fig.Fig()
    close30min = sub.Lines(szFig.ax1, szFig.ax1Top,df30minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500,step=50,
                           majorGrid='week',majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech30min = sub.Lines(szFig.ax2, szFig.ax2Top,df30minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close30min.attach_wave_sign(df30minAfter[['CLOSE','sign']])
    close30min.add_synchron(tech30min)
    tech30min.add_synchron(close30min)
    szFig.show()

    delPointDict = del_suspect_point(df30minAfter)
    close30min.attach_wave_sign(df30minAfter[['CLOSE', 'sign']], color='green')

    del szFig, df30minAfter,df30min,close30min,tech30min,delPointDict


# 这是显示删除过程000300.SH，5min
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df5min = fetch.n_min('000300.SH', n=5)
    cal_macd(df5min)
    df5min = df5min[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_suspect_point(df5min)
    # 删除之前
    szFig = fig.Fig()
    close5min = sub.Lines(szFig.ax1, szFig.ax1Top,df5minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500,step=50,
                           majorGrid='week',majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech5min = sub.Lines(szFig.ax2, szFig.ax2Top,df5minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close5min.attach_wave_sign(df5minAfter[['CLOSE','sign']])
    close5min.add_synchron(tech5min)
    tech5min.add_synchron(close5min)
    szFig.show()

    delPointDict = del_suspect_point(df5minAfter)
    close5min.attach_wave_sign(df5minAfter[['CLOSE', 'sign']], color='green')
    del szFig, df5minAfter,df5min,close5min,tech5min,delPointDict


# 这是显示删除过程000905.SH，日线
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df1day = fetch.index_one('000905.SH')
    cal_macd(df1day)
    df1day = df1day[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_suspect_point(df1day)
    # 删除之前
    szFig = fig.Fig()
    close1day = sub.Lines(szFig.ax1, szFig.ax1Top,df1dayAfter[['CLOSE']],
                          lenuplt=1000,step=50,majorGrid='year',majorFormat='%Y',minorGrid='month')
    tech1day = sub.Lines(szFig.ax2, szFig.ax2Top,df1dayAfter[['DEA']],baseLine=0,
                         lenuplt=1000, step=50, majorGrid='year', majorFormat='%Y', minorGrid='month')
    close1day.attach_wave_sign(df1dayAfter[['CLOSE','sign']])
    close1day.add_synchron(tech1day)
    tech1day.add_synchron(close1day)
    szFig.show()

    delPointDict = del_suspect_point(df1dayAfter)
    close1day.attach_wave_sign(df1dayAfter[['CLOSE', 'sign']], color='green')

    del szFig, df1dayAfter,df1day,close1day,tech1day,delPointDict


# 这是显示删除过程000905.SH，30min
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df30min = fetch.n_min('000905.SH', n=30)
    cal_macd(df30min)
    df30min = df30min[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_suspect_point(df30min)
    # 删除之前
    szFig = fig.Fig()
    close30min = sub.Lines(szFig.ax1, szFig.ax1Top,df30minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500,step=50,
                           majorGrid='week',majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech30min = sub.Lines(szFig.ax2, szFig.ax2Top,df30minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close30min.attach_wave_sign(df30minAfter[['CLOSE','sign']])
    close30min.add_synchron(tech30min)
    tech30min.add_synchron(close30min)
    szFig.show()

    delPointDict = del_suspect_point(df30minAfter)
    close30min.attach_wave_sign(df30minAfter[['CLOSE', 'sign']], color='green')

    del szFig, df30minAfter,df30min,close30min,tech30min,delPointDict


# 这是显示删除过程000905.SH，5min
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    df5min = fetch.n_min('000905.SH', n=5)
    cal_macd(df5min)
    df5min = df5min[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df5minAfter = gen_suspect_point(df5min)
    # 删除之前
    szFig = fig.Fig()
    close5min = sub.Lines(szFig.ax1, szFig.ax1Top,df5minAfter[['CLOSE']],
                           length=150,
                           lenuplt=1500,step=50,
                           majorGrid='week',majorFormat='%Y-%m-%d',
                           minorGrid='day',
                           dtFormat='%Y-%m-%d %H:%M:%S')
    tech5min = sub.Lines(szFig.ax2, szFig.ax2Top,df5minAfter[['DEA']],baseLine=0,
                         length=150,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')
    close5min.attach_wave_sign(df5minAfter[['CLOSE','sign']])
    close5min.add_synchron(tech5min)
    tech5min.add_synchron(close5min)
    szFig.show()

    delPointDict = del_suspect_point(df5minAfter)
    close5min.attach_wave_sign(df5minAfter[['CLOSE', 'sign']], color='green')
    del szFig, df5minAfter,df5min,close5min,tech5min,delPointDict


########################################################################################################################
# 显示当前情况000001.SH，日线和30min的联动
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related,pandas_related

    # 1天的线
    df1day = fetch.index_one('000001.SH')
    cal_macd(df1day)
    df1day = df1day[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_suspect_point(df1day)
    delPointDict1day = del_suspect_point(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('000001.SH', n=30)
    cal_macd(df30min)
    df30min = df30min[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_suspect_point(df30min)
    delPointDict30min = del_suspect_point(df30minAfter)

    # 由于两个After都截断过，用最长的df30min
    df30min['sign'] = df30minAfter['sign']
    df30min['sign'].fillna(0,inplace=True)
    df30min['PCTCHANGE'] = df30min['CLOSE'] / df30min['OPEN'] # 画K线图需要这一列
    # 将day的归入30min
    pandas_related.merge_day2min(df1dayAfter[['sign']], df30min, 'sign_day')
    pandas_related.merge_day2min(df1day[['DEA']], df30min, 'DEA_day',fillna=None)

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
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df30min[['DEA_day']],baseLine=0,
                         endNum=-1, length=200,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')

    close.attach_wave_sign(df30min[['CLOSE','sign']])
    close.attach_wave_sign(df30min[['CLOSE','sign_day']],color='cyan')

    close.add_synchron(techShort, techLong)
    techShort.add_synchron(close, techLong)
    techLong.add_synchron(close, techShort)

    curFig.show()

    del close, curFig, delPointDict1day, delPointDict30min, df1day, df1dayAfter, df30min, df30minAfter, techLong, techShort


# 显示当前情况399006.SZ，日线和30min的联动
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related,pandas_related

    # 1天的线
    df1day = fetch.index_one('399006.SZ')
    cal_macd(df1day)
    df1day = df1day[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_suspect_point(df1day)
    delPointDict1day = del_suspect_point(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('399006.SZ', n=30)
    cal_macd(df30min)
    df30min = df30min[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_suspect_point(df30min)
    delPointDict30min = del_suspect_point(df30minAfter)

    # 由于两个After都截断过，用最长的df30min
    df30min['sign'] = df30minAfter['sign']
    df30min['sign'].fillna(0,inplace=True)
    df30min['PCTCHANGE'] = df30min['CLOSE'] / df30min['OPEN'] # 画K线图需要这一列
    # 将day的归入30min
    pandas_related.merge_day2min(df1dayAfter[['sign']], df30min, 'sign_day')
    pandas_related.merge_day2min(df1day[['DEA']], df30min, 'DEA_day',fillna=None)

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
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df30min[['DEA_day']],baseLine=0,
                         endNum=-1, length=200,
                         lenuplt=1500, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d',
                         minorGrid='day',
                         dtFormat='%Y-%m-%d %H:%M:%S')

    close.attach_wave_sign(df30min[['CLOSE','sign']])
    close.attach_wave_sign(df30min[['CLOSE','sign_day']],color='cyan')

    close.add_synchron(techShort, techLong)
    techShort.add_synchron(close, techLong)
    techLong.add_synchron(close, techShort)

    curFig.show()

    del close, curFig, delPointDict1day, delPointDict30min, df1day, df1dayAfter, df30min, df30minAfter, techLong, techShort


# 小级别段的合并，这个还没有做完，15年牛市这波行情与这个模型出现巨大矛盾
if __name__ == '__main__':
    from tools.data import fetch
    from tools.mplot import sub, fig
    from tools.tinytools import stock_related,pandas_related

    # 1天的线
    df1day = fetch.index_one('399006.SZ')
    cal_macd(df1day)
    df1day = df1day[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df1dayAfter = gen_suspect_point(df1day)
    delPointDict1day = del_suspect_point(df1dayAfter)

    # 30分钟的线
    df30min = fetch.n_min('399006.SZ', n=30)
    cal_macd(df30min)
    df30min = df30min[120:] # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    df30minAfter = gen_suspect_point(df30min)
    delPointDict30min = del_suspect_point(df30minAfter)

    # 由于两个After都截断过，用最长的df30min
    df30min['sign'] = df30minAfter['sign']
    df30min['sign'].fillna(0,inplace=True)
    df30min['PCTCHANGE'] = df30min['CLOSE'] / df30min['OPEN'] # 画K线图需要这一列
    # 将day的归入30min
    pandas_related.merge_day2min(df1dayAfter[['sign']], df30min, 'sign_day')
    pandas_related.merge_day2min(df1day[['DEA']], df30min, 'DEA_day',fillna=None)

    del df30minAfter


    def long_attach_2_short(dfShort, longsign, shortsign, longsign_after):
        dfShort['order'] = range(len(dfShort.index))
        dfShort[longsign_after] = 0
        for longIndex in dfShort[dfShort[longsign] != 0].index:
            dfShort['distance'] = 0
            if dfShort.at[longIndex, longsign] == 1:
                dfShort['distance'] = (
                            dfShort.loc[dfShort[shortsign] == 1, 'order'] - dfShort.at[longIndex, 'order']).abs()
                dfShort.at[dfShort['distance'].idxmin(), longsign_after] = 1
            elif dfShort.at[longIndex, longsign] == -1:
                dfShort['distance'] = (
                            dfShort.loc[dfShort[shortsign] == -1, 'order'] - dfShort.at[longIndex, 'order']).abs()
                dfShort.at[dfShort['distance'].idxmin(), longsign_after] = -1
        dfShort.drop(['order', 'distance'], axis=1, inplace=True)

    # long_attach_2_short(df30min, 'sign_day','sign','sign_day_revised')

    curFig = fig.Fig(subplotdict={'axClose':[0.05,0.43,0.9,0.55],
                                  'axTechShort':[0.05,0.2,0.9,0.18],
                                  'axTechLong':[0.05,0.00,0.9,0.18]},
                     if_top_need=(True,True,True))
    close = sub.Lines(curFig.axClose, curFig.axCloseTop, df30min[['CLOSE']],
                      length=200,lenuplt=1000,step=50,
                      majorGrid='week',majorFormat='%Y-%m-%d',majorGridStyle='solid',
                      minorGrid='day',minorGridStyle='dotted')
    techShort = sub.Lines(curFig.axTechShort,curFig.axTechShortTop,df30min[['DEA']],baseLine=0,
                          length=200, lenuplt=1000, step=50,
                          majorGrid='week', majorFormat='%Y-%m-%d', majorGridStyle='solid',
                          minorGrid='day',minorGridStyle='dotted'
                          )
    techLong = sub.Lines(curFig.axTechLong, curFig.axTechLongTop, df30min[['DEA_day']],baseLine=0,
                         length=200, lenuplt=1000, step=50,
                         majorGrid='week', majorFormat='%Y-%m-%d', majorGridStyle='solid',
                         minorGrid='day',minorGridStyle='dotted')

    close.attach_buysell_sign(df30min[['CLOSE','sign']],[1,-1])
    close.attach_buysell_sign(df30min[['CLOSE','sign_day']],[1,-1],color='cyan')

    # close.attach_buysell_sign(df30min[['CLOSE','sign_day_revised']],[1,-1],color='red')

    close.add_synchron(techShort,techLong)
    techShort.add_synchron(close,techLong)
    techLong.add_synchron(close,techShort)

    curFig.show()



