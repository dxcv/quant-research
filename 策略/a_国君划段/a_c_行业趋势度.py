
import pandas as pd
import numpy as np
from 策略.a_国君划段 import a_b_DEA分段新逻辑, a_a_DEA分段


def groupfunc_find_subindustry_buysell(group):
    '''
    这是个应用于分组的组内行数，功能是对每个分组计算DEA分段新逻辑的买卖确认点，同时删去行业名称和行业代码。
    同时确认买卖状态，买点后卖点前是买入状态，反之。删去前面没有状态的日期。
    :param group:
    :return:
    '''
    print('\n\n' + group.name)
    groupCopy = group.copy()

    a_a_DEA分段.cal_macd(groupCopy)
    groupCopy = groupCopy[120:]  # 从头计算的MACD和行情软件的不同，需要截去前面大概120条，后面的基本一致
    groupCopy = a_b_DEA分段新逻辑.gen_DEA_segment(groupCopy)
    a_b_DEA分段新逻辑.fenduan_new_logic(groupCopy)

    groupCopy.drop(['DIFF','MACD','groupKey','updown'],axis=1,inplace=True)
    groupCopy.loc[groupCopy['确认点'] == 0, '确认点'] = np.nan
    groupCopy.fillna(method='ffill', inplace=True)
    groupCopy.dropna(inplace=True)
    return groupCopy


def groupFunc_find_updown_ratio(group):
    return pd.DataFrame([len(group.loc[group['确认点'] == 1].index),len(group.loc[group['确认点'] == -1].index)],index=['up','down']).T


# 下面是日线级别的行业趋势度的计算
if __name__ == '__main__':
    from tools.data import fetch
    dfAllA = fetch.index_one('881001.WI')
    # 取中信二级行业来代替原报告中申万二级行业
    dfCiticslvl2 = fetch.index_industry_list('all', citicslevel=2)
    dfCiticslvl2.dropna(inplace=True)
    dfCiticslvl2.drop(['S_INFO_NAME', 'S_INFO_INDUSTRYCODE', 'level'], axis=1, inplace=True)

    # 对中信2级行业中每个行业计算DEA分段新逻辑的买卖确认点
    dfCiticslvl2 = dfCiticslvl2.groupby(level=1).apply(groupfunc_find_subindustry_buysell)
    dfCiticslvl2.reset_index(level=0,inplace=True,drop=True)
    dfCiticslvl2.sort_index(level=[0,1],inplace=True)

    # 由于数据原因，前面的日期处于状态内的行业数只有少数几个，后面有些行业处于状态数也比较小，一个简单的标准是取大于40的日期。
    _groupCountTemp = dfCiticslvl2.groupby(level=0)['CLOSE'].count()
    start = _groupCountTemp[_groupCountTemp > 40].index[0]
    end = _groupCountTemp[_groupCountTemp > 40].index[-1]
    dfCiticslvl2 = dfCiticslvl2.loc[start:end]
    del start,end, _groupCountTemp

    # 计算行业趋势度，就是报告中处于买状态的比例
    _calTemp = dfCiticslvl2.groupby(level=0).apply(groupFunc_find_updown_ratio).reset_index(level=1, drop=True)
    _calTemp['CODE'] = '881001.WI'
    _calTemp = _calTemp.reset_index().set_index(['DATETIME', 'CODE'])
    dfAllA['day_trend'] = (_calTemp['up'] - _calTemp['down']) / (_calTemp['up'] + _calTemp['down'])
    dfAllA.dropna(inplace=True)

    from tools.backtest import timing, signal
    # 原始信号
    signal.change_step(dfAllA, 'day_trend', 0.1, -0.1,longSignalName='buy', shortSignalName='sell')
    signal.long_short_signal(dfAllA, colNameTup=('buy', 'sell'))
    dfAllA.drop(['buy', 'sell'], axis=1, inplace=True)
    # 实际信号
    signal.signal2position_single(dfAllA, signalCol='signal', mode='long')
    # 回测
    induDayStatage = timing.Single_Proportion(dfAllA,originSignal='signal',labels=('induDay',),mode='long')


    # 下面这个是显示信号和行情对应的情况
    from tools.mplot import fig, sub
    induFig = fig.Fig()
    close = sub.Lines(induFig.ax1, induFig.ax1Top,dfAllA[['CLOSE']],
                      majorGrid='year',majorFormat='%Y',lenuplt=1000,length=100,step=100)
    tech = sub.Lines(induFig.ax2,induFig.ax2Top,dfAllA[['day_trend']],baseLine=0,
                     majorGrid='year', majorFormat='%Y',lenuplt=1000,length=100,step=100)

    close.attach_buysell_sign(dfAllA[['CLOSE','poChg']],mode='long')
    tech.attach_buysell_sign(dfAllA[['day_trend','poChg']],mode='long')

    close.add_synchron(tech)
    tech.add_synchron(close)

    induFig.show()


# 下面是周线级别行业趋势度计算和策略
if __name__ == '__main__':
    from tools.data import fetch
    from tools.tinytools import stock_related,pandas_related
    dfAllA = fetch.index_one('881001.WI')
    dfAllAWeek = pandas_related.day_downSample(dfAllA)

    # 取中信二级行业来代替原报告中申万二级行业
    dfCiticslvl2 = fetch.index_industry_list('all', citicslevel=2)
    dfCiticslvl2.dropna(inplace=True)
    dfCiticslvl2.drop(['S_INFO_NAME','S_INFO_INDUSTRYCODE','level'],axis=1,inplace=True)

    # 得到中信二级行业的周线
    dfCiticslvl2Week = dfCiticslvl2.groupby(level=1).apply(pandas_related.day_downSample)
    dfCiticslvl2Week.reset_index(level=0, drop=True, inplace=True)

    # 对中信2级行业中每个行业计算DEA分段新逻辑的买卖确认点
    dfCiticslvl2Week = dfCiticslvl2Week.groupby(level=1).apply(groupfunc_find_subindustry_buysell)
    dfCiticslvl2Week.reset_index(level=0,inplace=True,drop=True)
    dfCiticslvl2Week.sort_index(level=[0,1],inplace=True)

    # 由于数据原因，前面的日期处于状态内的行业数只有少数几个，后面有些行业处于状态数也比较小，一个简单的标准是取大于40的日期。
    _groupCountTemp = dfCiticslvl2Week.groupby(level=0)['CLOSE'].count()
    start = _groupCountTemp[_groupCountTemp > 30].index[0]
    end = _groupCountTemp[_groupCountTemp > 30].index[-1]
    dfCiticslvl2Week = dfCiticslvl2Week.loc[start:end]
    del start,end, _groupCountTemp

    # 计算行业趋势度，就是报告中处于买状态的比例
    _calTemp = dfCiticslvl2Week.groupby(level=0).apply(groupFunc_find_updown_ratio).reset_index(level=1, drop=True)
    _calTemp['CODE'] = '881001.WI'
    _calTemp = _calTemp.reset_index().set_index(['DATETIME', 'CODE'])
    dfAllAWeek['day_trend'] = (_calTemp['up'] - _calTemp['down']) / (_calTemp['up'] + _calTemp['down'])
    dfAllAWeek.dropna(inplace=True)
    del _calTemp

    from tools.backtest import timing, signal
    # 原始信号
    signal.change_step(dfAllAWeek, 'day_trend', 0.1, -0.1,longSignalName='buy', shortSignalName='sell')
    signal.long_short_signal(dfAllAWeek, colNameTup=('buy', 'sell'))
    dfAllAWeek.drop(['buy', 'sell'], axis=1, inplace=True)
    # 实际信号
    signal.signal2position_single(dfAllAWeek, signalCol='signal', mode='longshort')
    # 回测
    induWeekS = timing.Single_Proportion(dfAllAWeek,originSignal='signal',labels=('induWeek',),mode='longshort')


    # 下面这个是显示信号和行情对应的情况
    from tools.mplot import fig, sub
    induFig = fig.Fig()
    close = sub.Lines(induFig.ax1, induFig.ax1Top,dfAllAWeek[['CLOSE']],
                      majorGrid='year',majorFormat='%Y',lenuplt=1000,length=100,step=100)
    tech = sub.Lines(induFig.ax2,induFig.ax2Top,dfAllAWeek[['day_trend']],baseLine=0,
                     majorGrid='year', majorFormat='%Y',lenuplt=1000,length=100,step=100)

    close.attach_buysell_sign(dfAllAWeek[['CLOSE','poChg']],mode='longshort')


    close.add_synchron(tech)
    tech.add_synchron(close)

    induFig.show()


# 下面是周线级别行业趋势度计算和策略，添加了亢奋区和悲观区不交易的条件，不过没有影响
if __name__ == '__main__':
    from tools.data import fetch
    from tools.tinytools import stock_related,pandas_related
    dfAllA = fetch.index_one('881001.WI')
    dfAllAWeek = pandas_related.day_downSample(dfAllA)

    # 取中信二级行业来代替原报告中申万二级行业
    dfCiticslvl2 = fetch.index_industry_list('all', citicslevel=2)
    dfCiticslvl2.dropna(inplace=True)
    dfCiticslvl2.drop(['S_INFO_NAME','S_INFO_INDUSTRYCODE','level'],axis=1,inplace=True)

    # 得到中信二级行业的周线
    dfCiticslvl2Week = dfCiticslvl2.groupby(level=1).apply(pandas_related.day_downSample)
    dfCiticslvl2Week.reset_index(level=0, drop=True, inplace=True)

    # 对中信2级行业中每个行业计算DEA分段新逻辑的买卖确认点
    dfCiticslvl2Week = dfCiticslvl2Week.groupby(level=1).apply(groupfunc_find_subindustry_buysell)
    dfCiticslvl2Week.reset_index(level=0,inplace=True,drop=True)
    dfCiticslvl2Week.sort_index(level=[0,1],inplace=True)

    # 由于数据原因，前面的日期处于状态内的行业数只有少数几个，后面有些行业处于状态数也比较小，一个简单的标准是取大于40的日期。
    _groupCountTemp = dfCiticslvl2Week.groupby(level=0)['CLOSE'].count()
    start = _groupCountTemp[_groupCountTemp > 30].index[0]
    end = _groupCountTemp[_groupCountTemp > 30].index[-1]
    dfCiticslvl2Week = dfCiticslvl2Week.loc[start:end]
    del start,end, _groupCountTemp

    # 计算行业趋势度，就是报告中处于买状态的比例
    _calTemp = dfCiticslvl2Week.groupby(level=0).apply(groupFunc_find_updown_ratio).reset_index(level=1, drop=True)
    _calTemp['CODE'] = '881001.WI'
    _calTemp = _calTemp.reset_index().set_index(['DATETIME', 'CODE'])
    dfAllAWeek['day_trend'] = (_calTemp['up'] - _calTemp['down']) / (_calTemp['up'] + _calTemp['down'])
    dfAllAWeek.dropna(inplace=True)
    del _calTemp, dfCiticslvl2, dfCiticslvl2Week

    from tools.backtest import timing, signal
    # 原始信号
    signal.change_step(dfAllAWeek, 'day_trend', 0.1, -0.1,longSignalName='buy', shortSignalName='sell')
    # 将大于0.8定义为亢奋区，不卖，小于-0.8为悲观区，不轻易买
    dfAllAWeek['buy'] = (dfAllAWeek['buy']) & (dfAllAWeek['day_trend'] > -0.8)
    dfAllAWeek['sell'] = (dfAllAWeek['sell']) & (dfAllAWeek['day_trend'] < 0.8)

    signal.long_short_signal(dfAllAWeek, colNameTup=('buy', 'sell'))
    dfAllAWeek.drop(['buy', 'sell'], axis=1, inplace=True)
    # 实际信号
    signal.signal2position_single(dfAllAWeek, signalCol='signal', mode='long')
    # 回测
    induWeekS = timing.Single_Proportion(dfAllAWeek,originSignal='signal',labels=('induWeek',),mode='long')


    # 下面这个是显示信号和行情对应的情况
    from tools.mplot import fig, sub
    induFig = fig.Fig()
    close = sub.Lines(induFig.ax1, induFig.ax1Top,dfAllAWeek[['CLOSE']],
                      majorGrid='year',majorFormat='%Y',lenuplt=1000,length=100,step=100)
    tech = sub.Lines(induFig.ax2,induFig.ax2Top,dfAllAWeek[['day_trend']],baseLine=0,
                     majorGrid='year', majorFormat='%Y',lenuplt=1000,length=100,step=100)

    close.attach_buysell_sign(dfAllAWeek[['CLOSE','poChg']],mode='long')


    close.add_synchron(tech)
    tech.add_synchron(close)

    induFig.show()