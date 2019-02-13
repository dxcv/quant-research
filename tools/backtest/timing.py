
'''
这是存放择时回测的函数。
分别可以进行单品种、多品种的回测。
多品种的金额分配分成按金额比例分配，按手数分配两种类型。
由于计算逻辑差异较大，所有分别分成Proportion和Num两类。
Proportion是不同品种按照金额比例进行分配。如果是一个品种的话则将全部金额分配到该品种上。
Num是
'''
import datetime as dt
import pandas as pd
import numpy as np
from tools.tinytools import fmt_related


#################### 单合约比例回测方法相应的函数 #########################################################################
def single_proportion_backtest(df, pxCol='CLOSE', poChgCol='poChg', tCost=0.0002, margin=1.0):
    '''
    这是用来回测单个品种，输入仓位变化的回测结果的函数，采用矩阵化算法，就是算（position*return）.cumprod，
    回测主要是得到equityCurve，
    position是用上一期的，所以需要下移，position.shift(1)
    :param df: DataFrame，只需要有价格序列和仓位变动序列。
    :param pxCol: str，价格序列的名称
    :param poChgCol: str，仓位变动序列的名称
    :param tCost: 双边交易的总成本，在开仓的时候扣除。上涨会低估，下跌会高估
    :param margin:保证金比例，1表示非杠杆，0.5表示保证金比例为0.5，杠杆2倍。
    :return: 没有返回，对df进行处理
    '''
    df['return'] = df[pxCol].pct_change().fillna(0)
    df['position'] = df[poChgCol].cumsum()
    df['return'] = df['return'] * df['position'].shift().fillna(0.0)
    df['return'] = df['return'] - (df[poChgCol].map(lambda x: 1 if x != 0 else 0)) * tCost
    df['return'] = df['return'] / margin
    df['equityCurve'] = (df['return']+ 1).cumprod()
    df.drop(['return','position'],axis=1,inplace=True)


def single_trade_details(df, poChgCol='poChg', originSignal='确认点',at_once=True, mode='longshort'):
    '''
    这个函数的目的是根据回测好的equityCurve来得到每次交易的收益率，以及持仓期数，仓位类型，平仓原因，
    由于平仓原因的判断是poChg和原始信号的对比来，所以需要poChg和原始信号两列
    :param df: 需要有equityCurve这一列
    :param poChgCol: 仓位变动列的名称
    :param originSignal: 单个原始信号列的名称
    :return: 返回交易细节的df，列名有['开始', '结束', '仓位', '持仓期数', '期间收益率','平仓原因']
    '''
    columns = ['开始', '结束', '持仓期数', '仓位', '期间收益率','平仓原因']
    detailDf = pd.DataFrame(index=range(len(df.loc[df[poChgCol] != 0.0].index)),
                            columns=columns)
    lastIndex = df[df[poChgCol] != 0.0].index[0]
    i = 0
    for curIndex in df[df[poChgCol] != 0.0].index[1:]:
        preIndex = df.index[df.index.get_loc(curIndex) - 1]
        # curIndex = df[df[poChgCol] != 0.0].index[1]
        detailDf.at[i, '开始'] = lastIndex[0]
        detailDf.at[i, '结束'] = curIndex[0]
        detailDf.at[i, '仓位'] = '多仓' if df.at[lastIndex, poChgCol] > 0 else '空仓'
        detailDf.at[i, '持仓期数'] = len(df.loc[lastIndex:preIndex].index)
        detailDf.at[i, '期间收益率'] \
            = df.at[curIndex, 'equityCurve'] / df.at[lastIndex, 'equityCurve'] - 1 # 这个收益率的计算不准确，高估了一个交易成本
        if at_once:
            detailDf.at[i, '平仓原因'] \
                = '信号' if df.at[curIndex, originSignal] != 0.0 \
                         else ('止损' if detailDf.at[i, '期间收益率'] < 0 else '止盈')
        else:
            detailDf.at[i, '平仓原因'] \
                = '信号' if df.at[preIndex, originSignal] != 0.0 \
                         else ('止损' if detailDf.at[i, '期间收益率'] < 0 else '止盈')
        i += 1
        lastIndex = curIndex

        if mode == 'longshort':
            pass
        elif mode == 'long':
            detailDf = detailDf[detailDf['仓位'] == '多仓']
        elif mode == 'short':
            detailDf = detailDf[detailDf['仓位'] == '空仓']
        else:
            raise ValueError('mode必须是longshort,long或者short之一！')

    detailDf.dropna(thresh=3,inplace=True)
    return detailDf


def overall_eval(df, detailDf):
    '''
    这个函数的作用是根据df和detailDf来统计这个策略的整体情况。
    df可以用于DATETIME和CODE两列或者DATTIME一列，以及只支持日线或者比日线更大级别的策略
    :param df: 回测完之后的df
    :param detailDf: 每次交易细节的df
    :return: 返回总体交易评价的series
    '''
    evaluationSer = pd.Series(
        index=['代码', '手续费', '描述',
               '策略区间', '累计收益率', '年化收益率',
               '交易次数', '胜率', '平均收益率', '收益率标准差',
               '平均盈利', '最大盈利', '平均亏损', '最大亏损', '盈亏比',
               '最大回撤', '最大回撤终点', '最大连胜次数', '最大连亏次数'])
    if df.index.nlevels == 2:
        evaluationSer['策略区间'] = df.index[0][0].strftime('%Y-%m-%d') + '至' + df.index[-1][0].strftime('%Y-%m-%d')
    elif df.index.nlevels ==1:
        evaluationSer['策略区间'] = df.index[0].strftime('%Y-%m-%d') + '至' + df.index[-1].strftime('%Y-%m-%d')
    evaluationSer['累计收益率'] = df.iat[-1, df.columns.get_loc('equityCurve')] / df.iat[0, df.columns.get_loc('equityCurve')] - 1
    if df.index.nlevels == 2:
        days = (df.index[-1][0] - df.index[0][0]).days
    elif df.index.nlevels == 1:
        days = (df.index[-1] - df.index[0]).days
    evaluationSer['年化收益率'] = np.exp(np.log(evaluationSer['累计收益率'] + 1) / (days / 365)) - 1

    evaluationSer['交易次数'] = len(detailDf.index)
    evaluationSer['胜率'] = len(detailDf.loc[detailDf['期间收益率'] > 0].index) / evaluationSer['交易次数']
    evaluationSer['平均收益率'] = detailDf['期间收益率'].mean()
    evaluationSer['收益率标准差'] = detailDf['期间收益率'].std()

    evaluationSer['平均盈利'] = detailDf.loc[detailDf['期间收益率'] > 0, '期间收益率'].mean()
    evaluationSer['最大盈利'] = detailDf.loc[detailDf['期间收益率'] > 0, '期间收益率'].max()
    evaluationSer['平均亏损'] = detailDf.loc[detailDf['期间收益率'] < 0, '期间收益率'].mean()
    evaluationSer['最大亏损'] = detailDf.loc[detailDf['期间收益率'] < 0, '期间收益率'].min()
    evaluationSer['盈亏比'] = - evaluationSer['胜率'] * evaluationSer['平均盈利'] \
                            / ((1 - evaluationSer['胜率']) * evaluationSer['平均亏损'])

    maxRetreat = 1.0
    retreatCount = 1.0
    retreatEndTime = None
    comboWin = 0
    winCount = 0
    comboLost = 0
    lostCount = 0
    for i in detailDf.index:
        # 统计连胜连亏次数
        if detailDf.at[i, '期间收益率'] > 0:
            winCount += 1
            lostCount = 0
            if winCount > comboWin:
                comboWin = winCount
            retreatCount = 1.0
        elif detailDf.at[i, '期间收益率'] < 0:
            lostCount += 1
            winCount = 0
            if lostCount > comboLost:
                comboLost = lostCount

            # 统计最大回撤
            retreatCount = retreatCount * (1 + detailDf.at[i, '期间收益率'])
            if retreatCount < maxRetreat:
                maxRetreat = retreatCount
                retreatEndTime = detailDf.at[i, '结束']

    evaluationSer['最大回撤'] = maxRetreat - 1
    evaluationSer['最大回撤终点'] = retreatEndTime
    evaluationSer['最大连胜次数'] = comboWin
    evaluationSer['最大连亏次数'] = comboLost

    # 转变成字符串来显示
    evaluationSer['盈亏比'] = fmt_related.to_str(evaluationSer['盈亏比'], float, '.2f')
    evaluationSer['最大回撤终点'] = fmt_related.to_str(evaluationSer['最大回撤终点'], dt.datetime, '%Y-%m-%d')
    evaluationSer = evaluationSer.apply(lambda x: fmt_related.to_str(x, float, '%'))
    evaluationSer = evaluationSer.apply(lambda x: fmt_related.to_str(x, int, 'd'))
    return evaluationSer


def year_month_analysis(detailDf):
    '''
    这个函数的目的是根据detailDf得到个月的收益率。
    :param detailDf: 每次交易细节的df
    :return:
    '''
    detailDf['year'] = detailDf['结束'].map(lambda x: x.year)
    detailDf['month'] = detailDf['结束'].map(lambda x: x.month)

    yearDF = (detailDf['期间收益率'] + 1).groupby([detailDf['year'], detailDf['month']]).prod()
    yearDF = pd.DataFrame(yearDF)
    yearDF['期间收益率'] = yearDF['期间收益率'] - 1
    detailDf.drop(['year','month'],axis=1,inplace=True)
    return yearDF


#################### 多合约比例回测方法相应的函数 #########################################################################
def multi_proportion_backtest(df, pxColList, longshortList, proportionList, tCost=0.0002, margin=1.0):
    '''
    这是用来回测多个品种，输入仓位变化的回测结果的函数，采用矩阵化算法，就是算（position*return）.cumprod，
    回测主要是得到equityCurve，
    position是用上一期的，所以需要下移，position.shift(1)
    :param df: DataFrame，只需要有价格序列和仓位变动序列。
    :param pxColList: list, 需要制定价格序列的列名
    :param longshortList: list, 需要制定对应价格序列的多空状况，只能是1或-1，量在下面的比例给出
    :param proportionList: list，每个品种的每次交易的投入资金比例。都是正数，加起来要等于1，例如[0.5,0.5]
    :param tCost: 双边交易的总成本，在开仓的时候扣除。上涨会低估，下跌会高估
    :param margin: 保证金比例，1表示非杠杆，0.5表示保证金比例为0.5，杠杆2倍。
    :return: 没有返回，对df进行处理
    '''
    # 输入检查，以免不小心出问题。
    if len(pxColList) != len(longshortList) or len(pxColList) != len(proportionList) or len(longshortList) != len(proportionList):
        raise ValueError('pxColList, longshortList, proportionList必须匹配！')

    df['position'] = df['poChg'].cumsum()
    evalStr = ''
    for i in range(len(proportionList)):
        evalStr = evalStr + f'df[\'return_{pxColList[i]}\'] ' \
                            f'* df[\'position_{pxColList[i]}\'].shift().fillna(0.0) ' \
                            f'* proportionList[i] + '
        df[f'return_{pxColList[i]}'] = df[pxColList[i]].pct_change().fillna(0.0)
        df[f'position_{pxColList[i]}'] = df['position'] * longshortList[i]
    evalStr = evalStr[:-3]
    df['return'] = eval(evalStr)

    df['return'] = df['return'] - (df['poChg'].map(lambda x: 1 if x != 0 else 0)) * tCost
    df['return'] = df['return'] / margin
    df['equityCurve'] = (df['return'] + 1).cumprod()
    df.drop(['return', 'position'], axis=1, inplace=True)
    for i in range(len(proportionList)):
        df.drop([f'return_{pxColList[i]}',f'position_{pxColList[i]}'], axis=1, inplace=True)


def multi_trade_details(df, poChgCol='poChg', originSignal='确认点',pxColList=[], longshortList=[], proportionList=[],margin=1.0):
    # 输入检查，以免不小心出问题。
    if len(pxColList) != len(longshortList) or len(pxColList) != len(proportionList) or len(longshortList) != len(proportionList):
        raise ValueError('pxColList, longshortList, proportionList必须匹配！')
    columns = ['开始', '结束', '持仓期数']
    columns.extend([f'仓位_{name}' for name in pxColList])
    columns.append('总体仓位')
    columns.extend([f'期间收益率_{name}' for name in pxColList])
    columns.extend(['期间收益率','平仓原因'])
    detailDf = pd.DataFrame(index=range(len(df.loc[df[poChgCol] != 0.0].index)),
                            columns=columns)
    detailDf[[f'仓位_{name}' for name in pxColList]] = 0
    lastIndex = df[df[poChgCol] != 0.0].index[0]
    i = 0
    for curIndex in df[df[poChgCol] != 0.0].index[1:]:
        preIndex = df.index[df.index.get_loc(curIndex) - 1]
        # curIndex = df[df[poChgCol] != 0.0].index[1]
        detailDf.at[i, '开始'] = lastIndex
        detailDf.at[i, '结束'] = curIndex
        detailDf.loc[i, [f'仓位_{name}' for name in pxColList]] \
            = (detailDf.loc[i, [f'仓位_{name}' for name in pxColList]] + longshortList) * df.at[lastIndex, poChgCol]
        detailDf.at[i, '总体仓位'] = '多仓' if df.at[lastIndex, poChgCol] > 0 else '空仓'
        detailDf.at[i, '持仓期数'] = len(df.loc[lastIndex:preIndex].index)
        detailDf.loc[i, [f'期间收益率_{name}' for name in pxColList]] \
            = ((df.loc[curIndex, pxColList] / df.loc[lastIndex, pxColList] - 1)
               * longshortList
               * (1 if df.at[lastIndex, poChgCol] > 0 else -1)
               * proportionList
               / margin).values # # 由于这个没有严格考虑交易成本的影响，
        detailDf.at[i, '期间收益率'] \
            = df.at[curIndex, 'equityCurve'] / df.at[lastIndex, 'equityCurve'] - 1
        detailDf.at[i, '平仓原因'] \
            = '信号' if df.at[curIndex, originSignal] != 0.0 \
                     else ('止损' if detailDf.at[i, '期间收益率'] < 0 else '止盈')
        i += 1
        lastIndex = curIndex
    for name in pxColList:
        detailDf[f'仓位_{name}'] = detailDf[f'仓位_{name}'].apply(lambda x: '多仓' if x > 0 else '空仓')
    detailDf.dropna(thresh=3,inplace=True)
    return detailDf


def single_num_backtest():
    pass


def multi_num_backtest():
    pass


#################### 将回测、分析和显示综合起来的类 #######################################################################
class BackTest(object):


    def plot_equity_curve(self,period='day',contrast='000001.SH', labels=('strategy')):
        from tools.mplot import sub, fig
        self.fig = fig.Fig(subplotdict={'axEquity':[0.05,0.43,0.7,0.55],
                                 'axYear':[0.05,0.03,0.7,0.37],
                                 'axDesc':[0.8,0.03,0.15,95]},
                    if_top_need=(True,False,False))
        self.equityCurve = sub.Equity_Curve(self.fig.axEquity, self.df[['equityCurve']], period=period,contrast=contrast, labels=labels)
        sub.draw_series_on_axes(self.fig.axDesc, self.evalSer, 'white')
        sub.draw_month_analysis(self.fig.axYear, self.yearDf)
        self.fig.show()


    def show(self):
        self.fig.show()


class Single_Proportion(BackTest):


    def __init__(self, df, priceCol='CLOSE', poChgCol='poChg', originSignal='确认点',
                 at_once=True, mode='longshort',
                 tCost=0.0002,margin=1.0,
                 period='day',
                 contrast='000300.SH', labels=('strategy',)):
        self.df = df
        single_proportion_backtest(self.df, pxCol=priceCol, poChgCol=poChgCol, tCost=tCost, margin=margin)
        self.detailDf = single_trade_details(self.df, poChgCol=poChgCol, originSignal=originSignal, at_once=at_once, mode=mode)
        self.evalSer = overall_eval(self.df, self.detailDf)
        self.yearDf = year_month_analysis(self.detailDf)
        self.plot_equity_curve(period=period,contrast=contrast,labels=labels)


#################### Single_Proportion用于日线策略的回测例子 #############################################################
# 这个是single_proportion_backtest的使用例子，以及在图上显示回测结果,参数是at_once=True,mode='longshort'
if __name__ == '__main__':
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal
    # 生成原始信号和买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    signal.signal2position_single(df, signalCol='signal', at_once=True, mode='longshort') # 仓位：即期，多空，止损，保证金

    # 单个品种的回测
    btest = Single_Proportion(df,priceCol='CLOSE',poChgCol='poChg',originSignal='signal',
                              at_once=True, mode='longshort') # 回测：信号与仓位，即期，多空，保证金，交易成本

    del df, btest


# 这个是single_proportion_backtest的使用例子，以及在图上显示回测结果,参数是at_once=False,mode='longshort'
if __name__ == '__main__':
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal
    # 生成原始信号和买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    signal.signal2position_single(df, signalCol='signal', at_once=False, mode='longshort') # 仓位：即期，多空，止损，保证金

    # 单个品种的回测
    btest = Single_Proportion(df,priceCol='CLOSE',poChgCol='poChg',originSignal='signal',
                              at_once=False, mode='longshort') # 回测：信号与仓位，即期，多空，保证金，交易成本

    del df, btest


# 这个是single_proportion_backtest的使用例子，以及在图上显示回测结果,参数是at_once=True,mode='long'
if __name__ == '__main__':
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal
    # 生成原始信号和买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    signal.signal2position_single(df, signalCol='signal', at_once=True, mode='long') # 仓位：即期，多空，止损，保证金

    # 单个品种的回测
    btest = Single_Proportion(df,priceCol='CLOSE',poChgCol='poChg',originSignal='signal',
                              at_once=True, mode='long') # 回测：信号与仓位，即期，多空，保证金，交易成本

    del df, btest


# 这个是single_proportion_backtest的使用例子，以及在图上显示回测结果,参数是at_once=True,mode='short'
if __name__ == '__main__':
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal
    # 生成原始信号和买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    signal.signal2position_single(df, signalCol='signal', at_once=True, mode='short') # 仓位：即期，多空，止损，保证金

    # 单个品种的回测
    btest = Single_Proportion(df,priceCol='CLOSE',poChgCol='poChg',originSignal='signal',
                              at_once=True, mode='short') # 回测：信号与仓位，即期，多空，保证金，交易成本

    del df, btest


# 这个是single_proportion_backtest的使用例子，以及在图上显示回测结果,参数是at_once=True,mode='longshort',zhisun=-0.08, zhiying=0.05
if __name__ == '__main__':
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal
    # 生成原始信号和买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    # 仓位：即期，多空，止损，保证金
    signal.signal2position_single(df, signalCol='signal', at_once=True, mode='longshort', zhisun=-0.08, zhiying=0.05)

    # 单个品种的回测
    btest = Single_Proportion(df,priceCol='CLOSE',poChgCol='poChg',originSignal='signal',
                              at_once=True, mode='longshort') # 回测：信号与仓位，即期，多空，保证金，交易成本

    del df, btest


# 这个是single_proportion_backtest的使用例子，以及在图上显示回测结果,参数是at_once=True,mode='long',zhisun=-0.08, zhiying=0.05
if __name__ == '__main__':
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal
    # 生成原始信号和买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    # 仓位：即期，多空，止损，保证金
    signal.signal2position_single(df, signalCol='signal', at_once=True, mode='long', zhisun=-0.08, zhiying=0.05)

    # 单个品种的回测
    btest = Single_Proportion(df,priceCol='CLOSE',poChgCol='poChg',originSignal='signal',
                              at_once=True, mode='long') # 回测：信号与仓位，即期，多空，保证金，交易成本

    del df, btest


# 这个是single_proportion_backtest的使用例子，以及在图上显示回测结果,参数是at_once=True,mode='short',zhisun=-0.08, zhiying=0.05
if __name__ == '__main__':
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal
    # 生成原始信号和买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    # 仓位：即期，多空，止损，保证金
    signal.signal2position_single(df, signalCol='signal', at_once=True, mode='short', zhisun=-0.08, zhiying=0.05)

    # 单个品种的回测
    btest = Single_Proportion(df,priceCol='CLOSE',poChgCol='poChg',originSignal='signal',
                              at_once=True, mode='short') # 回测：信号与仓位，即期，多空，保证金，交易成本

    del df, btest


# 这个是single_proportion_backtest的使用例子，以及在图上显示回测结果,参数是at_once=True,mode='longshort'，margin=0.3
if __name__ == '__main__':
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal
    # 生成原始信号和买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    # 仓位：即期，多空，止损，保证金
    signal.signal2position_single(df, signalCol='signal', at_once=True, mode='longshort', margin=0.3)

    # 单个品种的回测
    btest = Single_Proportion(df,priceCol='CLOSE',poChgCol='poChg',originSignal='signal',
                              at_once=True, mode='longshort',margin=0.3) # 回测：信号与仓位，即期，多空，保证金，交易成本

    del df, btest


# 这个是single_proportion_backtest的使用例子，以及在图上显示回测结果,参数是at_once=True,mode='longshort',zhisun=-0.08, zhiying=0.05，margin=0.3
if __name__ == '__main__':
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal
    # 生成原始信号和买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    # 仓位：即期，多空，止损，保证金
    signal.signal2position_single(df, signalCol='signal', at_once=True, mode='longshort', zhisun=-0.08, zhiying=0.05, margin=0.3)

    # 单个品种的回测
    btest = Single_Proportion(df,priceCol='CLOSE',poChgCol='poChg',originSignal='signal',
                              at_once=True, mode='longshort',margin=0.3) # 回测：信号与仓位，即期，多空，保证金，交易成本

    del df, btest


# 这个是single_proportion_backtest的使用例子，以及在图上显示回测结果,参数是at_once=True,mode='long',zhisun=-0.08, zhiying=0.05，margin=0.3
if __name__ == '__main__':
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal
    # 生成原始信号和买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    # 仓位：即期，多空，止损，保证金
    signal.signal2position_single(df, signalCol='signal', at_once=True, mode='long', zhisun=-0.08, zhiying=0.05, margin=0.3)

    # 单个品种的回测
    btest = Single_Proportion(df,priceCol='CLOSE',poChgCol='poChg',originSignal='signal',
                              at_once=True, mode='long',margin=0.3) # 回测：信号与仓位，即期，多空，保证金，交易成本

    del df, btest


# 这个是single_proportion_backtest的使用例子，以及在图上显示回测结果,参数是at_once=True,mode='short',zhisun=-0.08, zhiying=0.05，margin=0.3
if __name__ == '__main__':
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal
    # 生成原始信号和买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    # 仓位：即期，多空，止损，保证金
    signal.signal2position_single(df, signalCol='signal', at_once=True, mode='short', zhisun=-0.08, zhiying=0.05, margin=0.3)

    # 单个品种的回测
    btest = Single_Proportion(df,priceCol='CLOSE',poChgCol='poChg',originSignal='signal',
                              at_once=True, mode='short',margin=0.3) # 回测：信号与仓位，即期，多空，保证金，交易成本

    del df, btest


#################### Single_Proportion用于分钟线策略的回测例子 ###########################################################
if __name__ == '__main__':
    # 这个最大的问题就是equityCurve的长度比基准曲线长，这个时候会出问题，又不想截equityCurve。
    # 现在equityCurve的图已经完善得差不多了，主要问题都是基准的长度问题
    import talib as ta
    from tools.data import fetch
    df = fetch.n_min('000300.SH', 30,startTime='2017-07-01')
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal
    # 生成原始信号和买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    # 仓位：即期，多空，止损，保证金
    signal.signal2position_single(df, signalCol='signal', at_once=True, mode='longshort', margin=0.5)

    btest = Single_Proportion(df,originSignal='signal',margin=0.3,period='30min',contrast='000300.SH',tCost=0.001)

    del df, btest


#################### multi_proportion_backtest用于日线策略的回测例子 #####################################################

if __name__ == '__main__':
    from tools.data import fetch
    from tools.backtest import signal
    dfzz500 = fetch.index_one('000905.SH', startTime='2007-01-15')
    dfsz50 = fetch.index_one('000016.SH', startTime='2007-01-15')

    df = pd.DataFrame()
    df['target'] = dfsz50.reset_index(level=1)['PCTCHANGE'] - dfzz500.reset_index(level=1)['PCTCHANGE']
    df['sz50'] = dfsz50.reset_index(level=1)['CLOSE']
    df['zz500'] = dfzz500.reset_index(level=1)['CLOSE']
    df['return_sz50'] = dfsz50.reset_index(level=1)['PCTCHANGE'] / 100
    df['return_zz500'] = dfzz500.reset_index(level=1)['PCTCHANGE'] / 100
    # df.to_csv('D:\\df.csv')
    signal.up_cross(df, 'target')
    signal.signal2position_multi(df, signalCol='signal',
                                 mode='longshort', )

    multi_proportion_backtest(df, ['sz50', 'zz500'], [1, -1], [0.5, 0.5], tCost=0.0002, )
    detailDf = multi_trade_details(df, originSignal='signal', pxColList=['sz50', 'zz500'], longshortList=[1, -1],
                                          proportionList=[0.5, 0.5])
    evalSer = overall_eval(df, detailDf)
    yearDf = year_month_analysis(detailDf)

    #  这个显示回测结果的简单的图
    from tools.mplot import fig, sub
    btFig = fig.Fig(subplotdict={'axEquity':[0.05,0.43,0.7,0.55],
                                 'axYear':[0.05,0.03,0.7,0.37],
                                 'axDesc':[0.8,0.03,0.15,95]},
                    if_top_need=(True,False,False))
    equityCurve = sub.Equity_Curve(btFig.axEquity, df[['equityCurve']],contrast='000300.SH',labels=['DIFF-DEA'])
    sub.draw_series_on_axes(btFig.axDesc,evalSer,'white')
    sub.draw_month_analysis(btFig.axYear, yearDf)
    btFig.show()


