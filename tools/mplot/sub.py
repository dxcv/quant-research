
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, MultipleLocator, FuncFormatter, FixedFormatter
from matplotlib.font_manager import FontProperties
from matplotlib import transforms
from matplotlib.patches import Rectangle
from tools.data import fetch
from tools.tinytools import fmt_related
import numpy as np
import pandas as pd
import datetime as dt
from dateutil.parser import parse
import const

#################### 小函数 ############################################################################################
def _price_formatter_func(num, pos=None):
    '''
    价格的formatter
    :param num:
    :param pos:
    :return:
    '''
    return '%0.2f' % num


def _vol_formatter_func_wan(num, pos=None):
    '''
    量的formatter，单位是万股
    :param num:
    :param pos:
    :return:
    '''
    return u'%0.0f万' % (num/10000)


def _vol_formatter_func_shou(num, pos=None):
    '''
    量的formatter，单位是手
    :param num:
    :param pos:
    :return:
    '''
    return u'%0.0f手' % (num/100)


def _set_spines(ax, color, width=1.0):
    '''
    设置某个axes的边框的颜色和粗细
    :param ax:
    :param color: str
    :param width: float
    :return:
    '''
    ax.spines['top'].set_color(color)
    ax.spines['bottom'].set_color(color)
    ax.spines['left'].set_color(color)
    ax.spines['right'].set_color(color)

    ax.spines['top'].set_lw(width)
    ax.spines['bottom'].set_lw(width)
    ax.spines['left'].set_lw(width)
    ax.spines['right'].set_lw(width)


def _autolabel(ax,rects,sign,**kw):
    '''
    给bar添加数据标签
    :param ax:
    :param rects:
    :param sign:
    :param kw:
    :return:
    '''
    for rect in rects:
        height = rect.get_height() * sign
        if sign < 0:
            vaStr = 'top'
        elif sign > 0:
            vaStr = 'bottom'
        ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                '{:.2f}%'.format(height * 100),
                ha='center', va=vaStr,**kw)

#################### 数据处理 ###########################################################################################
def _gen_grid_col(df, majorGrid=None, minorGrid=None):
    '''
    这个函数是在df中生成一个用于标记主grid和次grid的列
    :param df: df的index是int，必须有DATETIME这一列
    :param majorGrid: 可选的是month,week,day,hour
    :param minorGrid: 可选的是month,week,day,hour
    :return: 实现的功能是df增加一列。
    '''
    if majorGrid is None and minorGrid is None:
        return None
    if majorGrid == 'month':
        df['majorGridCol'] = (
            df['DATETIME'].apply(lambda x: x.month) - df['DATETIME'].apply(lambda x: x.month).shift(1)) \
            .apply(lambda x: True if x == 1.0 else False)
        df.at[0, 'majorGridCol'] = True
    elif majorGrid == 'week':
        df['majorGridCol'] = (
            df['DATETIME'].apply(lambda x: x.weekday()) - df['DATETIME'].apply(lambda x: x.weekday()).shift(-1)). \
            apply(lambda x: True if x > 0.0 else False)
        df.at[0, 'majorGridCol'] = True
    elif majorGrid == 'day':
        df['majorGridCol'] = (
            df['DATETIME'].apply(lambda x: x.day) - df['DATETIME'].apply(lambda x: x.day).shift(1)). \
            apply(lambda x: True if x != 0.0 else False)
        df.at[0, 'majorGridCol'] = True
    elif majorGrid == 'hour':
        df['majorGridCol'] = (
            df['DATETIME'].apply(lambda x: x.hour) - df['DATETIME'].apply(lambda x: x.hour).shift(1)). \
            apply(lambda x: True if x != 0.0 else False)
        df.at[0, 'majorGridCol'] = True
    elif majorGrid == 'year':
        df['majorGridCol'] = (
            df['DATETIME'].apply(lambda x: x.year) - df['DATETIME'].apply(lambda x: x.year).shift(1)) \
            .apply(lambda x: True if x == 1.0 else False)
        df.at[0, 'majorGridCol'] = True

    if minorGrid is None:
        pass
    elif minorGrid == 'month':
        df['minorGridCol'] = (
            df['DATETIME'].apply(lambda x: x.month) - df['DATETIME'].apply(lambda x: x.month).shift(1)) \
            .apply(lambda x: True if x == 1.0 else False)
        df.at[0, 'minorGridCol'] = True
    elif minorGrid == 'week':
        df['minorGridCol'] = (
            df['DATETIME'].apply(lambda x: x.weekday()) - df['DATETIME'].apply(lambda x: x.weekday()).shift(-1)). \
            apply(lambda x: True if x > 0.0 else False)
        df.at[0, 'minorGridCol'] = True
    elif minorGrid == 'day':
        df['minorGridCol'] = (
            df['DATETIME'].apply(lambda x: x.day) - df['DATETIME'].apply(lambda x: x.day).shift(1)). \
            apply(lambda x: True if x != 0.0 else False)
        df.at[0, 'minorGridCol'] = True
    elif minorGrid == 'hour':
        df['minorGridCol'] = (
            df['DATETIME'].apply(lambda x: x.hour) - df['DATETIME'].apply(lambda x: x.hour).shift(1)). \
            apply(lambda x: True if x != 0.0 else False)
        df.at[0, 'minorGridCol'] = True
    elif minorGrid == 'year':
        df['minorGridCol'] = (
            df['DATETIME'].apply(lambda x: x.year) - df['DATETIME'].apply(lambda x: x.year).shift(1)) \
            .apply(lambda x: True if x == 1.0 else False)
        df.at[0, 'minorGridCol'] = True


def _init_range_nums(startNum, endNum, length, df):
    '''
    这个函数是根据df的长度初始化适合使用的startNum, endNum, length
    :param startNum:
    :param endNum:
    :param length:
    :param df:
    :return: 处理过的startNum,endNum,showLen
    '''
    if endNum is None:
        startNum = startNum
        endNum = length + startNum
    elif endNum > 0:
        raise ValueError('endNum must be smaller than or equal to 0!')
    elif endNum == 0:
        startNum = df.index[-1] + 1 - length
        endNum = df.index[-1] + 1
    else:
        startNum = df.index[endNum] - length
        endNum = df.index[endNum]
    showLen = length
    return startNum,endNum,showLen


def _detect_period_and_code(df):
    '''
    从df中找到时间间隔，代码，中文名称
    :param df: df必须有DATETIME列和CODE列，这两列不能作为index
    :return:
    '''
    # 对输入的df进行判断
    if not (isinstance(df['DATETIME'][0], dt.datetime)):
        raise ValueError('必须有DATETIME这一列，而且里面的是dt.datetime格式')

    # 判断周期
    delta = df['DATETIME'][1] - df['DATETIME'][0]
    if delta == dt.timedelta(1):
        periodStr = '日线'
    elif delta > dt.timedelta(1):
        periodStr = f'{delta.days}日线'
    elif delta < dt.timedelta(1):
        periodStr = f'{int(delta.seconds / 60)}分钟线'
    else:
        periodStr = ''

    # 检测股票代码和中文名
    code = df['CODE'][0]
    try:
        _dfCode = fetch._db_command_2_df(
            'select name from name_ashare_index '
            'where code = \'{code}\' '
            'order by datetime desc'.format(code=code),
            db='stock_day')
        chineseName = _dfCode.values[0][0]
    except:
        print('无法读取中文名，返回空字符串！')
        chineseName = ''
    return delta, periodStr, code, chineseName


#################### 画主要内容 #########################################################################################
def _draw_candleStick(ax,df,align='center'):
    '''
    这个是在ax中画K线图函数，
    :param ax:
    :param df:
    :param align:对齐方式，'center'或者'left'
    :return:
    '''
    df['updown'] = df['CLOSE'] - df['OPEN']
    df['updown'] = df['updown'].apply(lambda x: (True if x >= 0 else False))
    updf = df[df['updown'] == True]
    downdf = df[df['updown'] == False]

    if align == 'center':
        containerVlineUP = ax.vlines(updf.index, updf['LOW'], updf['HIGH'],
                                        color='red', linewidth=1.0, label='_nolegend_',
                                        zorder=2)
        containerVlineDOWN = ax.vlines(downdf.index, downdf['LOW'], downdf['HIGH'],
                                          color='green', linewidth=1.0, label='_nolegend_',
                                          zorder=2)

        containerBarUP = ax.bar(updf.index,
                                   updf['CLOSE'] - updf['OPEN'],
                                   bottom=updf['OPEN'].values,
                                   width=0.6, facecolor='black', linewidth=1, edgecolor='red',
                                   zorder=3, picker=True,align='center')
        containerBarDOWN = ax.bar(downdf.index,
                                     downdf['CLOSE'] - downdf['OPEN'],
                                     bottom=downdf['OPEN'].values,
                                     width=0.6, facecolor='green', linewidth=1,
                                     edgecolor='green',
                                     zorder=3, picker=True,align='center')
    elif align == 'left':
        containerVlineUP = ax.vlines(updf.index+0.3, updf['LOW'], updf['HIGH'],
                                     color='red', linewidth=1.0, label='_nolegend_',
                                     zorder=2)
        containerVlineDOWN = ax.vlines(downdf.index+0.3, downdf['LOW'], downdf['HIGH'],
                                       color='green', linewidth=1.0, label='_nolegend_',
                                       zorder=2)

        containerBarUP = ax.bar(updf.index,
                                updf['CLOSE'] - updf['OPEN'],
                                bottom=updf['OPEN'].values,
                                width=0.6, facecolor='black', linewidth=1, edgecolor='red',
                                zorder=3, picker=True)
        containerBarDOWN = ax.bar(downdf.index,
                                  downdf['CLOSE'] - downdf['OPEN'],
                                  bottom=downdf['OPEN'].values,
                                  width=0.6, facecolor='green', linewidth=1,
                                  edgecolor='green',
                                  zorder=3, picker=True)


def _draw_volume(ax,df,align='center'):
    '''
    这个是在ax中画成交量柱形图的函数
    :param ax:
    :param df:
    :param align:对齐方式，'center'或者'left'
    :return:
    '''
    df['updown'] = df['CLOSE'] - df['OPEN']
    df['updown'] = df['updown'].apply(lambda x: (True if x >= 0 else False))
    updf = df[df['updown'] == True]
    downdf = df[df['updown'] == False]
    if align == 'center':
        containerVolUP = ax.bar(updf.index, updf['VOLUME'].values,
                                   width=0.6, align='center', facecolor='black', linewidth=1, edgecolor='red',
                                   zorder=2, picker=True)
        containerVolDOWN = ax.bar(downdf.index, downdf['VOLUME'].values,
                                     width=0.6, align='center', facecolor='green', linewidth=1,
                                     edgecolor='green',
                                     zorder=2, picker=True)
    elif align == 'left':
        containerVolUP = ax.bar(updf.index, updf['VOLUME'].values,
                                width=0.6, facecolor='black', linewidth=1, edgecolor='red',
                                zorder=2, picker=True)

        containerVolDOWN = ax.bar(downdf.index, downdf['VOLUME'].values,
                                  width=0.6, facecolor='green', linewidth=1,
                                  edgecolor='green',
                                  zorder=2, picker=True)


def _draw_lines(ax, dfLines, baseLine=None, colors=const.COLORS, markers=const.MARKERS,**kwargs):
    '''
    这个是在ax中画线的函数，dfLines中只含有需要画的线的列，
    :param ax:
    :param dfLines: dfLines中只含有需要画的线的列，
    :param colors: list，各条线的颜色
    :param markers: list，各条线的标记
    :param kwargs: 其他画线所需的参数
    :return:
    '''
    if baseLine is None:
        i = 0
        for lineName in dfLines.columns:
            if lineName == 'DATETIME' or lineName == 'CODE' \
                    or lineName == 'majorGridCol' or lineName == 'minorGridCol':
                continue
            else:
                ax.plot(dfLines.index, dfLines[lineName].values, color=colors[i],marker=markers[i],**kwargs)
                i += 1
    elif isinstance(baseLine, float) or isinstance(baseLine, int):
        ax.plot(dfLines.index, [baseLine,]*len(dfLines.index),color='red', lw=2)
        i = 1
        for lineName in dfLines.columns:
            if lineName == 'DATETIME' or lineName == 'CODE' \
                    or lineName == 'majorGridCol' or lineName == 'minorGridCol':
                continue
            else:
                ax.plot(dfLines.index, dfLines[lineName].values, color=colors[i],marker=markers[i],**kwargs)
                i += 1
    elif isinstance(baseLine, list) and isinstance(baseLine[0],float) or isinstance(baseLine[0],int):
        for bline in baseLine:
            ax.plot(dfLines.index, [bline,]*len(dfLines.index),color='red', lw=2)
        i = len(baseLine)
        for lineName in dfLines.columns:
            if lineName == 'DATETIME' or lineName == 'CODE' \
                    or lineName == 'majorGridCol' or lineName == 'minorGridCol':
                continue
            else:
                ax.plot(dfLines.index, dfLines[lineName].values, color=colors[i],marker=markers[i],**kwargs)
                i += 1
    else:
        raise ValueError('baseLine必须是float或者int！')


def _draw_intraday_vol(axVol, df1minShow, fontProp,
                       majorList=(1, 60, 120, 180, 240), minorList=(30, 90, 150, 210)):
    axVol.bar(range(241), df1minShow['VOLUME'],
              width=0.6, align='center', color='yellow', edgecolor='yellow')

    # 设置x轴的范围
    axVol.set_xlim(0 - 0.5, len(df1minShow.index) - 0.5)

    closeXMajorLocator = FixedLocator(np.array(majorList))
    closeXMinorLocator = FixedLocator(np.array(minorList))

    # 设置x轴的locator
    volXAxis = axVol.get_xaxis()
    volXAxis.set_major_locator(closeXMajorLocator)
    volXAxis.set_minor_locator(closeXMinorLocator)

    # x轴的ticklabel
    majorFormatter = FixedFormatter(['09:30','10:30','13:00','14:00','15:00',])
    volXAxis.set_major_formatter(majorFormatter)

    volXAxis.get_ticklabels()[0].set_ha('left')
    volXAxis.get_ticklabels()[-1].set_ha('right')

    # x轴的grid
    volXAxis.grid(True, 'major', color='red', linestyle='solid', linewidth=0.5, zorder=1)
    volXAxis.grid(True, 'minor', color='red', linestyle='dotted', linewidth=0.5, zorder=1)

    # vol y轴的东西
    yMaxVol = df1minShow['VOLUME'].max()
    yMinVol = 0.0
    axVol.set_ylim(ymin=yMinVol, ymax=yMaxVol)

    # y的locator
    try:
        volYMajorLocator = FixedLocator(np.arange(round(yMinVol, 0) * 1.1,
                                                  round(yMaxVol, 0),
                                                  round((round(yMaxVol, 0) - (round(yMinVol, 0) * 1.1)) / 4, 0)
                                                  )
                                        )
    except ZeroDivisionError:
        volYMajorLocator = FixedLocator([0.0,1.0])
    volYAxis = axVol.get_yaxis()
    volYAxis.set_major_locator(volYMajorLocator)
    volYAxis.set_ticks_position('right')
    volYAxis.grid(True, 'major', color='red', linestyle='dotted', linewidth=0.5, zorder=1)

    volYMajorFormatter = FuncFormatter(_vol_formatter_func_shou)
    volYAxis.set_major_formatter(volYMajorFormatter)
    for tickLabel in volYAxis.get_ticklabels():
        tickLabel.set_fontproperties(fontProp)

    volYAxis.get_ticklabels()[0].set_va('bottom')
    volYAxis.get_ticklabels()[-1].set_va('top')

    # 设置ticklabel的格式
    axVol.yaxis.set_tick_params(labelcolor='white')


def _draw_intraday_close(axClose, df1minShow, preclose,
                         majorList=(1, 60, 120, 180, 240), minorList=(30, 90, 150, 210)):
    # 画主要的图
    axClose.plot(range(241), df1minShow['CLOSE'], color='yellow', lw=2)
    axClose.plot(range(241), [preclose, ] * len(df1minShow.index), color='red')

    # x轴格式的设置
    # x轴的显示范围
    axClose.set_xlim(0 - 0.5, len(df1minShow.index) - 0.5)

    # x轴的tick
    closeXMajorLocator = FixedLocator(np.array(majorList))
    closeXMinorLocator = FixedLocator(np.array(minorList))

    closeXAxis = axClose.get_xaxis()
    closeXAxis.set_major_locator(closeXMajorLocator)
    closeXAxis.set_minor_locator(closeXMinorLocator)

    closeXAxis.grid(True, 'major', color='red', linestyle='solid', linewidth=0.5, zorder=1)
    closeXAxis.grid(True, 'minor', color='red', linestyle='dotted', linewidth=0.5, zorder=1)

    # x轴的ticklabel
    majorFormatter = FixedFormatter(['09:30','10:30','13:00','14:00','15:00',])
    closeXAxis.set_major_formatter(majorFormatter)

    closeXAxis.get_ticklabels()[0].set_ha('left')
    closeXAxis.get_ticklabels()[-1].set_ha('right')

    # 设置y轴的范围
    distance = max(abs(df1minShow['CLOSE'].min() - preclose),
                        abs(df1minShow['CLOSE'].max() - preclose))
    yMinClose = (preclose - distance) * 0.998
    yMaxClose = (preclose + distance) * 1.002
    axClose.set_ylim(yMinClose, yMaxClose)

    # y的locator
    # np.arange(yMinClose, yMaxClose, (yMaxClose - yMinClose) / 10)

    closeYMajorLocator = FixedLocator(
        np.arange(yMinClose, yMaxClose + (yMaxClose - yMinClose) / 10,
                  (yMaxClose - yMinClose) / 10
                  )
    )

    closeYAxis = axClose.get_yaxis()
    closeYAxis.set_major_locator(closeYMajorLocator)
    closeYAxis.set_ticks_position('right')
    closeYAxis.grid(True, 'major', color='red', linestyle='dotted', linewidth=0.5, zorder=1)

    # y的formatter
    def closeMajorFormatterFunc_y(num, pos=None):
        return '%0.1f%% %0.2f' % (abs(num / preclose - 1) * 100, num)

    closeYMajorFormatter = FuncFormatter(closeMajorFormatterFunc_y)
    closeYAxis.set_major_formatter(closeYMajorFormatter)

    closeYAxis.get_ticklabels()[0].set_va('bottom')
    closeYAxis.get_ticklabels()[-1].set_va('top')

    closeYAxis.get_ticklabels()[0].set_color('green')
    closeYAxis.get_ticklabels()[1].set_color('green')
    closeYAxis.get_ticklabels()[2].set_color('green')
    closeYAxis.get_ticklabels()[3].set_color('green')
    closeYAxis.get_ticklabels()[4].set_color('green')

    closeYAxis.get_ticklabels()[6].set_color('red')
    closeYAxis.get_ticklabels()[7].set_color('red')
    closeYAxis.get_ticklabels()[8].set_color('red')
    closeYAxis.get_ticklabels()[9].set_color('red')
    closeYAxis.get_ticklabels()[10].set_color('red')


def _draw_intraday_lines(ax, dfLines, baseLine=None,
                         majorList=(1, 60, 120, 180, 240), minorList=(30, 90, 150, 210),
                         colors=const.COLORS, markers=const.MARKERS, verticalExpansionRatio=0.0, **kwargs):
    if baseLine is None:
        i = 0
        for lineName in dfLines.columns:
            if lineName == 'DATETIME' or lineName == 'CODE' \
                    or lineName == 'majorGridCol' or lineName == 'minorGridCol':
                continue
            else:
                ax.plot(range(241), dfLines[lineName].values, color=colors[i],marker=markers[i],**kwargs)
                i += 1
    elif isinstance(baseLine, float) or isinstance(baseLine, int):
        ax.plot(range(241), [baseLine,] * len(dfLines.index), color='red', lw=2)
        i = 1
        for lineName in dfLines.columns:
            if lineName == 'DATETIME' or lineName == 'CODE' \
                    or lineName == 'majorGridCol' or lineName == 'minorGridCol':
                continue
            else:
                ax.plot(range(241), dfLines[lineName].values, color=colors[i], marker=markers[i], **kwargs)
                i += 1
    elif isinstance(baseLine, list) and isinstance(baseLine[0],float) or isinstance(baseLine[0],int):
        for bline in baseLine:
            ax.plot(range(241), [bline,]*len(dfLines.index),color='red', lw=2)
        i = len(baseLine)
        for lineName in dfLines.columns:
            if lineName == 'DATETIME' or lineName == 'CODE' \
                    or lineName == 'majorGridCol' or lineName == 'minorGridCol':
                continue
            else:
                ax.plot(range(241), dfLines[lineName].values, color=colors[i],marker=markers[i],**kwargs)
                i += 1
    else:
        raise ValueError('baseLine必须是float或者int！')

    # x轴格式的设置
    # x轴的显示范围
    ax.set_xlim(0 - 0.5, len(dfLines.index) - 0.5)

    # x轴的tick
    closeXMajorLocator = FixedLocator(np.array(majorList))
    closeXMinorLocator = FixedLocator(np.array(minorList))

    closeXAxis = ax.get_xaxis()
    closeXAxis.set_major_locator(closeXMajorLocator)
    closeXAxis.set_minor_locator(closeXMinorLocator)

    closeXAxis.grid(True, 'major', color='red', linestyle='solid', linewidth=0.5, zorder=1)
    closeXAxis.grid(True, 'minor', color='red', linestyle='dotted', linewidth=0.5, zorder=1)

    # x轴的ticklabel
    majorFormatter = FixedFormatter(['09:30','10:30','13:00','14:00','15:00',])
    closeXAxis.set_major_formatter(majorFormatter)

    closeXAxis.get_ticklabels()[0].set_ha('left')
    closeXAxis.get_ticklabels()[-1].set_ha('right')

    # 调整y轴的显示范围
    if isinstance(dfLines, pd.DataFrame):
        maxNum = dfLines.max().max()
        minNum = dfLines.min().min()
    elif isinstance(dfLines, pd.Series):
        maxNum = dfLines.max()
        minNum = dfLines.min()


    # 如果有baseline，就要考虑在内
    if baseLine is None:
        pass
    elif isinstance(baseLine, int) or isinstance(baseLine, float):
        distance = max(abs(maxNum - baseLine), abs(baseLine - minNum))
        maxNum = baseLine + distance
        minNum = baseLine - distance
    elif isinstance(baseLine, list) and (isinstance(baseLine[0], int) or isinstance(baseLine[0], float)):
        maxNum = max(maxNum, max(baseLine))
        minNum = min(minNum, min(baseLine))

    # 设置y轴
    ax.set_ylim(ymin=minNum * (1-verticalExpansionRatio), ymax=maxNum * (1+verticalExpansionRatio))

    # 设置y的locator
    yLocator = FixedLocator(np.arange(minNum * (1-verticalExpansionRatio),
                                      maxNum * (1+verticalExpansionRatio),
                                      (maxNum * (1+verticalExpansionRatio) - (minNum * (1-verticalExpansionRatio))) / 3
                                      )
                            )
    yAxis = ax.get_yaxis()
    yAxis.set_major_locator(yLocator)
    yAxis.grid(True, 'major', color='red', linestyle='dotted', linewidth=0.5, zorder=1)

    yAxis.set_ticks_position('right')
    yFormatter = FuncFormatter(formatter_func)
    yAxis.set_major_formatter(yFormatter)

    yAxis.get_ticklabels()[0].set_va('bottom')
    yAxis.get_ticklabels()[-1].set_va('top')


def draw_month_analysis(ax, yearDf, colName='期间收益率'):
    dfPlot = yearDf.reset_index()
    rects1 = ax.bar(dfPlot[dfPlot[colName] > 0].index, dfPlot.loc[dfPlot[colName] > 0, colName], color='red')
    rects2 = ax.bar(dfPlot[dfPlot[colName] < 0].index, dfPlot.loc[dfPlot[colName] < 0, colName], color='green')
    ax.set_xlim(0, len(dfPlot.index))
    ax.plot(dfPlot.index, [0, ] * len(dfPlot.index), color='red', zorder=2)

    # 设置x轴显示标签的步骤的
    # 第一步：获取x轴
    xAxis = ax.get_xaxis()
    # 第二步：设置locator，这个可以加一个grid
    xMajorLocator = FixedLocator(range(0, len(dfPlot.index), 4))
    xAxis.set_major_locator(xMajorLocator)
    xAxis.grid(True, 'major', color='red', linestyle='dotted', linewidth=0.5, zorder=1)

    # 第三步，设置标签的显示，FuncFormatter
    def date_formatter_func_major_year_month(num, pos=None):
        return dt.datetime(dfPlot.at[num, 'year'], dfPlot.at[num, 'month'], 1).strftime('%Y-%m')

    majorFormatter = FuncFormatter(date_formatter_func_major_year_month)
    xAxis.set_major_formatter(majorFormatter)

    # 第四步，设置format_xdata

    def date_xdata_formatter(num):
        return dt.datetime(dfPlot.at[int(round(num)), 'year'], dfPlot.at[int(round(num)), 'month'], 1).strftime('%Y-%m')

    ax.format_xdata = date_xdata_formatter

    _autolabel(ax, rects1, 1, color='white')
    _autolabel(ax, rects2, -1, color='white')


def _draw_x_ticklable_grid(ax, df,
                           majorCol, majorFormat, majorGridStyle='solid',
                           minorCol=None, minorFormat=None, minorGridStyle='dotted'):
    '''
    画x轴全部grid和ticklabel的函数。不把ticklabel的颜色放到这里，原因是有些情况可能不需要显示ticklabel
    :param ax:
    :param df:以int为index，还要有列majorCol，minorCol具体就是主grid和副grid对应的列名，str
    :param majorCol:str，主ticklabel的列名，例如：'majorGridCol'
    :param majorFormat:majorFormat是主标签的格式，例如：'%y/%m'。如果是None的话则不画ticklable
    :param majorGridStyle:str，majorGridStyle主grid的样式，默认是'solid'
    :param minorCol:次ticklabel的列名，例如：'minorGridCol'
    :param minorFormat:
    :param minorGridStyle:
    :return:
    '''
    xAxis = ax.get_xaxis()
    try:
        xMajorLocator = FixedLocator(np.array(df.index[df[majorCol] == True]))
    except KeyError:
        return None
    else:
        # x轴的tick
        xAxis.set_major_locator(xMajorLocator)
        xAxis.grid(True, 'major', color='red', linestyle=majorGridStyle, linewidth=0.5, zorder=1)

        # x轴的ticklabel
        def date_formatter_func_major(num, pos=None):
            return df['DATETIME'][num].strftime(majorFormat)

        majorFormatter = FuncFormatter(date_formatter_func_major)
        xAxis.set_major_formatter(majorFormatter)

        # 如果有次grid的话
        try:
            xMinorLocator = FixedLocator(np.array(df.index[df[minorCol] == True]))
        except:
            pass
        else:
            xAxis.set_minor_locator(xMinorLocator)
            xAxis.grid(True, 'minor', color='red', linestyle=minorGridStyle, linewidth=0.5, zorder=1)
            if minorFormat is None:
                pass
            else:
                def date_formatter_func_minor(num, pos=None):
                    return df['DATETIME'][num].strftime(minorFormat)

                minorFormatter = FuncFormatter(date_formatter_func_minor)
                xAxis.set_minor_formatter(minorFormatter)


def _date_format_xdata(ax, df, format='%Y-%m-%d'):
    '''
    这是设置在左下角显示的格式的函数
    :param ax:
    :param df:df的index是int，必须有DATETIME这一列
    :param format: '%Y-%m-%d'，显示的格式。
    :return:
    '''
    def date_formatter_func_major_1(num):
        return df['DATETIME'].iat[int(round(num))].strftime(format)
    ax.format_xdata = date_formatter_func_major_1


def _draw_y_label_grid(ax, minNum, maxNum, verticalExpansionRatio, gridNum, linestyle='dotted'):
    # 设置y的locator
    try:
        yLocator = FixedLocator(np.arange(round(minNum, 0) * (1-verticalExpansionRatio),
                                          round(maxNum, 0) * (1+verticalExpansionRatio),
                                          round(
                                              (round(maxNum, 0) * (1+verticalExpansionRatio) - (round(minNum, 0) * (1-verticalExpansionRatio))) / gridNum, 1
                                               )
                                          )
                                )
    except:
        yLocator = FixedLocator([0.0,1.0])
    yAxis = ax.get_yaxis()
    yAxis.set_major_locator(yLocator)
    yAxis.grid(True, 'major', color='red', linestyle=linestyle, linewidth=0.5, zorder=1)


def _set_display_range(ax, df, startNum, endNum, if_vol=False,
                       baseLine=None, align='center', verticalExpansionRatio=0.5,
                       formatter_func=_price_formatter_func):
    '''
    这个函数是设置x轴的显示范围，和y轴的显示范围，以及y轴的grid，ticklabel，ydata的format
    :param ax:
    :param df:df是指标线的df，含且仅含有显示的数据，index是数字
    :param startNum: startNum, endNum,x轴的范围
    :param endNum:
    :param if_vol:
    :param baseLine:该显示有没有基线
    :param align:
    :param verticalExpansionRatio:
    :param formatter_func:
    :return:
    '''
    if align == 'left':
        ax.set_xlim(startNum - 0.01, endNum - 0.01)
    elif align == 'center':
        ax.set_xlim(startNum - 0.5, endNum - 0.5)

    dfNeed = df.loc[startNum:endNum-1]
    if dfNeed.dropna().empty:
        return None
    # 设置y轴
    # 找到最大最小值
    if isinstance(dfNeed, pd.DataFrame):
        maxNum = dfNeed.max().max()
        minNum = dfNeed.min().min()
    elif isinstance(dfNeed, pd.Series):
        maxNum = dfNeed.max()
        minNum = dfNeed.min()

    if if_vol:
        minNum = 0
    else:
        pass

    # 如果有baseline，就要考虑在内
    if baseLine is None:
        pass
    elif isinstance(baseLine, int) or isinstance(baseLine, float):
        distance = max(abs(maxNum - baseLine), abs(baseLine - minNum))
        maxNum = baseLine + distance
        minNum = baseLine - distance
    elif isinstance(baseLine, list) and (isinstance(baseLine[0], int) or isinstance(baseLine[0], float)):
        maxNum = max(maxNum, max(baseLine))
        minNum = min(minNum, min(baseLine))

    # 设置y轴
    ax.set_ylim(ymin=minNum * (1-verticalExpansionRatio), ymax=maxNum * (1+verticalExpansionRatio))

    # 设置y的locator
    try:
        yLocator = FixedLocator(np.arange(round(minNum, 0) * (1-verticalExpansionRatio),
                                          round(maxNum, 0) * (1+verticalExpansionRatio),
                                          round(
                                              (round(maxNum, 0) * (1+verticalExpansionRatio) - (round(minNum, 0) * (1-verticalExpansionRatio))) / 3, 0
                                               )
                                          )
                                )
    except ZeroDivisionError:
        yLocator = FixedLocator([0.0,1.0])
    yAxis = ax.get_yaxis()
    yAxis.set_major_locator(yLocator)
    yAxis.grid(True, 'major', color='red', linestyle='dotted', linewidth=0.5, zorder=1)

    yAxis.set_ticks_position('right')
    yFormatter = FuncFormatter(formatter_func)
    yAxis.set_major_formatter(yFormatter)

    yAxis.get_ticklabels()[0].set_va('bottom')
    yAxis.get_ticklabels()[-1].set_va('top')

    return None


def _keyUp_regulate(startNum, endNum, showLen, pickedX, lenDownLt=50, step=20):
    '''
    这个函数对按键·↑后start，end，showlen的关系进行处理。显示范围变小，显示的K线条数变少，每根K线宽度变大。
    :param startNum:
    :param endNum:
    :param showLen:
    :param pickedX:
    :param lenDownLt:
    :param step: 每次按键增加的显示范围
    :return: startNum, endNum, showLen, 是否需要更新
    '''
    if pickedX is None: # 没有选定K线的情况
        if showLen <= lenDownLt:
            print('不需要更新！')
            return startNum, endNum, showLen, False  # showlen,start,end都没有变化
        elif showLen < step + lenDownLt:
            startNum = endNum - lenDownLt
            showLen = lenDownLt
            # showlen变成50,end不变，start变成end-50
        else:
            startNum = startNum + step
            showLen = showLen - step
        return startNum,endNum,showLen,True
    else: # 有选定K线的情况
        if showLen <= lenDownLt:
            print(u'不需要更新！')
            return startNum, endNum, showLen, False
        elif showLen >= step + lenDownLt:
            startNum = startNum + int(round((pickedX - startNum) / showLen * step))
            showLen = showLen - step
            endNum = startNum + showLen
        else:
            startNum = startNum + int(round((pickedX - startNum) / showLen * (showLen - lenDownLt)))
            showLen = lenDownLt
            endNum = startNum + showLen
        return startNum, endNum, showLen, True


def _keyDown_regulate(startNum, endNum, showLen, pickedX, totalLenDay, lenUpLt=300, step=20):
    '''
    这个函数对按键·↓后start，end，showlen的关系进行处理。显示范围变大，显示的K线条数变多，每根K线宽度变小。
    :param startNum:
    :param endNum:
    :param showLen:
    :param pickedX:
    :param totalLenDay: 数据的总长度
    :param lenUpLt:
    :param step:
    :return: startNum, endNum, showLen, 是否需要更新
    '''
    if pickedX is None:  # 没有选定K线的情况
        # 这个是以右边为轴
        # 如果已经超过300，或者开始那个为0，则不更新
        if showLen >= lenUpLt or startNum == 0:
            print(u'不需要更新图！')
            return startNum,endNum,showLen,False
        elif showLen <= lenUpLt - step and startNum - step >= 0:
            startNum = startNum - step
            showLen = showLen + step
            # end不变
        elif showLen <= lenUpLt - step and startNum - step < 0:
            startNum = 0
            showLen = endNum - startNum
        elif showLen > lenUpLt - step and startNum - step >= 0:
            showLen = lenUpLt
            startNum = endNum - lenUpLt
        elif showLen > lenUpLt - step and startNum - step < 0:
            startNum = 0
            showLen = endNum - startNum
        return startNum,endNum,showLen,True
    else:  # 有选定K线的情况
        if showLen >= lenUpLt:
            print(u'不需要更新！')
            return startNum,endNum,showLen,True
        elif showLen <= lenUpLt - step:
            if startNum - int(round((pickedX - startNum) / showLen * step)) < 0:
                startNum = 0
                showLen = showLen + step
                endNum = startNum + showLen
            elif endNum + int(round((endNum - pickedX) / showLen * step)) > totalLenDay + 1:
                endNum = totalLenDay + 1
                showLen = showLen + step
                startNum = endNum - showLen
            else:
                startNum = startNum - int(round((pickedX - startNum) / showLen * step))
                showLen = showLen + step
                endNum = startNum + showLen
        elif showLen > lenUpLt - step:
            if startNum - int(round((pickedX - startNum) / showLen * (lenUpLt - showLen))) < 0:
                startNum = 0
                showLen = lenUpLt
                endNum = startNum + showLen
            elif endNum + int(round((endNum - pickedX) / showLen * (lenUpLt - showLen))) > totalLenDay + 1:
                endNum = totalLenDay + 1
                showLen = lenUpLt
                startNum = endNum - showLen
            else:
                startNum = startNum - int(round((pickedX - startNum) / showLen * (lenUpLt - showLen)))
                showLen = lenUpLt
                endNum = startNum + showLen
        return startNum, endNum, showLen, True


def _num_shift(startNum, endNum, showLen, pickedX, shiftUnit, totalLenDay):
    '''
    这是个处理按键·←、按键·→或拖动后start，end，showlen的关系
    :param startNum:
    :param endNum:
    :param showLen:
    :param pickedX:
    :param shiftUnit: 移动的数量
    :param totalLenDay: 数据的总长度
    :return:
    '''
    if shiftUnit == 0:
        # print('不需要移动！')
        return startNum,endNum,showLen,False,False
    elif shiftUnit < 0:  # 左移，startNum是减
        if pickedX is None:
            if startNum == 0:
                print(u'无法左移，不需更新！')
                return startNum,endNum,showLen,False,False
            elif startNum + shiftUnit < 0:
                actualShiftUnit = startNum
                startNum = 0
                endNum = endNum - actualShiftUnit
            else:
                startNum = startNum + shiftUnit
                endNum = endNum + shiftUnit
                # 长度不变

            return startNum, endNum, showLen, True, False
        else:
            if startNum == 0:
                print(u'无法左移！')
                return startNum, endNum, showLen, False, False
            elif startNum + shiftUnit < 0:
                actualShiftUnit = startNum  # 这是变量恒为正
                startNum = 0

                # 选择的k线移动后超出范围的情况
                if endNum == pickedX + actualShiftUnit:
                    endNum = endNum - actualShiftUnit
                    return startNum, endNum, showLen, True, True

                else:
                    # 这个情况就不用去掉选择了
                    endNum = endNum - actualShiftUnit
                    return startNum, endNum, showLen, True, False

            else:
                # 移动后超出范围的情况
                if pickedX - shiftUnit >= endNum:
                    startNum = startNum + shiftUnit
                    endNum = endNum + shiftUnit
                    return startNum, endNum, showLen, True, True

                else:
                    # 未超出的情况
                    startNum = startNum + shiftUnit
                    endNum = endNum + shiftUnit
                    return startNum, endNum, showLen, True, False
    elif shiftUnit > 0:  # 右移，startNum是加
        if pickedX is None:
            if endNum == totalLenDay + 1:
                print(u'无法右移！')
                return startNum, endNum, showLen, False, False
            elif endNum + shiftUnit > totalLenDay + 1:
                actualShiftUnit = totalLenDay + 1 - endNum
                endNum = totalLenDay + 1
                startNum = startNum + actualShiftUnit
            else:
                startNum = startNum + shiftUnit
                endNum = endNum + shiftUnit
                # 长度不变
            return startNum, endNum, showLen, True, False

        else:
            if endNum == totalLenDay + 1:
                print(u'无法右移！')
                return startNum, endNum, showLen, False, False

            elif endNum + shiftUnit > totalLenDay + 1:
                actualShiftUnit = totalLenDay + 1 - endNum  # 这是变量恒为正
                endNum = totalLenDay + 1

                # 选择的k线移动后超出范围的情况
                if pickedX == startNum + actualShiftUnit - 1:
                    startNum = startNum + actualShiftUnit
                    return startNum, endNum, showLen, True, True

                else:
                    # 这个情况就不用去掉选择了
                    startNum = startNum + actualShiftUnit
                    return startNum, endNum, showLen, True, False

            else:
                # 移动后超出范围的情况
                if pickedX - shiftUnit < startNum:
                    startNum = startNum + shiftUnit
                    endNum = endNum + shiftUnit
                    return startNum, endNum, showLen, True, True

                else:
                    # 未超出的情况
                    startNum = startNum + shiftUnit
                    endNum = endNum + shiftUnit
                    return startNum, endNum, showLen, True, False


#################### 显示文字 ###########################################################################################
def rainbow_text(x, y, strings, colors, ax=None, split=' ', **kw):
    '''
    这个是在ax中对不同字显示不同颜色的函数。这个函数实在matplotlib的官网example中弄下来的。
    :param x:float，位置
    :param y:float，位置
    :param strings:list，需要显示的字符串的list
    :param colors:list，字符串对应的颜色的list
    :param ax:
    :param split: str，不同字符串之间的分隔符。
    :param kw:
    :return:
    '''
    if ax is None:
        ax = plt.gca()
    t = ax.transData
    canvas = ax.figure.canvas

    # horizontal version
    for s, c in zip(strings, colors):
        text = ax.text(x, y, s + split, color=c, transform=t, **kw)
        text.draw(canvas.get_renderer())
        ex = text.get_window_extent()
        t = transforms.offset_copy(text._transform, x=ex.width, units='dots')


def _draw_text_candle(axTop, pickedX, df, fontProp,
                      textX=0.01, textY=0.15,dtFormat='%Y-%m-%d', **kw):
    '''
    这是在axTop中显示选定的K线的开高低收量额等信息的函数。
    :param axTop: 存放文字行情的ax
    :param pickedX:int. 当前选定的k线
    :param df: 生成这个bar图所根据的数据，必须包含所选k线，且index是range，没有经过截取。
    :param fontProp: 显示中文的字体和大小
    :param textX: =0.01，在ax的相对X位置
    :param textY: =0.15,在ax的相对Y位置
    :param kw:
    :return:
    '''
    delta, periodStr, code, chineseName = _detect_period_and_code(df)
    axTop.texts = []
    strings = [df.iloc[pickedX, df.columns.get_loc('DATETIME')].strftime(dtFormat),
               code,
               chineseName,
               periodStr,
               '开:%0.2f' % df.iloc[pickedX, df.columns.get_loc('OPEN')],
               '高:%0.2f' % df.iloc[pickedX, df.columns.get_loc('HIGH')],
               '低:%0.2f' % df.iloc[pickedX, df.columns.get_loc('LOW')],
               '收:%0.2f' % df.iloc[pickedX, df.columns.get_loc('CLOSE')],
               '量:%0.2f万' % (df.iloc[pickedX, df.columns.get_loc('VOLUME')] / 10000),
               '额:%0.2f亿' % (df.iloc[pickedX, df.columns.get_loc('AMOUNT')] / 100000),
               '涨跌幅:%0.2f%%' % (df.iloc[pickedX, df.columns.get_loc('PCTCHANGE')]),]

    colors = ['white'] * 11

    colors[5] = 'red'
    colors[6] = 'lawngreen'
    colors[7] = 'yellow'

    rainbow_text(textX, textY, strings, colors, ax=axTop, fontproperties=fontProp, ha='left', **kw)


def _draw_text_lines(axTop, pickedX, df, fontProp, showCol='all',
                     colors=const.COLORS,
                     textX=0.01, textY=0.15, dtFormat='%Y-%m-%d', **kw):
    '''
    这个是在axTop中显示当前选定的指标的数值的函数
    :param axTop: 存放文字行情的ax
    :param pickedX: 当前选定的k线
    :param df: 生成这个bar图所根据的数据，必须包含所选k线，且index是range，没有经过截取。
    :param fontProp: 显示中文的字体和大小
    :param colors: 各个指标的颜色，必须跟线的颜色对应，一般选默认
    :param textX: =0.01，在ax的相对X位置
    :param textY: =0.15,在ax的相对Y位置
    :param kw:
    :return:
    '''
    axTop.texts = []
    strings = [df.iloc[pickedX, df.columns.get_loc('DATETIME')].strftime(dtFormat), ]
    colorShow = ['white',]
    i = 0
    if showCol == 'all':
        for lineName in df.columns:
            if lineName == 'DATETIME' or lineName == 'CODE' \
                    or lineName == 'majorGridCol' or lineName == 'minorGridCol':
                continue
            else:
                strings.append(lineName)
                strings.append('%0.2f' % df.iloc[pickedX, df.columns.get_loc(lineName)])
                colorShow.append(colors[i])
                colorShow.append(colors[i])
                i += 1
    else:
        for lineName in showCol:
            strings.append(lineName)
            strings.append('%0.2f' % df.iloc[pickedX, df.columns.get_loc(lineName)])
            colorShow.append(colors[i])
            colorShow.append(colors[i])
            i += 1

    rainbow_text(textX, textY, strings, colorShow, ax=axTop, fontproperties=fontProp, ha='left', **kw)


def draw_series_on_axes(ax, ser, color, size=13):
    '''
    这个函数是在ax里面显示series的内容，series必须全部都是str
    :param ax:
    :param ser:
    :param color:
    :param size:
    :return:
    '''
    fontProp = FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=size)
    itemsNum = len(ser)

    for yI in range(itemsNum):

        rainbow_text(0.00, (100 - 100 / itemsNum * (yI + 1)) / 100 + 0.01,
                     [ser.index[yI],ser[yI]],
                     [color] * 2,
                     ax=ax, split='  ',
                     fontproperties=fontProp, ha='left')
    ax.set_ylim(0,100)


#################### 对外使用的类 #######################################################################################
class Continuous(object):
    '''
    这个是画图模块的不同Block公用的一些函数。
    1、加同步
    2、鼠标选择，拖拽和按键的回调函数，由于不同Block之间的联动需要每个子类的回调函数一致。所以必须统一在父类内进行管理。
    需要特别注意的是父类中没有定义大部分属性和方法，新建子类的时候需要将所有的这些属性和方法都定义一遍。
    1、_draw_text_selectLine()、_set_display_range()
    2、ax，fig、synchronList、pickedX、dragEvent、totalLen
    '''
    def add_synchron(self, *instance):
        self.synchronList.extend(list(instance))


    def select_date(self,dateStr):
        try:
            _try = list(self.df['DATETIME']).index(np.datetime64(dateStr))
        except:
            print('所选日期非交易日，请修改！')
        else:
            if _try < self.endNum and _try >= self.startNum:
                print('目标日期已在显示范围内！')
            else:
                self.pickedX = _try
                self.startNum = self.pickedX - self.showLen // 2
                self.endNum = self.startNum + self.showLen

                if self.startNum < 0:
                    self.startNum = 0
                    self.endNum = self.showLen
                    # self.pickedX = self.showLen // 2
                elif self.endNum > self.totalLen:
                    self.endNum = self.totalLen
                    self.startNum = self.endNum - self.showLen
                    # self.pickedX = self.endNum - self.showLen // 2
                else:
                    pass

                # 画日线的图
                self._set_display_range()
                self._draw_text_selectLine(dtFormat=self.dtFormat)
                self.fig.canvas.draw_idle()

                if self.synchronList == []:
                    pass
                else:
                    for instance in self.synchronList:
                        instance.pickedX = self.pickedX
                        instance.startNum = self.startNum
                        instance.endNum = self.endNum
                        instance._set_display_range()
                        instance._draw_text_selectLine(dtFormat=instance.dtFormat)
                        instance.fig.canvas.draw_idle()


    def attach_buysell_sign(self, dfNeed, align='center', mode='longshort'):
        dfNeedIn = dfNeed.reset_index(drop=True)

        pxCol = dfNeed.columns[0]
        signCol = dfNeed.columns[1]

        if align == 'center':
            alignPlus = 0.5
        elif align == 'left':
            alignPlus = 0
        elif align == 'right':
            alignPlus = 1

        dfNeed2 = dfNeedIn[dfNeedIn[signCol] != 0]
        if mode == 'long':
            if dfNeed2.iat[-1,dfNeed2.columns.get_loc(signCol)] == -1:
                for showIndex in dfNeed2[dfNeed2[signCol] > 0].index:
                    nextIndex = dfNeed2.index[dfNeed2.index.get_loc(showIndex) + 1]
                    self.ax.plot([showIndex + alignPlus, nextIndex + alignPlus],
                                  [dfNeed2.at[showIndex, pxCol], dfNeed2.at[nextIndex, pxCol]], 'ro-')
            elif dfNeed2.iat[-1,dfNeed2.columns.get_loc(signCol)] == 1:
                for showIndex in dfNeed2[dfNeed2[signCol] > 0].index[:-1]:
                    nextIndex = dfNeed2.index[dfNeed2.index.get_loc(showIndex) + 1]
                    self.ax.plot([showIndex + alignPlus, nextIndex + alignPlus],
                                  [dfNeed2.at[showIndex, pxCol], dfNeed2.at[nextIndex, pxCol]], 'ro-')
        elif mode == 'short':
            if dfNeed2.iat[-1, dfNeed2.columns.get_loc(signCol)] == 1:
                for showIndex in dfNeed2[dfNeed2[signCol] < 0].index:
                    nextIndex = dfNeed2.index[dfNeed2.index.get_loc(showIndex) + 1]
                    self.ax.plot([showIndex + alignPlus, nextIndex + alignPlus],
                                  [dfNeed2.at[showIndex, pxCol], dfNeed2.at[nextIndex, pxCol]], 'go-')
            elif dfNeed2.iat[-1, dfNeed2.columns.get_loc(signCol)] == 1:
                for showIndex in dfNeed2[dfNeed2[signCol] < 0].index[:-1]:
                    nextIndex = dfNeed2.index[dfNeed2.index.get_loc(showIndex) + 1]
                    self.ax.plot([showIndex + alignPlus, nextIndex + alignPlus],
                                  [dfNeed2.at[showIndex, pxCol], dfNeed2.at[nextIndex, pxCol]], 'go-')
        elif mode == 'longshort':
            for showIndex in dfNeed2.index[:-1]:
                if dfNeed2.at[showIndex, signCol] > 0:
                    nextIndex = dfNeed2.index[dfNeed2.index.get_loc(showIndex) + 1]
                    self.ax.plot([showIndex + alignPlus, nextIndex + alignPlus],
                                  [dfNeed2.at[showIndex, pxCol], dfNeed2.at[nextIndex, pxCol]], 'ro-')
                else:
                    nextIndex = dfNeed2.index[dfNeed2.index.get_loc(showIndex) + 1]
                    self.ax.plot([showIndex + alignPlus, nextIndex + alignPlus],
                                  [dfNeed2.at[showIndex, pxCol], dfNeed2.at[nextIndex, pxCol]], 'go-')


    def attach_wave_sign(self,dfNeed, align='center',color='yellow',name='default',**kwargs):
        try:
            self.waveSignDict
        except AttributeError:
            self.waveSignDict = {}

        dfNeedIn = dfNeed.reset_index(drop=True)
        pxCol = dfNeed.columns[0]
        signCol = dfNeed.columns[1]
        dfNeed2 = dfNeedIn[dfNeedIn[signCol] != 0]

        if align == 'center':
            alignPlus = 0.5
        elif align == 'left':
            alignPlus = 0
        elif align == 'right':
            alignPlus = 1

        if name == 'default':
            self.waveSignDict[len(self.waveSignDict) + 1] \
                = self.ax.plot(dfNeed2.index + alignPlus, dfNeed2[pxCol], color=color,**kwargs)
        else:
            self.waveSignDict[name] = self.ax.plot(dfNeed2.index + alignPlus, dfNeed2[pxCol], color=color,**kwargs)


    def show_wave_sign(self):
        print('目前的wave sign有以下几个：')
        print(self.waveSignDict.keys())


    def detach_wave_sign(self,name):
        self.ax.lines.remove(self.waveSignDict[name][0])
        del self.waveSignDict[name]
        self.ax.figure.canvas.draw_idle()


    def _onpick(self, event):
        if isinstance(event.artist, Rectangle) and event.artist.axes == self.ax:
            self.pickedX = int(round(event.artist.get_x(), 0))
            self._draw_text_selectLine(dtFormat=self.dtFormat)

            if self.synchronList == []:
                pass
            else:
                for instance in self.synchronList:
                    instance.pickedX = self.pickedX
                    instance._draw_text_selectLine(dtFormat=instance.dtFormat)
                    instance.fig.canvas.draw_idle()

            self.fig.canvas.draw_idle()


    def _press(self, event):
        if event.inaxes == self.ax:
            if event.key == 'up':
                self.startNum, self.endNum, self.showLen, ifChg \
                    = _keyUp_regulate(startNum=self.startNum, endNum=self.endNum, showLen=self.showLen,
                                      pickedX=self.pickedX, lenDownLt=self.lendownlt, step=self.step)
                if ifChg:
                    self._set_display_range()
                    self.fig.canvas.draw_idle()
                else:
                    pass

                if self.synchronList == []:
                    pass
                else:
                    for instance in self.synchronList:
                        instance.startNum, instance.endNum, instance.showLen = self.startNum, self.endNum, self.showLen
                        if ifChg:
                            instance._set_display_range()
                            instance.fig.canvas.draw_idle()

            elif event.key == 'down':
                self.startNum, self.endNum, self.showLen, ifChg \
                    = _keyDown_regulate(startNum=self.startNum, endNum=self.endNum, showLen=self.showLen,
                                        pickedX=self.pickedX, totalLenDay=self.totalLen, lenUpLt=self.lenuplt, step=self.step)
                if ifChg:
                    self._set_display_range()
                    self.fig.canvas.draw_idle()

                if self.synchronList == []:
                    pass
                else:
                    for instance in self.synchronList:
                        instance.startNum, instance.endNum, instance.showLen = self.startNum, self.endNum, self.showLen
                        if ifChg:
                            instance._set_display_range()
                            instance.fig.canvas.draw_idle()

            elif event.key == 'left':
                if self.pickedX is None:
                    self._num_shift(shiftUnit=-1)
                else:
                    if self.pickedX == 0:
                        print('已经是最左端了，不需要更新！')
                    else:
                        self.pickedX = self.pickedX - 1
                        self._draw_text_selectLine(dtFormat=self.dtFormat)
                        self.fig.canvas.draw_idle()

                if self.synchronList == []:
                    pass
                else:
                    for instance in self.synchronList:
                        instance.pickedX = self.pickedX
                        instance._draw_text_selectLine(dtFormat=instance.dtFormat)
                        instance.fig.canvas.draw_idle()

            elif event.key == 'right':
                if self.pickedX is None:
                    self._num_shift(shiftUnit=1)
                else:
                    if self.pickedX == self.endNum - 1:
                        print('已经是最右端了，不需要更新！')
                    else:
                        self.pickedX = self.pickedX + 1
                        self._draw_text_selectLine(dtFormat=self.dtFormat)
                        self.fig.canvas.draw_idle()

                if self.synchronList == []:
                    pass
                else:
                    for instance in self.synchronList:
                        instance.pickedX = self.pickedX
                        instance._draw_text_selectLine(dtFormat=instance.dtFormat)
                        instance.fig.canvas.draw_idle()

        else:
            pass


    def _onpress(self, event):
        if event.inaxes == self.ax:
            self.dragEvent = event
        else:
            return


    def _onrelease(self, event):
        if self.dragEvent is None:
            return None
        elif event.inaxes == self.dragEvent.inaxes:
            self.dx = int(round(event.xdata - self.dragEvent.xdata,0))
            self._num_shift(shiftUnit=-self.dx)
            if self.synchronList == []:
                pass
            else:
                for instance in self.synchronList:
                    instance._num_shift(shiftUnit=-self.dx)
        else:
            self.dragEvent = None
        self.fig.canvas.draw_idle()


    def _num_shift(self, shiftUnit):
        self.startNum, self.endNum, self.showLen, ifChg, ifExceeded \
            = _num_shift(startNum=self.startNum, endNum=self.endNum, showLen=self.showLen,
                         pickedX=self.pickedX,
                         shiftUnit=shiftUnit, totalLenDay=self.totalLen)
        if ifChg:
            self._set_display_range()


class Candle(Continuous):

    def __init__(self, ax, axTop, df,
                 startNum=0, endNum=None, length=80,
                 lenuplt=300, lendownlt=50, step=10,
                 majorGrid='month', majorFormat='%Y-%m', majorGridStyle='solid',
                 minorGrid='week', minorFormat=None, minorGridStyle='dotted',
                 show_x_tick=True,
                 dtFormat='%Y-%m-%d'):
        '''

        :param ax:
        :param axTop:
        :param df:
        :param startNum:
        :param endNum:
        :param length:
        :param lenuplt:
        :param lendownlt:
        :param step:
        :param majorGrid:
        :param majorFormat:
        :param majorGridStyle:
        :param minorGrid:
        :param minorFormat:
        :param minorGridStyle:
        :param show_x_tick:
        :param dtFormat:
        '''
        self.fontProp = FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=10)

        self.ax = ax
        self.axTop = axTop
        self.fig = self.ax.figure
        self.df = df.reset_index()
        if 'PCTCHANGE' in self.df.columns:
            pass
        else:
            self.df['PCTCHANGE'] = (self.df['CLOSE'] / self.df['CLOSE'].shift(1) - 1) * 100
            self.df.iat[0, self.df.columns.get_loc('PCTCHANGE')] = 0

        self.totalLen = len(self.df.index)
        _gen_grid_col(self.df, majorGrid=majorGrid, minorGrid=minorGrid) # 可以同时能为None，majorGrid为空的话就没有X的grid
        self.startNum,self.endNum,self.showLen \
            = _init_range_nums(startNum=startNum, endNum=endNum, length=length, df=self.df)
        self.delta, self.periodStr,self.code, self.chineseName = _detect_period_and_code(self.df)
        self.lenuplt, self.lendownlt, self.step = (lenuplt if lenuplt < self.totalLen else self.totalLen), lendownlt, step

        self.dtFormat = dtFormat

        self.pickedX = self.startNum
        self.selectionLine = None

        # 画主体
        _draw_candleStick(self.ax, self.df) # 画K线
        _draw_x_ticklable_grid(ax=self.ax, df=self.df,
                               majorCol='majorGridCol', majorFormat=majorFormat, majorGridStyle=majorGridStyle,
                               minorCol='minorGridCol', minorFormat=minorFormat, minorGridStyle=minorGridStyle)
        _date_format_xdata(self.ax, self.df,format=dtFormat)
        self._draw_text_selectLine(dtFormat=self.dtFormat)
        self._set_display_range() # 设置显示范围和Y轴的grid，Y轴的ticklabel

        # 设置ticklabel的显示与否
        if show_x_tick:
            for tickLabel in self.ax.get_xaxis().get_ticklabels():
                tickLabel.set_visible(True)
            self.ax.xaxis.set_tick_params(labelcolor='white')
        else:
            for tickLabel in self.ax.get_xaxis().get_ticklabels():
                tickLabel.set_visible(False)
        self.ax.yaxis.set_tick_params(labelcolor='white')

        self.fig.canvas.draw_idle()

        # mpl交互 ##############################################################################
        # 拖拽的初始设置
        self.dragEvent = None
        # 连接
        self.fig.canvas.mpl_connect('pick_event', self._onpick)
        self.fig.canvas.mpl_connect('key_press_event', self._press)
        self.fig.canvas.mpl_connect('button_press_event', self._onpress)
        self.fig.canvas.mpl_connect('button_release_event', self._onrelease)

        # 同步
        self.synchronList = []


    def _draw_text_selectLine(self, dtFormat='%Y-%m-%d'):
        # 显示文字
        _draw_text_candle(axTop=self.axTop, pickedX=self.pickedX,
                          df=self.df, fontProp=self.fontProp,
                          textX=0.01, textY=0.3,dtFormat=dtFormat)
        # 画选择的的虚线
        if self.selectionLine is None:
            self.selectionLine = self.ax.plot((self.pickedX,) * 2, self.ax.get_ylim(),
                                              color='white', linewidth=1, linestyle='dotted',
                                              zorder=10)[0]
        else:
            self.ax.lines.remove(self.selectionLine)
            self.selectionLine = self.ax.plot((self.pickedX,) * 2, self.ax.get_ylim(),
                                              color='white', linewidth=1, linestyle='dotted',
                                              zorder=10)[0]


    def _set_display_range(self):
        _set_display_range(self.ax, self.df[['OPEN', 'HIGH', 'LOW', 'CLOSE']],
                           self.startNum, self.endNum, verticalExpansionRatio=0.02)


class Lines(Continuous):

    def __init__(self, ax, axTop, dfNeed, baseLine=None,
                 startNum=0, endNum=None, length=80,
                 lenuplt=300, lendownlt=50, step=10,
                 majorGrid='month', majorFormat='%Y-%m', majorGridStyle='dotted',
                 minorGrid=None, minorFormat=None, minorGridStyle='dotted',
                 show_x_tick=True,
                 dtFormat='%Y-%m-%d', ):
        self.fontProp = FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=10)

        self.ax = ax
        self.axTop = axTop
        self.fig = self.ax.figure

        self.columns = list(dfNeed.columns)
        self.df = dfNeed.reset_index()

        self.totalLen = len(self.df.index)
        _gen_grid_col(self.df, majorGrid=majorGrid, minorGrid=minorGrid)
        self.startNum,self.endNum,self.showLen \
            = _init_range_nums(startNum=startNum, endNum=endNum, length=length, df=self.df)
        self.lenuplt, self.lendownlt, self.step = (lenuplt if lenuplt < self.totalLen else self.totalLen), lendownlt, step

        self.dtFormat = dtFormat

        self.pickedX = self.startNum
        self.selectionLine = None

        # 画主体
        _draw_lines(self.ax, self.df, baseLine=baseLine, ms=2, lw=0.3)
        _draw_x_ticklable_grid(ax=self.ax, df=self.df,
                               majorCol='majorGridCol', majorFormat=majorFormat, majorGridStyle=majorGridStyle,
                               minorCol='minorGridCol', minorFormat=minorFormat, minorGridStyle=minorGridStyle)
        _date_format_xdata(self.ax, self.df, format=dtFormat)
        self._draw_text_selectLine(dtFormat=self.dtFormat)
        self._set_display_range()

        # 设置ticklabel的显示与否
        if show_x_tick:
            for tickLabel in self.ax.get_xaxis().get_ticklabels():
                tickLabel.set_visible(True)
            self.ax.xaxis.set_tick_params(labelcolor='white')
        else:
            for tickLabel in self.ax.get_xaxis().get_ticklabels():
                tickLabel.set_visible(False)
        self.ax.yaxis.set_tick_params(labelcolor='white')

        self.fig.canvas.draw_idle()

        # mpl交互 ##############################################################################
        # 拖拽的初始设置
        self.dragEvent = None
        # 连接
        self.fig.canvas.mpl_connect('pick_event', self._onpick)
        self.fig.canvas.mpl_connect('key_press_event', self._press)
        self.fig.canvas.mpl_connect('button_press_event', self._onpress)
        self.fig.canvas.mpl_connect('button_release_event', self._onrelease)

        # 同步
        self.synchronList = []


    def _onrelease(self, event):
        if self.dragEvent is None:
            return None
        elif event.inaxes == self.dragEvent.inaxes:
            self.dx = int(round(event.xdata - self.dragEvent.xdata,0))
            if abs(self.dx) >= 1:
                self._num_shift(shiftUnit=-self.dx)
            else:
                self.pickedX = int(round(event.xdata, 0))
                self._draw_text_selectLine(dtFormat=self.dtFormat)
                self.fig.canvas.draw_idle()

            if self.synchronList == []:
                pass
            else:
                for instance in self.synchronList:
                    if abs(self.dx) >= 1:
                        instance._num_shift(shiftUnit=-self.dx)
                    else:
                        instance.pickedX = int(round(event.xdata, 0))
                        instance._draw_text_selectLine(dtFormat=instance.dtFormat)
                        instance.fig.canvas.draw_idle()
        else:
            self.dragEvent = None
        self.fig.canvas.draw_idle()


    def _draw_text_selectLine(self, dtFormat='%Y-%m-%d'):
        # 显示文字
        _draw_text_lines(axTop=self.axTop, pickedX=self.pickedX,
                         df=self.df, fontProp=self.fontProp,
                         textX=0.01, textY=0.3,
                         dtFormat=dtFormat)
        # 画选择的的虚线
        if self.selectionLine is None:
            self.selectionLine = self.ax.plot((self.pickedX,) * 2, self.ax.get_ylim(),
                                              color='white', linewidth=1, linestyle='dotted',
                                              zorder=10)[0]
        else:
            self.ax.lines.remove(self.selectionLine)
            self.selectionLine = self.ax.plot((self.pickedX,) * 2, self.ax.get_ylim(),
                                              color='white', linewidth=1, linestyle='dotted',
                                              zorder=10)[0]


    def _set_display_range(self):
        _set_display_range(self.ax, self.df[self.columns],
                           self.startNum, self.endNum, verticalExpansionRatio=0.02)


class Volume(Continuous):
    def __init__(self, ax, df,
                 startNum=0, endNum=None, length=80,
                 lenuplt=300, lendownlt=50, step=10,
                 majorGrid='month', majorFormat='%Y-%m', majorGridStyle='dotted',
                 minorGrid='week',minorFormat=None, minorGridStyle='dotted',
                 show_x_tick=True,
                 dtFormat='%Y-%m-%d'):
        '''
        初始化的效果生成一个成交量的柱形图
        :param ax:
        :param df:
        :param startNum:
        :param endNum:
        :param length:
        :param lenuplt:
        :param lendownlt:
        :param step:
        '''
        self.ax = ax
        self.fig = self.ax.figure

        self.df = df.reset_index()

        self.totalLen = len(self.df.index)
        _gen_grid_col(self.df, majorGrid=majorGrid, minorGrid=minorGrid)
        self.startNum,self.endNum,self.showLen \
            = _init_range_nums(startNum=startNum, endNum=endNum, length=length, df=self.df)
        self.lenuplt, self.lendownlt, self.step = (lenuplt if lenuplt < self.totalLen else self.totalLen), lendownlt, step

        self.dtFormat = dtFormat

        self.pickedX = self.startNum
        self.selectionLine = None

        # 画主体
        _draw_volume(ax, self.df)
        _draw_x_ticklable_grid(ax=self.ax, df=self.df,
                               majorCol='majorGridCol', majorFormat=majorFormat, majorGridStyle=majorGridStyle,
                               minorCol='minorGridCol', minorFormat=minorFormat, minorGridStyle=minorGridStyle)
        _date_format_xdata(self.ax, self.df,format=dtFormat)
        self._draw_text_selectLine()
        self._set_display_range()

        # 设置ticklabel的显示与否
        if show_x_tick:
            for tickLabel in self.ax.get_xaxis().get_ticklabels():
                tickLabel.set_visible(True)
            self.ax.xaxis.set_tick_params(labelcolor='white')
        else:
            for tickLabel in self.ax.get_xaxis().get_ticklabels():
                tickLabel.set_visible(False)
        self.ax.yaxis.set_tick_params(labelcolor='white')

        self.fig.canvas.draw_idle()

        # mpl交互 ##############################################################################
        # 拖拽的初始设置
        self.dragEvent = None
        # 连接
        self.fig.canvas.mpl_connect('pick_event', self._onpick)
        self.fig.canvas.mpl_connect('key_press_event', self._press)
        self.fig.canvas.mpl_connect('button_press_event', self._onpress)
        self.fig.canvas.mpl_connect('button_release_event', self._onrelease)

        # 同步
        self.synchronList = []


    def _draw_text_selectLine(self, dtFormat=None):
        # 画选择的的虚线
        if self.selectionLine is None:
            self.selectionLine = self.ax.plot((self.pickedX,)*2,self.ax.get_ylim(),
                                              color='white',linewidth=1,linestyle='dotted',
                                              zorder=10)[0]
        else:
            self.ax.lines.remove(self.selectionLine)
            self.selectionLine = self.ax.plot((self.pickedX,) * 2, self.ax.get_ylim(),
                                              color='white', linewidth=1, linestyle='dotted',
                                              zorder=10)[0]


    def _set_display_range(self):
        _set_display_range(self.ax, self.df[['VOLUME']],
                           self.startNum, self.endNum,if_vol=True,
                           verticalExpansionRatio=0.02,formatter_func=_vol_formatter_func_wan)


class Equity_Curve(object):

    def __init__(self, ax, dfNeed, contrast='000001.SH', period='day',labels=('strategry',), colors=const.COLORS, show_x_tick=True):
        '''
        这个函数的作用是在ax中画equityCurve，有对比，
        :param ax:
        :param dfNeed:
        :param contrast: 对比的基准，默认的上证指数
        :param labels: 输入的equityCurve的名字的list
        :param colors: 各个equityCurve的颜色，默认是采用默认方案。
        '''
        self.ax = ax
        self.df = dfNeed.rename(columns=dict(zip(dfNeed.columns, labels))).reset_index()
        if period == 'day':
            self._baseDF \
                = fetch.index_one(contrast, type='normal',
                                  startTime=self.df['DATETIME'][0].strftime('%Y-%m-%d'),
                                  endTime=self.df['DATETIME'].iat[-1].strftime('%Y-%m-%d'))
        else:
            self._baseDF = fetch.n_min(contrast, n=int(''.join(list(filter(str.isdigit, period)))),
                                       startTime=self.df['DATETIME'][0].strftime('%Y-%m-%d %H:%M:%S'),
                                       endTime=self.df['DATETIME'].iat[-1].strftime('%Y-%m-%d %H:%M:%S')
                                       )
        self._baseDF = self._baseDF.reset_index(level=1).loc[pd.Index(self.df['DATETIME']), ['CODE', 'CLOSE']]
        self._baseDF[contrast] = self._baseDF['CLOSE'] / self._baseDF.iat[0, self._baseDF.columns.get_loc('CLOSE')]
        self._baseDF.reset_index(inplace=True)
        self.df[contrast] = self._baseDF[contrast]
        del self._baseDF

        ax.plot(self.df.index, self.df[contrast], color='white', label=contrast)
        for i, col in enumerate(labels):
            ax.plot(self.df.index, self.df[col], color=colors[i+1], label=labels[i])
        ax.legend(loc='upper left')
        ax.plot(self.df.index, [1, ] * len(self.df.index), color='red')

        ax.set_xlim(self.df.index[0], self.df.index[-1])
        if period == 'day':
            _date_format_xdata(ax, self.df)
        else:
            _date_format_xdata(ax, self.df,format='%Y-%m-%d %H:%M:%S')
        _gen_grid_col(self.df, majorGrid='year')
        _draw_x_ticklable_grid(ax=ax, df=self.df, majorCol='majorGridCol', majorFormat='%Y', majorGridStyle='dotted')
        if show_x_tick:
            ax.xaxis.set_tick_params(labelcolor='white')
        else:
            ax.xaxis.set_visible(False)
        _draw_y_label_grid(ax=ax, minNum=ax.get_ylim()[0], maxNum=ax.get_ylim()[1], verticalExpansionRatio=0.0, gridNum=6)
        # 设置yticklable的颜色和位置
        ax.yaxis.set_ticks_position('right')
        ax.yaxis.set_tick_params(labelcolor='white')


    def add_equity_curve(self, dfNeedAdd, labels=('strategry'),colors=const.COLORS):
        if len(self.df.index) != len(dfNeedAdd.index):
            raise ValueError('添加的equityCurve的长度必须与原来的相等！')
        else:
            for label in labels:
                self.df[label] = dfNeedAdd.rename(columns=dict(zip(dfNeedAdd.columns, labels))).reset_index()[label]
            #画线
            for i, col in enumerate(labels):
                self.ax.plot(self.df.index, self.df[col], color=colors[i + len(self.df.columns) - len(labels)], label=labels[i])
            self.ax.legend(loc='upper left')
            self.ax.figure.canvas.draw_idle()


class Equity_Curve_Assoc(Lines):
    pass


class Intraday(object):


    def add_synchron(self, *instance):
        self.synchronList.extend(list(instance))


    def _press(self, event):
        if event.inaxes == self.ax:

            if event.key == 'left':
                if self.pickedX == 0:
                    print('已经是最左端了，不需要更新！')
                else:
                    self.pickedX = self.pickedX - 1
                    self._draw_text_selectLine()
                    self.fig.canvas.draw_idle()

                if self.synchronList == []:
                    pass
                else:
                    for instance in self.synchronList:
                        instance.pickedX = self.pickedX
                        instance._draw_text_selectLine()
                        instance.fig.canvas.draw_idle()

            elif event.key == 'right':
                if self.pickedX == 240:
                    print('已经是最右端了，不需要更新！')
                else:
                    self.pickedX = self.pickedX + 1
                    self._draw_text_selectLine()
                    self.fig.canvas.draw_idle()

                if self.synchronList == []:
                    pass
                else:
                    for instance in self.synchronList:
                        instance.pickedX = self.pickedX
                        instance._draw_text_selectLine()
                        instance.fig.canvas.draw_idle()

        else:
            pass


    def _onpress(self, event):
        if event.inaxes == self.ax:
            # self.onpressEvent = event
            self.pickedX = int(round(event.xdata,0))
            self._draw_text_selectLine()
            self.fig.canvas.draw_idle()

            if self.synchronList == []:
                pass
            else:
                for instance in self.synchronList:
                    instance.pickedX = self.pickedX
                    instance._draw_text_selectLine()
                    instance.fig.canvas.draw_idle()
        else:
            pass


class Intraday_Close(Intraday):


    def __init__(self, ax, axTop, df1minInOneday, preclose, show_x_tick=True):
        self.ax = ax
        self.axTop = axTop
        self.fig = self.ax.figure

        self.fontProp = FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=10)

        if 'DATETIME' in df1minInOneday.columns:
            self.df = df1minInOneday
        else:
            self.df = df1minInOneday.reset_index()

        self.pickedX = 0
        self.selectionLine = None

        _draw_intraday_close(ax, self.df,preclose)
        _date_format_xdata(self.ax, self.df, format='%Y-%m-%d %H:%M:%S')
        self._draw_text_selectLine()

        # 设置ticklabel的显示与否
        if show_x_tick:
            for tickLabel in self.ax.get_xaxis().get_ticklabels():
                tickLabel.set_visible(True)
            self.ax.xaxis.set_tick_params(labelcolor='white')
        else:
            for tickLabel in self.ax.get_xaxis().get_ticklabels():
                tickLabel.set_visible(False)

        # mpl连接
        self.fig.canvas.mpl_connect('key_press_event', self._press)
        self.fig.canvas.mpl_connect('button_press_event', self._onpress)

        # 同步
        self.synchronList = []


    def _draw_text_selectLine(self):
        _draw_text_lines(axTop=self.axTop, pickedX=self.pickedX,
                         df=self.df, fontProp=self.fontProp, showCol=['CLOSE','VOLUME'],
                         textX=0.01, textY=0.3, dtFormat='%Y-%m-%d %H:%M:%S')
        # 画选择的的虚线
        if self.selectionLine is None:
            self.selectionLine = self.ax.plot((self.pickedX,) * 2, self.ax.get_ylim(),
                                              color='white', linewidth=1, linestyle='dotted',
                                              zorder=10)[0]
        else:
            self.ax.lines.remove(self.selectionLine)
            self.selectionLine = self.ax.plot((self.pickedX,) * 2, self.ax.get_ylim(),
                                              color='white', linewidth=1, linestyle='dotted',
                                              zorder=10)[0]


class Intraday_Volume(Intraday):


    def __init__(self, ax, df1minInOneday, show_x_tick=True):
        self.ax = ax
        self.fig = self.ax.figure

        self.fontProp = FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=10)

        if 'DATETIME' in df1minInOneday.columns:
            self.df = df1minInOneday
        else:
            self.df = df1minInOneday.reset_index()

        self.pickedX = 0
        self.selectionLine = None

        _draw_intraday_vol(ax, self.df, fontProp=self.fontProp)
        _date_format_xdata(self.ax, self.df, format='%Y-%m-%d %H:%M:%S')
        self._draw_text_selectLine()

        # 设置ticklabel的显示与否
        if show_x_tick:
            for tickLabel in self.ax.get_xaxis().get_ticklabels():
                tickLabel.set_visible(True)
            self.ax.xaxis.set_tick_params(labelcolor='white')
        else:
            for tickLabel in self.ax.get_xaxis().get_ticklabels():
                tickLabel.set_visible(False)
        # self.ax.yaxis.set_tick_params(labelcolor='white')

        # mpl连接
        self.fig.canvas.mpl_connect('key_press_event', self._press)
        self.fig.canvas.mpl_connect('button_press_event', self._onpress)

        # 同步
        self.synchronList = []


    def _draw_text_selectLine(self):
        # 画选择的的虚线
        if self.selectionLine is None:
            self.selectionLine = self.ax.plot((self.pickedX,) * 2, self.ax.get_ylim(),
                                              color='white', linewidth=1, linestyle='dotted',
                                              zorder=10)[0]
        else:
            self.ax.lines.remove(self.selectionLine)
            self.selectionLine = self.ax.plot((self.pickedX,) * 2, self.ax.get_ylim(),
                                              color='white', linewidth=1, linestyle='dotted',
                                              zorder=10)[0]


class Intraday_Lines(Intraday):


    def __init__(self, ax, axTop, dfNeed, baseLine=None, pickedX=0, show_x_tick=True):
        self.ax = ax
        self.axTop = axTop
        self.fig = self.ax.figure

        self.fontProp = FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=10)

        if 'DATETIME' in dfNeed.columns:
            self.df = dfNeed
        else:
            self.df = dfNeed.reset_index()

        self.pickedX = pickedX
        self.selectionLine = None

        _draw_intraday_lines(self.ax, self.df, baseLine=baseLine)
        _date_format_xdata(self.ax, self.df, format='%Y-%m-%d %H:%M:%S')
        self._draw_text_selectLine()

        # 设置ticklabel的显示与否
        if show_x_tick:
            for tickLabel in self.ax.get_xaxis().get_ticklabels():
                tickLabel.set_visible(True)
            self.ax.xaxis.set_tick_params(labelcolor='white')
        else:
            for tickLabel in self.ax.get_xaxis().get_ticklabels():
                tickLabel.set_visible(False)

        # mpl连接
        self.fig.canvas.mpl_connect('key_press_event', self._press)
        self.fig.canvas.mpl_connect('button_press_event', self._onpress)

        # 同步
        self.synchronList = []


    def _draw_text_selectLine(self):
        _draw_text_lines(axTop=self.axTop, pickedX=self.pickedX,
                         df=self.df, fontProp=self.fontProp, showCol='all',
                         textX=0.01, textY=0.3, dtFormat='%Y-%m-%d %H:%M:%S')
        # 画选择的的虚线
        if self.selectionLine is None:
            self.selectionLine = self.ax.plot((self.pickedX,) * 2, self.ax.get_ylim(),
                                              color='white', linewidth=1, linestyle='dotted',
                                              zorder=10)[0]
        else:
            self.ax.lines.remove(self.selectionLine)
            self.selectionLine = self.ax.plot((self.pickedX,) * 2, self.ax.get_ylim(),
                                              color='white', linewidth=1, linestyle='dotted',
                                              zorder=10)[0]



#################### Candle使用范例 #####################################################################################
if __name__ == '__main__':
    # candle的各种参数的
    from tools.mplot import fig
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 生成fig
    canvolFIg = fig.Fig()

    candle = Candle(canvolFIg.ax1, canvolFIg.ax1Top, df,  # 必选参数，ax和数据，candle里面需要有开高低收
                    startNum=0,endNum=None,length=80,  # 初始显示的范围
                    lenuplt=300,lendownlt=50,step=10,  # 按↑和↓变动上下限，以及变动步长
                    majorGrid='month',majorFormat='%Y-%m',majorGridStyle='solid',
                    minorGrid='week',minorFormat=None, minorGridStyle='dotted',
                    show_x_tick=True,
                    dtFormat='%Y-%m-%d')

    canvolFIg.show()
    del df, canvolFIg, candle


if __name__ == '__main__':
    # candle的各种参数的
    from tools.mplot import fig
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 生成fig
    canvolFIg = fig.Fig()

    candle = Candle(canvolFIg.ax1, canvolFIg.ax1Top, df,  # 必选参数，ax和数据，candle里面需要有开高低收
                    startNum=0,endNum=None,length=80,  # 初始显示的范围
                    lenuplt=300,lendownlt=50,step=10,  # 按↑和↓变动上下限，以及变动步长
                    majorGrid=None,majorFormat='%Y-%m',majorGridStyle='solid', # 可以两个都为None，那么就没有grid
                    minorGrid=None,minorFormat=None, minorGridStyle='dotted',
                    show_x_tick=False, # 注意show_x_tick也要是False
                    dtFormat='%Y-%m-%d')

    canvolFIg.show()

    del df, canvolFIg, candle


if __name__ == '__main__':
    # 显示分钟线数据
    from tools.mplot import fig
    df = fetch.n_min('000001.SH', 30, startTime='2017-12-31') # 短一点，避免卡
    canvolFIg = fig.Fig()
    candle = Candle(canvolFIg.ax1, canvolFIg.ax1Top, df,  # 必选参数，ax和数据，candle里面需要有开高低收
                    startNum=0,endNum=None,length=80,  # 初始显示的范围
                    lenuplt=300,lendownlt=50,step=10,  # 按↑和↓变动上下限，以及变动步长
                    majorGrid='day',majorFormat='%Y-%m-%d',majorGridStyle='solid', # 注意分钟线的grid的单位
                    minorGrid=None,minorFormat=None, minorGridStyle='dotted', # 只显示major
                    show_x_tick=True, # 注意show_x_tick也要是False
                    dtFormat='%Y-%m-%d %H:%M:%S')

    canvolFIg.show()

    del df, canvolFIg, candle


#################### Lines使用范例 #####################################################################################
if __name__ == '__main__':
    # Lines的各种参数的
    from tools.mplot import fig
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 生成fig
    closeFig = fig.Fig()

    close = Lines(closeFig.ax1, closeFig.ax1Top,
                   df[['CLOSE']], baseLine=None,# 必选参数，ax和数据，Lines只需要输入所需的线就可以了。可以没有基线，基线是红色
                   startNum=0, endNum=None, length=80,  # 初始显示的范围
                   lenuplt=300, lendownlt=50, step=10,  # 按↑和↓变动上下限，以及变动步长
                   majorGrid='month', majorFormat='%Y-%m', majorGridStyle='dotted',
                   minorGrid=None, minorFormat=None, minorGridStyle='dotted', # 默认是不画次坐标grid的，当然也是可以画的
                   show_x_tick=True,
                   dtFormat='%Y-%m-%d')

    closeFig.show()
    del df, closeFig, close


if __name__ == '__main__':
    # Lines的各种参数的
    from tools.mplot import fig
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 生成fig
    closeFig = fig.Fig()

    close = Lines(closeFig.ax1, closeFig.ax1Top,
                   df[['CLOSE']], baseLine=None,# 必选参数，ax和数据，Lines只需要输入所需的线就可以了。可以没有基线，基线是红色
                   startNum=0, endNum=None, length=80,  # 初始显示的范围
                   lenuplt=300, lendownlt=50, step=10,  # 按↑和↓变动上下限，以及变动步长
                   majorGrid='month', majorFormat='%Y-%m', majorGridStyle='solid',
                   minorGrid='week', minorFormat=None, minorGridStyle='dotted', # 默认是不画次坐标grid的，当然也是可以画的
                   show_x_tick=True,
                   dtFormat='%Y-%m-%d')

    closeFig.show()
    del df, closeFig, close


if __name__ == '__main__':
    # Lines的各种参数的，分钟线显示
    from tools.mplot import fig
    df = fetch.n_min('000001.SH', 30, startTime='2017-12-31')  # 短一点，避免卡
    # 生成fig
    closeFig = fig.Fig()

    close = Lines(closeFig.ax1, closeFig.ax1Top,
                   df[['CLOSE']], baseLine=None,# 必选参数，ax和数据，Lines只需要输入所需的线就可以了。可以没有基线，基线是红色
                   startNum=0, endNum=None, length=80,  # 初始显示的范围
                   lenuplt=300, lendownlt=50, step=10,  # 按↑和↓变动上下限，以及变动步长
                   majorGrid='day', majorFormat='%Y-%m-%d', majorGridStyle='dotted',
                   minorGrid=None, minorFormat=None, minorGridStyle='dotted', # 默认是不画次坐标grid的，当然也是可以画的
                   show_x_tick=True,
                   dtFormat='%Y-%m-%d %H:%M:%S') # 分钟线注意dtFormat

    closeFig.show()

    del df, closeFig, close


if __name__ == '__main__':
    # Lines的各种参数的，分钟线显示，显示两根线
    from tools.mplot import fig
    import talib as ta

    df = fetch.n_min('000001.SH', 30, startTime='2017-12-31')  # 短一点，避免卡

    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    closeFig = fig.Fig()

    # 在subplot上面画图
    close = Lines(closeFig.ax1, closeFig.ax1Top,
                  df[['CLOSE']], baseLine=None,# 必选参数，ax和数据，Lines只需要输入所需的线就可以了。可以没有基线，基线是红色
                  startNum=0, endNum=None, length=100,  # 初始显示的范围
                  lenuplt=1000, lendownlt=50, step=50,  # 按↑和↓变动上下限，以及变动步长
                  majorGrid='day', majorFormat='%Y-%m-%d', majorGridStyle='dotted',
                  minorGrid=None, minorFormat=None, minorGridStyle='dotted', # 默认是不画次坐标grid的，当然也是可以画的
                  show_x_tick=True,
                  dtFormat='%Y-%m-%d %H:%M:%S') # 分钟线注意dtFormat
    tech =  Lines(closeFig.ax2, closeFig.ax2Top,
                  df[['DIFF','DEA']], baseLine=0,
                  startNum=0, endNum=None, length=100,  # 初始显示的范围
                  lenuplt=1000, lendownlt=50, step=50,  # 按↑和↓变动上下限，以及变动步长
                  majorGrid='day', majorFormat='%Y-%m-%d', majorGridStyle='dotted',
                  minorGrid=None, minorFormat=None, minorGridStyle='dotted',  # 默认是不画次坐标grid的，当然也是可以画的
                  show_x_tick=True,
                  dtFormat='%Y-%m-%d %H:%M:%S')  # 分钟线注意dtFormat

    # 联动设置
    close.add_synchron(tech)
    tech.add_synchron(close)

    # 显示
    closeFig.show()

    del df, closeFig, tech, close


#################### Volume使用范例 #####################################################################################
if __name__ == '__main__':
    # 这个是画K线和成交量两个联动的图
    from tools.mplot import fig
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡

    # 生成fig
    canvolFIg = fig.Fig()

    # 在subplot上面画图
    candle = Candle(canvolFIg.ax1, canvolFIg.ax1Top, df,  # 必选参数，ax和数据，candle里面需要有开高低收
                    startNum=0,endNum=None,length=80,  # 初始显示的范围
                    lenuplt=300,lendownlt=50,step=10,  # 按↑和↓变动上下限，以及变动步长
                    majorGrid='month',majorFormat='%Y-%m',majorGridStyle='solid',
                    minorGrid='week',minorFormat=None, minorGridStyle='dotted',
                    show_x_tick=True,
                    dtFormat='%Y-%m-%d')
    vol = Volume(canvolFIg.ax2, df,
                 startNum=0, endNum=None, length=80,  # 初始显示的范围
                 lenuplt=300, lendownlt=50, step=10,  # 按↑和↓，变动上下限，以及变动步长
                 majorGrid='month', majorFormat='%Y-%m', majorGridStyle='solid',
                 minorGrid='week', minorFormat=None, minorGridStyle='dotted',
                 show_x_tick=False,  # 不显示tick
                 dtFormat='%Y-%m-%d')

    # 联动设置
    candle.add_synchron(vol)
    vol.add_synchron(candle)

    # 显示
    canvolFIg.show()

    del df, candle, vol, canvolFIg


#################### 其他功能 ###########################################################################################
if __name__ == '__main__':
    # 这个是画buysell标志的图
    from tools.mplot import fig
    import talib as ta
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2
    # 生成买卖标记
    from tools.backtest import signal
    signal.up_cross(df, fastline='DIFF', slowline='DEA')

    buysellFig = fig.Fig()

    # 在subplot上面画图
    close = Lines(buysellFig.ax1, buysellFig.ax1Top,
                  df[['CLOSE']],
                  lenuplt=1000, step=50,
                  majorFormat='%Y')
    tech = Lines(buysellFig.ax2, buysellFig.ax2Top,
                 df[['DIFF','DEA']], baseLine=0,
                 lenuplt=1000, step=50,
                 majorFormat='%Y',
                 show_x_tick=False)

    # 添加买卖标记
    close.attach_buysell_sign(df[['CLOSE', 'signal']], align='left',mode='longshort')

    # 联动设置
    close.add_synchron(tech)
    tech.add_synchron(close)

    # 显示
    buysellFig.show()

    del df, close, tech, buysellFig


if __name__ == '__main__':
    # 这个是画buysell标志的图，分钟线
    from tools.mplot import fig
    import talib as ta

    df = fetch.n_min('000001.SH', 30, startTime='2017-12-31')  # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2
    # 生成买卖标记
    from tools.backtest import signal
    signal.up_cross(df, fastline='DIFF', slowline='DEA')

    buysellFig = fig.Fig()

    # 在subplot上面画图
    close = Lines(buysellFig.ax1, buysellFig.ax1Top,
                  df[['CLOSE']],
                  lenuplt=1000, step=50,
                  majorFormat='%Y-%m-%d',
                  dtFormat='%Y-%m-%d %H:%M:%S')
    tech = Lines(buysellFig.ax2, buysellFig.ax2Top,
                 df[['DIFF','DEA']], baseLine=0,
                 lenuplt=1000, step=50,
                 majorFormat='%Y-%m-%d',
                 show_x_tick=False,
                 dtFormat='%Y-%m-%d %H:%M:%S')

    # 添加买卖标记
    close.attach_buysell_sign(df[['CLOSE', 'signal']], align='left',mode='short')

    # 联动设置
    close.add_synchron(tech)
    tech.add_synchron(close)

    # 显示
    buysellFig.show()
    del df, close, tech, buysellFig


# 这个是画wave sign标志的图
if __name__ == '__main__':
    from tools.mplot import fig
    import talib as ta
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2
    # 生成买卖标记
    from tools.backtest import signal
    signal.up_cross(df, fastline='DIFF', slowline='DEA')

    buysellFig = fig.Fig()

    # 在subplot上面画图
    close = Lines(buysellFig.ax1, buysellFig.ax1Top,
                  df[['CLOSE']],
                  lenuplt=1000, step=50,
                  majorFormat='%Y')
    tech = Lines(buysellFig.ax2, buysellFig.ax2Top,
                 df[['DIFF','DEA']],baseLine=0,
                 lenuplt=1000, step=50,
                 majorFormat='%Y',
                 show_x_tick=False)

    # 添加wave sign
    close.attach_wave_sign(df[['CLOSE', 'signal']], align='left')


    # 联动设置
    close.add_synchron(tech)
    tech.add_synchron(close)

    # 显示
    buysellFig.show()

    # 删去wave sign
    close.show_wave_sign()
    close.detach_wave_sign(1)

    del df, close, tech, buysellFig


#################### 这个是测试draw_series_on_axes和draw_month_analysis的效果 ############################################
if __name__ == '__main__':
    import talib as ta
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal,timing
    # 生成买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    signal.signal2position_single(df, signalCol='signal', at_once=False, mode='longshort', zhisun=-0.08,)

    # 单个品种的回测
    timing.single_proportion_backtest(df,)
    detailDf = timing.single_trade_details(df,originSignal='signal',at_once=False,mode='longshort',)
    evalSer = timing.overall_eval(df,detailDf)
    yearDf = timing.year_month_analysis(detailDf)

    #  这个显示回测结果的简单的图
    from tools.mplot import fig
    btFig = fig.Fig(subplotdict={'axEquity':[0.05,0.43,0.7,0.55],
                                 'axYear':[0.05,0.03,0.7,0.37],
                                 'axDesc':[0.8,0.03,0.15,95]},
                    if_top_need=(True,False,False))
    equityCurve = Equity_Curve(btFig.axEquity, df[['equityCurve']],contrast='000001.SH',labels=('DIFF-DEA',),)
    draw_series_on_axes(btFig.axDesc,evalSer,'white')
    draw_month_analysis(btFig.axYear, yearDf)
    btFig.show()

    del df, detailDf, evalSer, yearDf, btFig


# 这个是equityCurve画周线的情形，
if __name__ == '__main__':
    import talib as ta
    from tools.tinytools import stock_related,pandas_related
    import numpy as np
    df = fetch.index_one('000001.SH')
    df = pandas_related.day_downSample(df)
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal, timing
    # 生成买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    signal.signal2position_single(df, signalCol='signal', at_once=True, mode='long', zhisun=-0.05,)

    # 单个品种的回测
    timing.single_proportion_backtest(df)
    detailDf = timing.single_trade_details(df,originSignal='signal',at_once=True, mode='long',)
    evalSer = timing.overall_eval(df,detailDf)
    yearDf = timing.year_month_analysis(detailDf)

    from tools.mplot import fig
    ecFig = fig.Equity_Fig()
    ec = Equity_Curve(ecFig.axEquity, df[['equityCurve']],contrast='000001.SH',labels=('DIFF-DEA',))
    draw_series_on_axes(ecFig.axDesc,evalSer,'white')
    draw_month_analysis(ecFig.axYear, yearDf)
    ecFig.show()

    del df, detailDf, evalSer, yearDf, ec, ecFig


# 这个是equityCurve画分钟线的情形，
if __name__ == '__main__':
    import talib as ta
    df = fetch.n_min('000001.SH', 30, startTime='2017-12-31')  # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    from tools.backtest import signal,timing
    # 生成买卖标记
    signal.up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal
    signal.signal2position_single(df, signalCol='signal', at_once=False, mode='longshort', zhisun=-0.08,)

    # 单个品种的回测
    timing.single_proportion_backtest(df,)
    detailDf = timing.single_trade_details(df,originSignal='signal',at_once=False,mode='longshort',)
    evalSer = timing.overall_eval(df,detailDf)
    yearDf = timing.year_month_analysis(detailDf)

    #  这个显示回测结果的简单的图
    from tools.mplot import fig
    btFig = fig.Fig(subplotdict={'axEquity':[0.05,0.43,0.7,0.55],
                                 'axYear':[0.05,0.03,0.7,0.37],
                                 'axDesc':[0.8,0.03,0.15,95]},
                    if_top_need=(True,False,False))
    equityCurve = Equity_Curve(btFig.axEquity, df[['equityCurve']],contrast='000001.SH',labels=('DIFF-DEA',),period='30min',)
    draw_series_on_axes(btFig.axDesc,evalSer,'white')
    draw_month_analysis(btFig.axYear, yearDf)
    btFig.show()

    del df, detailDf, evalSer, yearDf, btFig, equityCurve


#################### 这个是测试intraday的效果 ############################################################################
if __name__ == '__main__':
    # intraday close和vol的显示和联动
    from tools.mplot import fig
    intradayFig = fig.Fig()

    df1min = fetch.n_min('601318.SH', 1)
    df1min.reset_index(inplace=True)
    df1min['date'] = df1min['DATETIME'].apply(lambda x: x.replace(hour=0, minute=0, second=0, microsecond=0))
    df1minInOneday = df1min[df1min['date'] == dt.datetime(2014,9,16)]

    intradayClose = Intraday_Close(intradayFig.ax1, intradayFig.ax1Top,
                                   df1minInOneday, 42.34,
                                   show_x_tick=True)
    intradayVol = Intraday_Volume(intradayFig.ax2,
                                  df1minInOneday,
                                  show_x_tick=False)

    intradayClose.add_synchron(intradayVol)
    intradayVol.add_synchron(intradayClose)

    intradayFig.show()

    del df1minInOneday, df1min, intradayFig, intradayClose, intradayVol

