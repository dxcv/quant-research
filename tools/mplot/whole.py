
import pandas as pd
from tools.mplot import fig, sub
from tools.data import fetch
from tools.tinytools import stock_related
from dateutil.parser import parse
from matplotlib.patches import Rectangle


#################### 内部函数 ###########################################################################################
def _wipe_axes(*args):
    for ax in args:
        ax.collections = []
        ax.patches = []
        ax.lines = []
        ax.texts = []


#################### 对外使用的类 #######################################################################################
defaultSubplotdict = {'candleDay':[0.02,0.68,0.95,0.30],
                      'volDay':[0.02,0.53,0.95,0.15],
                      'closeMin':[0.02,0.16,0.45,0.32],
                      'volMin':[0.02,0.00,0.45,0.16],
                      'tech1':[0.52,0.25,0.45,0.23],
                      'tech2':[0.52,0.00,0.45,0.23]}
defaultIf_top_need = (True,False,True,False,True,True)


class Intraday_General(object):


    def __init__(self, subplotdict=defaultSubplotdict, if_top_need=defaultIf_top_need,
                 dfDay=None, df1min=None):
        # 指定dfDay和df1min的时候只有在defaultSubplotdict可用，这是为了一个常用的功能而设置了便利，而且dfDay必须包含开高低收量额，
        # df1min必须包含CLOSE和量额。
        self.window = fig.Fig(subplotdict=subplotdict, if_top_need=if_top_need)
        if dfDay.index[0][0] != df1min.index[0][0].replace(hour=0,minute=0,second=0) \
                or dfDay.index[-1][0] != df1min.index[-1][0].replace(hour=0,minute=0,second=0):
            raise ValueError('dfDay和df1min的时间区间必须相同')
        # 储存数据和相关设定的df
        self.allDf = pd.DataFrame(index=subplotdict.keys(),
                                  columns=['区域类','类名','类型',
                                           'if_top_need', 'show_x_tick',
                                           '数据df', '分钟线展示的数据', '分钟线preclose数据源', '分钟线preclose','musk'])
        for i, key in enumerate(subplotdict):
            self.allDf.at[key, 'if_top_need'] = if_top_need[i]

        self.musk_day = False
        self.musk_min = False


        # 初始化，主要是用用于default情况
        if dfDay is not None and df1min is not None:
            self.allDf.at['candleDay', '数据df'] = dfDay
            self.allDf.at['volDay', '数据df'] = dfDay
            self.dateSeries = self.allDf.at['candleDay', '数据df'].reset_index()['DATETIME']

            # 弄好设置
            self.allDf.at['candleDay', '类名'] = 'Candle'
            self.allDf.at['volDay', '类名'] = 'Volume'

            self.allDf.at['candleDay', '类型'] = 'day'
            self.allDf.at['volDay', '类型'] = 'day'

            self.allDf.at['candleDay', 'show_x_tick'] = False
            self.allDf.at['volDay', 'show_x_tick'] = True

            # # 画日线的图
            for areaName in self.allDf[self.allDf['类型'] == 'day'].index:
                # areaName = self.allDf['类型'][self.allDf['类型'] == 'day'].index[1]
                # print(areaName)
                if self.allDf.at[areaName, 'if_top_need'] is True:
                    exec(f"self.allDf.at['{areaName}', '区域类'] = sub.{self.allDf.at[areaName, '类名']}(self.window.{areaName}, self.window.{areaName}Top, self.allDf.at['{areaName}', '数据df'], show_x_tick={self.allDf.at[areaName, 'show_x_tick']})")
                else:
                    exec(
                        f"self.allDf.at['{areaName}', '区域类'] = sub.{self.allDf.at[areaName, '类名']}(self.window.{areaName}, self.allDf.at['{areaName}', '数据df'], show_x_tick={self.allDf.at[areaName, 'show_x_tick']})")

            for areaName in self.allDf[self.allDf['类型'] == 'day'].index:
                for synName in self.allDf[(self.allDf['类型'] == 'day') & (self.allDf.index != areaName)].index:
                    self.allDf.at[areaName, '区域类'].add_synchron(self.allDf.at[synName, '区域类'])

            # 画分钟线
            self.__df1min = df1min.reset_index()
            self.__df1min['date'] = self.__df1min['DATETIME'].apply(
                lambda x: x.replace(hour=0, minute=0, second=0, microsecond=0))
            self.allDf.at['closeMin', '数据df'] = self.__df1min
            self.allDf.at['volMin', '数据df'] = self.__df1min

            self.allDf.at['closeMin', '类名'] = 'Intraday_Close'
            self.allDf.at['volMin', '类名'] = 'Intraday_Volume'

            self.allDf.at['closeMin', '类型'] = 'min'
            self.allDf.at['volMin', '类型'] = 'min'

            self.allDf.at['closeMin', 'show_x_tick'] = False
            self.allDf.at['volMin', 'show_x_tick'] = False

            self.allDf.at['closeMin', '分钟线preclose数据源'] = self.allDf.at['candleDay', '数据df']

            self.selectedDate = self.dateSeries.iat[0]

            self._update_min_according_date()
            self._draw_minute_close()
            self.window.show()

        self.window.fig.canvas.mpl_connect('key_press_event', self._press)
        self.window.fig.canvas.mpl_connect('pick_event', self._onpick)


    def _update_min_according_date(self):
        for minshowindex in self.allDf[self.allDf['类型'] == 'min'].index:
            self.allDf.at[minshowindex, '分钟线展示的数据'] \
                = self.allDf.at[minshowindex, '数据df'][
                self.allDf.at[minshowindex, '数据df']['date'] == self.selectedDate]
            if self.allDf.at[minshowindex, '类名'] == 'Intraday_Close':
                self.allDf.at[minshowindex, '分钟线preclose'] \
                    = self.allDf.at[minshowindex, '分钟线preclose数据源'].reset_index(level=1).at[self.selectedDate, 'PRECLOSE']


    def _draw_minute_close(self):
        for minshowindex in self.allDf[self.allDf['类型'] == 'min'].index:
            exec(f"_wipe_axes(self.window.{minshowindex})")
            if self.allDf.at[minshowindex, 'if_top_need'] is True:
                exec(f"_wipe_axes(self.window.{minshowindex}Top)")
                exec(
                    f"self.allDf.at['{minshowindex}', '区域类'] = sub.{self.allDf.at[minshowindex, '类名']}(self.window.{minshowindex}, self.window.{minshowindex}Top, self.allDf.at['{minshowindex}', '分钟线展示的数据'], self.allDf.at['{minshowindex}', '分钟线preclose'], show_x_tick={self.allDf.at[minshowindex, 'show_x_tick']})")
            else:
                exec(
                    f"self.allDf.at['{minshowindex}', '区域类'] = sub.{self.allDf.at[minshowindex, '类名']}(self.window.{minshowindex}, self.allDf.at['{minshowindex}', '分钟线展示的数据'], show_x_tick={self.allDf.at[minshowindex, 'show_x_tick']})")

        for minshowindex in self.allDf[self.allDf['类型'] == 'min'].index:
            for synName in self.allDf[(self.allDf['类型'] == 'min') & (self.allDf.index != minshowindex)].index:
                self.allDf.at[minshowindex, '区域类'].add_synchron(self.allDf.at[synName, '区域类'])


    def _update_musk(self, minRenew=False):
        if self.musk_day and not self.allDf[self.allDf['类型'] == 'day'].empty:
            for dayIndex in self.allDf[self.allDf['类型'] == 'day'].index:
                self.allDf.at[dayIndex, 'musk'].set_x(self.allDf.at[dayIndex,'区域类'].pickedX+0.5)
                self.allDf.at[dayIndex, 'musk'].set_y(self.allDf.at[dayIndex,'区域类'].ax.get_ylim()[0])
                self.allDf.at[dayIndex, 'musk'].set_width(len(self.allDf.at[dayIndex, '数据df']) - self.allDf.at[dayIndex,'区域类'].pickedX)
                self.allDf.at[dayIndex, 'musk'].set_height(self.allDf.at[dayIndex,'区域类'].ax.get_ylim()[1])

        if self.musk_min and not self.allDf[self.allDf['类型'] == 'min'].empty:
            if minRenew:
                for minIndex in self.allDf[self.allDf['类型'] == 'min'].index:
                    self.allDf.at[minIndex, 'musk'].set_x(self.allDf.at[minIndex, '区域类'].pickedX + 0.5)
                    self.allDf.at[minIndex, 'musk'].set_y(self.allDf.at[minIndex, '区域类'].ax.get_ylim()[0])
                    self.allDf.at[minIndex, 'musk'].set_width(
                        len(self.allDf.at[minIndex, '数据df']) - self.allDf.at[minIndex, '区域类'].pickedX)
                    self.allDf.at[minIndex, 'musk'].set_height(self.allDf.at[minIndex, '区域类'].ax.get_ylim()[1])
                    exec(f"self.window.{minIndex}.add_patch(self.allDf.at['{minIndex}', 'musk'])")
            else:
                for minIndex in self.allDf[self.allDf['类型'] == 'min'].index:
                    self.allDf.at[minIndex, 'musk'].set_x(self.allDf.at[minIndex, '区域类'].pickedX + 0.5)
                    self.allDf.at[minIndex, 'musk'].set_y(self.allDf.at[minIndex, '区域类'].ax.get_ylim()[0])
                    self.allDf.at[minIndex, 'musk'].set_width(
                        len(self.allDf.at[minIndex, '数据df']) - self.allDf.at[minIndex, '区域类'].pickedX)
                    self.allDf.at[minIndex, 'musk'].set_height(self.allDf.at[minIndex, '区域类'].ax.get_ylim()[1])


    def _press(self, event):
        for index in self.allDf[self.allDf['类型'] == 'day'].index:
            if self.allDf.at[index, '类型'] == 'day':
                if event.inaxes == self.allDf.at[index, '区域类'].ax:
                    if event.key == 'right':
                        self.selectedDate = self.dateSeries[self.allDf.at[index, '区域类'].pickedX]
                        self._update_min_according_date()
                        self._draw_minute_close()
                        self._update_musk(minRenew=True)
                        self.window.fig.canvas.draw_idle()
                        break
                    elif event.key == 'left':
                        self.selectedDate = self.dateSeries[self.allDf.at[index, '区域类'].pickedX]
                        self._update_min_according_date()
                        self._draw_minute_close()
                        self._update_musk(minRenew=True)
                        self.window.fig.canvas.draw_idle()
                        break
                    else:
                        break
            elif self.allDf.at[index, '类型'] == 'min':
                if event.inaxes == self.allDf.at[index, '区域类'].ax:
                    if event.key == 'right':
                        self._update_musk()
                        self.window.fig.canvas.draw_idle()
                        break
                    elif event.key == 'left':
                        self._update_musk()
                        self.window.fig.canvas.draw_idle()
                        break
                    else:
                        break


    def _onpick(self, event):
        for dayIndex in self.allDf[self.allDf['类型'] == 'day'].index:
            if event.artist.axes == self.allDf.at[dayIndex, '区域类'].ax and isinstance(event.artist, Rectangle):
                self.selectedDate = self.dateSeries[self.allDf.at[dayIndex, '区域类'].pickedX]
                self._update_min_according_date()
                self._draw_minute_close()
                self._update_musk(minRenew=True)
                self.window.fig.canvas.draw_idle()
                break
            else:
                pass


    def _onpress(self, event):
        # 补充
        for minIndex in self.allDf[self.allDf['类型'] == 'min'].index:
            if event.inaxes == self.allDf.at[minIndex, '区域类'].ax:
                self._update_musk(minRenew=False)
                self.window.fig.canvas.draw_idle()
                break


    def attach_min_data(self, dfminTech, axName, className, precloseSrcDf=None,baseLine=None,show_x_tick=False):
        if not self.allDf[self.allDf['类型'] == 'day'].empty:
            if self.allDf[self.allDf['类型'] == 'day'].iat[0, self.allDf.columns.get_loc('数据df')].index[0][0] != dfminTech.index[0][0].replace(hour=0,minute=0,second=0) \
                    or self.allDf[self.allDf['类型'] == 'day'].iat[0, self.allDf.columns.get_loc('数据df')].index[-1][0] != dfminTech.index[-1][0].replace(hour=0,minute=0,second=0):
                raise ValueError('dfminTech的时间区间必须与原来的相同')
        else:
            pass

        __tmpMinDf = dfminTech.reset_index()
        __tmpMinDf['date'] = __tmpMinDf['DATETIME'].apply(
            lambda x: x.replace(hour=0, minute=0, second=0, microsecond=0))
        self.allDf.at[axName, '数据df'] = __tmpMinDf
        self.allDf.at[axName, '类名'] = className
        self.allDf.at[axName, '类型'] = 'min'
        self.allDf.at[axName, 'if_top_need'] = True
        self.allDf.at[axName, 'show_x_tick'] = show_x_tick
        if className == 'Intraday_Close' and precloseSrcDf is None:
            raise ValueError('Intraday_Close必须提供前收的数据')
        elif className == 'Intraday_Close' and precloseSrcDf is not None:
            self.allDf.at[axName, '分钟线展示的数据'] \
                = self.allDf.at[axName, '数据df'][
                self.allDf.at[axName, '数据df']['date'] == self.selectedDate]
            self.allDf.at[axName, '分钟线preclose数据源'] = precloseSrcDf
            self.allDf.at[axName, '分钟线preclose'] \
                = self.allDf.at[axName, '分钟线preclose数据源'].reset_index(level=1).at[self.selectedDate, 'PRECLOSE']
            exec(
                f"self.allDf.at['{axName}', '区域类'] = sub.{self.allDf.at[axName, '类名']}(self.window.{axName}, self.window.{axName}Top, self.allDf.at['{axName}', '分钟线展示的数据'], self.allDf.at['{axName}', '分钟线preclose'], show_x_tick={self.allDf.at[axName, 'show_x_tick']})")
            if not self.allDf[(self.allDf['类型'] == 'min') & (self.allDf.index != axName)].empty:
                exec(f"self.allDf.at['{axName}','区域类'].pickedX = self.allDf[(self.allDf['类型'] == 'min') & (self.allDf.index != '{axName}')].iat[0 ,self.allDf.columns.get_loc('区域类')].pickedX")
                exec(f"self.allDf.at['{axName}','区域类']._draw_text_selectLine()")
                for minIndex in self.allDf[(self.allDf['类型'] == 'min') & (self.allDf.index != axName)].index:
                    self.allDf.at[axName, '区域类'].add_synchron(self.allDf.at[minIndex, '区域类'])
                    self.allDf.at[minIndex, '区域类'].add_synchron(self.allDf.at[axName, '区域类'])
        elif className == 'Intraday_Lines':
            exec(
                f"self.allDf.at['{axName}', '区域类'] = sub.{self.allDf.at[axName, '类名']}(self.window.{axName}, self.window.{axName}Top, self.allDf.at['{axName}', '分钟线展示的数据'], baseLine={baseLine}, show_x_tick={self.allDf.at[axName, 'show_x_tick']})")
            if not self.allDf[(self.allDf['类型'] == 'min') & (self.allDf.index != axName)].empty:
                exec(f"self.allDf.at['{axName}','区域类'].pickedX = self.allDf[(self.allDf['类型'] == 'min') & (self.allDf.index != '{axName}')].iat[0 ,self.allDf.columns.get_loc('区域类')].pickedX")
                exec(f"self.allDf.at['{axName}','区域类']._draw_text_selectLine()")
                for minIndex in self.allDf[(self.allDf['类型'] == 'min') & (self.allDf.index != axName)].index:
                    self.allDf.at[axName, '区域类'].add_synchron(self.allDf.at[minIndex, '区域类'])
                    self.allDf.at[minIndex, '区域类'].add_synchron(self.allDf.at[axName, '区域类'])
        elif className == 'Intraday_Volume':
            pass


    def attach_day_data(self, dfdayTech, axName, className, baseLine=None, show_x_tick=False):
        if not self.allDf[self.allDf['类型'] == 'day'].empty:
            if self.allDf[self.allDf['类型'] == 'day'].iat[0, self.allDf.columns.get_loc('数据df')].index[0][0] != dfdayTech.index[0][0] \
                    or self.allDf[self.allDf['类型'] == 'day'].iat[0, self.allDf.columns.get_loc('数据df')].index[-1][0] != dfdayTech.index[-1][0]:
                raise ValueError('dfdayTech的时间区间必须与原来的相同')
        else:
            pass

        self.allDf.at[axName, '数据df'] = dfdayTech
        self.allDf.at[axName, '类名'] = className
        self.allDf.at[axName, '类型'] = 'day'
        self.allDf.at[axName, 'if_top_need'] = True
        self.allDf.at[axName, 'show_x_tick'] = show_x_tick

        if className == 'Candle':
            exec(
                f"self.allDf.at['{axName}', '区域类'] = sub.{self.allDf.at[axName, '类名']}(self.window.{axName}, self.window.{axName}Top, self.allDf.at['{axName}', '数据df'], show_x_tick={self.allDf.at[axName, 'show_x_tick']})")
        elif className == 'Lines':
            exec(
                f"self.allDf.at['{axName}', '区域类'] = sub.{self.allDf.at[axName, '类名']}(self.window.{axName}, self.window.{axName}Top, self.allDf.at['{axName}', '数据df'], baseLine={baseLine}, show_x_tick={self.allDf.at[axName, 'show_x_tick']})")

        if not self.allDf[(self.allDf['类型'] == 'day') & (self.allDf.index != axName)].empty:
            __otherDayClass = self.allDf[(self.allDf['类型'] == 'day') & (self.allDf.index != axName)].iat[0, self.allDf.columns.get_loc('区域类')]
            exec(
                f"self.allDf.at['{axName}', '区域类'].startNum = self.allDf[(self.allDf['类型'] == 'day') & (self.allDf.index != '{axName}')].iat[0, self.allDf.columns.get_loc('区域类')].startNum")
            exec(
                f"self.allDf.at['{axName}', '区域类'].endNum = self.allDf[(self.allDf['类型'] == 'day') & (self.allDf.index != '{axName}')].iat[0, self.allDf.columns.get_loc('区域类')].endNum")
            exec(
                f"self.allDf.at['{axName}', '区域类'].showLen = self.allDf[(self.allDf['类型'] == 'day') & (self.allDf.index != '{axName}')].iat[0, self.allDf.columns.get_loc('区域类')].showLen")
            exec(
                f"self.allDf.at['{axName}', '区域类'].pickedX = self.allDf[(self.allDf['类型'] == 'day') & (self.allDf.index != '{axName}')].iat[0, self.allDf.columns.get_loc('区域类')].pickedX")
            exec(
                f"self.allDf.at['{axName}', '区域类']._draw_text_selectLine()")
            exec(
                f"self.allDf.at['{axName}', '区域类']._set_display_range()")
            del __otherDayClass

            for dayIndex in self.allDf[(self.allDf['类型'] == 'day') & (self.allDf.index != axName)].index:
                self.allDf.at[axName, '区域类'].add_synchron(self.allDf.at[dayIndex, '区域类'])
                self.allDf.at[dayIndex, '区域类'].add_synchron(self.allDf.at[axName, '区域类'])


    def musk(self, if_day=False, if_min=False):
        if if_day and self.musk_day is False:
            self.musk_day = True
            if not self.allDf[self.allDf['类型'] == 'day'].empty:
                for dayIndex in self.allDf[self.allDf['类型'] == 'day'].index:
                    self.allDf.at[dayIndex, 'musk'] = Rectangle((self.allDf.at[dayIndex,'区域类'].pickedX+0.5, self.allDf.at[dayIndex,'区域类'].ax.get_ylim()[0]),
                                                             len(self.allDf.at[dayIndex, '数据df']) - self.allDf.at[dayIndex,'区域类'].pickedX,
                                                             self.allDf.at[dayIndex, '区域类'].ax.get_ylim()[1],
                                                             edgecolor=None, facecolor='black', zorder=12)
                    exec(f"self.window.{dayIndex}.add_patch(self.allDf.at['{dayIndex}', 'musk'])")
        elif if_day is False and self.musk_day is True:
            if not self.allDf[self.allDf['类型'] == 'day'].empty:
                for dayIndex in self.allDf[self.allDf['类型'] == 'day'].index:
                    exec(f"self.window.{dayIndex}.patches.remove(self.allDf.at['{dayIndex}', 'musk'])")
                    self.allDf.at[dayIndex, 'musk'] = None

        if if_min is True and self.musk_min is False:
            self.musk_min = True
            if not self.allDf[self.allDf['类型'] == 'min'].empty:
                for minIndex in self.allDf[self.allDf['类型'] == 'min'].index:
                    self.allDf.at[minIndex, 'musk'] = Rectangle((self.allDf.at[minIndex,'区域类'].pickedX+0.5, self.allDf.at[minIndex,'区域类'].ax.get_ylim()[0]),
                                                             len(self.allDf.at[minIndex, '数据df']) - self.allDf.at[minIndex,'区域类'].pickedX,
                                                             self.allDf.at[minIndex, '区域类'].ax.get_ylim()[1],
                                                             edgecolor=None, facecolor='black', zorder=12)
                    exec(f"self.window.{minIndex}.add_patch(self.allDf.at['{minIndex}', 'musk'])")
        elif if_min is False and self.musk_min is True:
            if not self.allDf[self.allDf['类型'] == 'min'].empty:
                for minIndex in self.allDf[self.allDf['类型'] == 'min'].index:
                    exec(f"self.window.{minIndex}.patches.remove(self.allDf.at['{minIndex}', 'musk'])")
                    self.allDf.at[minIndex, 'musk'] = None

        self.window.fig.canvas.draw_idle()


if __name__ == '__main__':
    # intradayWindow = Intraday_General()
    dfDay = fetch.stock_one('601318', startTime='2014-04-28', endTime='2018-08-10')
    df1min = fetch.n_min('601318', 1)
    intraday = Intraday_General(dfDay=dfDay, df1min=df1min)

    dftechMin = fetch.n_min('000001.SH', startTime='2014-04-28', endTime='2018-08-10')
    dftechDay = fetch.index_one('000001.SH', startTime='2014-04-28', endTime='2018-08-10')
    intraday.attach_min_data(dfminTech=dftechMin, axName='tech1', className='Intraday_Close', precloseSrcDf=dftechDay)
    intraday.attach_day_data(dfdayTech=dftechDay, axName='tech2', className='Candle')

    intraday.musk(if_day=False, if_min=False)
