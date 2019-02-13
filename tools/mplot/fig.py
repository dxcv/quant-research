
'''
各种常用的fig的便利函数
'''
import matplotlib.pyplot as plt


#################### 内部函数 ###########################################################################################
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


#################### 对外使用的类 #######################################################################################
class Fig(object):


    def __init__(self, figsize=(20, 9), facecolor='black', subplotdict={}, if_top_need=()):
        '''
        这个类的作用生成一个fig，里面有几个subplot。
        subplot的名字和大小位置由suplotdict来指定。例如
        subplotdict = {'axCandle':[0.05,0.45,0.9,0.5],
                      'axTech1':[0.05,0.2,0.9,0.18],
                      'axTech2': [0.05, 0, 0.9, 0.18],}
        底下的时间标签得留0.03的空间
        if_top_need给出对应的subplot是否有顶部显示文字，所以设计subplotdict的时候必须给顶部留出0.02空间。
        :param figsize: fig的大小，默认(20, 9)
        :param facecolor: 整个fig和下面subplot底色，more黑色
        :param subplotdict: 见上面的描述
        :param if_top_need: 见上面的描述
        '''
        if len(subplotdict) != len(if_top_need):
            raise ValueError('suplotdict和if_top_need必须对应!')
        elif subplotdict == {} and if_top_need == ():
            # 如果两个都是空的话，那么就默认返回两个ax。0.6:0.4的默认图，带有两个Top，名字分别是ax1和ax2
            _subplotdict = {'ax1': [0.05, 0.43, 0.9, 0.55],
                            'ax2': [0.05, 0.0, 0.9, 0.38]}
            self.fig = plt.figure(figsize=figsize, facecolor=facecolor)
            _if_top_need = (True,True)
            for i,name in enumerate(_subplotdict):
                exec(f'self.{name} = self.fig.add_axes({_subplotdict[name]},facecolor=\'{facecolor}\')')
                exec(f'_set_spines(self.{name}, \'white\', width=0.5)')
                if _if_top_need[i]:
                    topCord = _subplotdict[name]
                    topCord[1] = topCord[1] + topCord[3]
                    topCord[3] = 0.02
                    exec(f'self.{name}Top = self.fig.add_axes({topCord},facecolor=\'{facecolor}\')')
                    exec(f'_set_spines(self.{name}Top, \'white\', width=0.5)')
                else:
                    pass
            print('可引用的ax是ax1,ax1Top,ax2,ax2Top')
        else:
            self.fig = plt.figure(figsize=figsize,facecolor=facecolor)
            for i,name in enumerate(subplotdict):
                exec(f'self.{name} = self.fig.add_axes({subplotdict[name]},facecolor=\'{facecolor}\')')
                exec(f'_set_spines(self.{name}, \'white\', width=0.5)')
                if if_top_need[i]:
                    topCord = subplotdict[name]
                    topCord[1] = topCord[1] + topCord[3]
                    topCord[3] = 0.02
                    exec(f'self.{name}Top = self.fig.add_axes({topCord},facecolor=\'{facecolor}\')')
                    exec(f'_set_spines(self.{name}Top, \'white\', width=0.5)')
                else:
                    pass


    def show(self):
        '''
        便利的函数，可以用来show
        :return:
        '''
        self.fig.show()


class Equity_Fig(Fig):


    def __init__(self):
        super(Equity_Fig, self).__init__(subplotdict={'axEquity':[0.05,0.43,0.7,0.55],
                                 'axYear':[0.05,0.03,0.7,0.37],
                                 'axDesc':[0.8,0.03,0.15,95]},
                                         if_top_need=(True,False,False))
        print('可引用的ax是axEquity,axEquityTop,axYear,axDesc')


#################### 使用范例 ###########################################################################################
if __name__ == '__main__':
    # 这个是用来测试，一个Fig的类，里面有6个ax，名字分别'axCandle','axCandleTop'，
    # 'axTech1','axTech1Top',
    # 'axTech2','axTech2Top'
    subplotdict = {'axCandle': [0.05, 0.43, 0.9, 0.55],
                   'axTech1': [0.05, 0.2, 0.9, 0.18],
                   'axTech2': [0.05, 0, 0.9, 0.18], }
    testFig = Fig(subplotdict=subplotdict,if_top_need=(True,True,True))
    testFig.fig.show()

    # @todo 需要做的可能就是修改边框颜色。


if __name__ == '__main__':
    # 这个是测试默认是否可用
    defaultFig = Fig()
    defaultFig.show()