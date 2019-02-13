
#################### 产生信号的函数 #####################################################################################
# 金叉死叉
# 两条线 →→→→ 可能信号（一列1，0, -1）
def up_cross(df, fastline, slowline=None, signalname='signal'):
    '''
    这个函数的目的是在fastline上穿slowline开仓信号，反之。
    信号是当期的信号。
    :param df:
    :param fastline: 主动上穿的线，
    :param slowline:  被上穿的线
    :param signalname:
    :return:
    '''
    if slowline == None:
        df[signalname] = (df[fastline] * df[fastline].shift(1)).apply(lambda x: 1 if x < 0 else 0)
        df[signalname] = df[signalname] * df[fastline].apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)
    else:
        df[signalname] = ((df[fastline] - df[slowline])
                          * (df[fastline] - df[slowline]).shift(1)).apply(lambda x: 1 if x < 0 else 0)
        df[signalname] = df[signalname] * \
                         (df[fastline] - df[slowline]).apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)


# 多空信号分开两列合并成一个信号
# 多空信号（两列TF） →→→→ 可能信号（一列1，0, -1）
def long_short_signal(df, colNameTup=('longSignal', 'shortSignal'), signalName='signal'):
    '''
    这个函数的目的是根据'longSignal'和'shortSignal'来产生信号，两列都是TF。生成新的一列，默认是signal
    :param df:需要生成信号的列
    :param colNameTup:多空信号的名字
    :param signalName:生成的信号列的名字
    :return:
    '''
    _firstLoc = df.index.get_loc(df[df[colNameTup[0]] ^ df[colNameTup[1]]].index[0])
    df[signalName] = 0

    df.iat[_firstLoc, df.columns.get_loc(signalName)] \
        = 1 if (df.iat[_firstLoc, df.columns.get_loc(colNameTup[0])] == True) else -1
    curPo = df.iat[_firstLoc, df.columns.get_loc(signalName)]

    for curIndex in df.index[_firstLoc + 1:]:
        if (df.at[curIndex, colNameTup[0]] == True) \
                and (df.at[curIndex, colNameTup[1]] == False) \
                and (curPo < 0):
            df.at[curIndex, signalName] = 1
            curPo = 1
        elif (df.at[curIndex, colNameTup[0]] == False) \
                and (df.at[curIndex, colNameTup[1]] == True) \
                and curPo > 0:
            df.at[curIndex, signalName] = -1
            curPo = -1


# 增量策略，变动大于butstep时多，小于sellstep时空
# 一条线 →→→→ 多空信号（两列TF）
def change_step(df, changingLine, buystep, sellstep, longSignalName='buy', shortSignalName='sell'):
    '''
    这个函数的目的是根据techLine来生成买卖信号，techLine的变动大于sellstep出多信号，变动小于sellstep出空信号。
    生成的信号的列的名字默认是signal
    :param df:
    :param changingLine:
    :param buystep:
    :param sellstep:
    :param longSignalName:
    :param shortSignalName:
    :return:
    '''
    df[longSignalName] = (df[changingLine] - df[changingLine].shift()).fillna(0.0) > buystep
    df[shortSignalName] = (df[changingLine] - df[changingLine].shift()).fillna(0.0) < sellstep


# 大周期辅助小周期
# 一个[1,0,-1](originSignalCol)，一个TF列(longConditionCol) →
# @todo 这个在开多模式下，如果原始信号第一个是空的，就会出错。没有遇到足够多的例子来做这个模块。
def daxiao_liandong(df, originSignalCol, longConditionCol, rstSignalCol='signal'):
    # 这个是输入一个[1,0](originSignalCol)，一个TF列(longConditionCol)，只在True时候开多，而且是long模式
    # @todo 其他模式没有遇到相关的例子，先不做了
    df[rstSignalCol] = 0
    position = 0
    for index in df.index:
        # 开仓
        if position == 0 and df.at[index, originSignalCol] == 1 and df.at[index, longConditionCol] == True:
            df.at[index, rstSignalCol] = 1
            position = 1
        # 平仓
        elif position == 1 and df.at[index, originSignalCol] == -1:
            position = 0
            df.at[index, rstSignalCol] = -1


#################### 产生实际仓位的函数 ##################################################################################
def signal2position_single(df, signalCol='signal', pxCol='CLOSE',
                           at_once=True, mode='longshort',
                           zhisun=-100.0, zhiying=100.0, margin=1.0):
    '''
    单个品种用初始信号生成仓位，主要是对多空以及止损止盈进行处理
    这个函数的主要目的将原始的单个买卖信号。提供的功能主要有三类，
    1、是否信号当期开仓
    2、'longshort'：多空连续，'long'：仅多，'short'：仅空
    3、是否根据止损止盈来对信号进行调整，这里把止损止盈放到信号调整而不放到回测模块的目的是回测模块是基于矩阵的，止损止盈分析是基于循环的。
    而且不考虑的止损止盈的情况下效率较高，不想牺牲回测的效率。所以把止损止盈功能放到信号调整这里
    :param df: 需要对信号进行处理的df
    :param signalCol: 原始买卖信号的列名
    :param pxCol: 价格序列，主要是计算止损止盈的时候使用
    :param at_once: 是否信号出来的当期开平仓
    :param mode: 'longshort'：多空连续，'long'：仅多，'short'：仅空
    :param zhisun: float，止损，-0.05表示亏损超过5%就止损
    :param zhiying: 止盈，0.05表示盈利超过5%则止盈
    :return: 没有返回，对df进行处理，生成一列poChg
    '''
    # 仓位变化和信号的延迟
    if at_once:
        df['poChg'] = df[signalCol]
    else:
        df['poChg'] = df[signalCol].shift(1).fillna(0)

    # 止损和止盈设置
    if zhisun == -100.0 and zhiying == 100.0:
        # 仓位模式，'longshort'：多空连续，'long'：仅多，'short'：仅空
        if mode == 'longshort':
            # 最后不保留仓位
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] \
                    * df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] < 0:
                pass
            else:
                df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0
            # 中间的乘以2，连续开仓和平仓
            df.loc[df['poChg'] != 0.0, 'poChg'] = df.loc[df['poChg'] != 0.0, 'poChg'] * 2
            df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] = \
                df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] / 2
            df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = \
                df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] / 2

        elif mode == 'long':
            # 如果首个开空，那么不开
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] == -1:
                df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] = 0
            else:
                pass

            # 最后不保留仓位
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] \
                    * df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] < 0:
                pass
            else:
                df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0

        elif mode == 'short':
            # 如果首个开多，那么不开
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] == 1:
                df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] = 0
            else:
                pass
            # 最后不保留仓位
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] \
                    * df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] < 0:
                pass
            else:
                df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0
    else:
        if mode == 'longshort':
            position = 0
            df['poChg_real'] = 0.0
            df['position'] = 0
            for index in df.index:
                if position == 0 and df.at[index, 'poChg'] == 0:
                    pass
                elif position == 0 and df.at[index, 'poChg'] != 0.0:
                    position = df.at[index, 'poChg']
                    df.at[index, 'position'] = position
                    df.at[index, 'poChg_real'] = df.at[index, 'poChg']
                    _px = df.at[index, pxCol]

                elif position != 0 and df.at[index, 'poChg'] == 0:
                    _return = (df.at[index, pxCol] / _px - 1) * position / margin
                    if _return > zhiying:
                        df.at[index, 'poChg_real'] = -position
                        position = 0
                        df.at[index, 'position'] = 0
                    elif _return < zhisun:
                        df.at[index, 'poChg_real'] = -position
                        position = 0
                        df.at[index, 'position'] = 0
                    else:
                        df.at[index, 'position'] = position
                        pass
                elif position != 0 and df.at[index, 'poChg'] == position:
                    pass
                elif position != 0 and df.at[index, 'poChg'] == -position:
                    df.at[index, 'poChg_real'] = -position * 2 # 连续开仓
                    position = -position
                    df.at[index, 'position'] = position
                    _px = df.at[index, pxCol]
            df['poChg'] = df['poChg_real']
            if df.iat[-1,df.columns.get_loc('position')] != 0.0:
                if abs(df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg']) == 2:
                    df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] / 2
                elif abs(df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg']) == 1:
                    df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0
            else:
                pass
            df.drop(['poChg_real','position'],axis=1,inplace=True)

        elif mode == 'long':
            # 如果首个开空，那么不开
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] == -1:
                df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] = 0
            else:
                pass

            position = 0
            df['poChg_real'] = 0.0
            # df['position'] = 0.0
            for index in df.index:
                if position == 0 and df.at[index, 'poChg'] == 0:
                    pass
                elif position == 0 and df.at[index, 'poChg'] == 1:
                    position = 1
                    df.at[index, 'poChg_real'] = 1
                    # df.at[index, 'position'] = position
                    _px = df.at[index, pxCol]

                elif position != 0 and df.at[index, 'poChg'] == 0:
                    _return = (df.at[index, pxCol] / _px - 1) * position / margin
                    if _return > zhiying:
                        df.at[index, 'poChg_real'] = -position
                        position = 0
                        # df.at[index, 'position'] = position
                    elif _return < zhisun:
                        df.at[index, 'poChg_real'] = -position
                        position = 0
                        # df.at[index, 'position'] = position
                    else:
                        # df.at[index, 'position'] = position
                        pass
                elif position != 0 and df.at[index, 'poChg'] == position:
                    pass
                elif position != 0 and df.at[index, 'poChg'] == -position:
                    df.at[index, 'poChg_real'] = -position
                    position = 0
                    # df.at[index, 'position'] = position
            df['poChg'] = df['poChg_real']
            # if df.iat[-1,df.columns.get_loc('position')] != 0.0:
            #     df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0
            # else:
            #     pass
            # df.drop(['poChg_real','position'],axis=1,inplace=True)
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] \
                    * df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] < 0:
                pass
            else:
                df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0
            df.drop('poChg_real', axis=1, inplace=True)

        elif mode == 'short':
            # 如果首个开多，那么不开
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] == 1:
                df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] = 0
            else:
                pass

            position = 0
            df['poChg_real'] = 0.0
            # df['position'] = 0.0
            for index in df.index:
                if position == 0 and df.at[index, 'poChg'] == 0:
                    pass
                elif position == 0 and df.at[index, 'poChg'] == -1:
                    position = -1
                    df.at[index, 'poChg_real'] = -1
                    _px = df.at[index, pxCol]
                    # df.at[index, 'position'] = position

                elif position != 0 and df.at[index, 'poChg'] == 0:
                    _return = (df.at[index, pxCol] / _px - 1) * position / margin
                    if _return > zhiying:
                        df.at[index, 'poChg_real'] = -position
                        position = 0
                        # df.at[index, 'position'] = position
                    elif _return < zhisun:
                        df.at[index, 'poChg_real'] = -position
                        position = 0
                        # df.at[index, 'position'] = position
                    else:
                        # df.at[index, 'position'] = position
                        pass
                elif position != 0 and df.at[index, 'poChg'] == position:
                    pass
                elif position != 0 and df.at[index, 'poChg'] == -position:
                    df.at[index, 'poChg_real'] = -position  # 连续开仓
                    position = 0
                    # df.at[index, 'position'] = position
            df['poChg'] = df['poChg_real']
            # if df.iat[-1,df.columns.get_loc('position')] != 0.0:
            #     df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0
            # else:
            #     pass
            # df.drop(['poChg_real','position'],axis=1,inplace=True)
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] \
                    * df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] < 0:
                pass
            else:
                df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0
            df.drop('poChg_real', axis=1, inplace=True)


def signal2position_multi(df, signalCol='signal', at_once=True,
                          mode='longshort',
                          pxColList=[], longshortList=[], proportionList=[],
                          zhisun=-100.0, zhiying=100.0, margin=1.0):
    # 多个个品种用初始信号生成实际仓位，主要是对多空以及止损止盈进行处理
    # 仓位变化和信号的延迟
    if at_once:
        df['poChg'] = df[signalCol]
    else:
        df['poChg'] = df[signalCol].shift(1).fillna(0)

    if zhisun == -100.0 and zhiying == 100.0:
        # 仓位模式，'longshort'：多空连续，'long'：仅多，'short'：仅空
        if mode == 'longshort':
            # 最后不保留仓位
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] \
                    * df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] < 0:
                pass
            else:
                df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0
            # 中间的乘以2，连续开仓和平仓
            df.loc[df['poChg'] != 0.0, 'poChg'] = df.loc[df['poChg'] != 0.0, 'poChg'] * 2
            df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] = \
                df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] / 2
            df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = \
                df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] / 2

        elif mode == 'long':
            # 如果首个开空，那么不开
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] == -1:
                df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] = 0
            else:
                pass

            # 最后不保留仓位
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] \
                    * df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] < 0:
                pass
            else:
                df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0

        elif mode == 'short':
            # 如果首个开多，那么不开
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] == 1:
                df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] = 0
            else:
                pass
            # 最后不保留仓位
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] \
                    * df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] < 0:
                pass
            else:
                df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0
    else: # @todo 不知道这个的止损逻辑有没有错
        # 如果需要进行止损止盈的分析的话，那么必须提供价格序列列名，对应的多空和资金比例
        # 这里值提供资金比例模式的止损止盈分析，不提供数量模式的止盈止损分析
        if len(pxColList) != len(longshortList) \
                or len(pxColList) != len(proportionList) \
                or len(longshortList) != len(proportionList) \
                or len(pxColList) == 0:
            raise ValueError('pxColList, longshortList, proportionList必须匹配！')

        # 仓位模式，'longshort'：多空连续，'long'：仅多，'short'：仅空
        if mode == 'longshort':
            position = 0
            df['poChg_real'] = 0.0
            df['position'] = 0
            # df['_return'] = 0.0
            for index in df.index:
                if position == 0 and df.at[index, 'poChg'] == 0:
                    pass
                elif position == 0 and df.at[index, 'poChg'] != 0.0:
                    position = df.at[index, 'poChg']
                    df.at[index, 'position'] = position
                    df.at[index, 'poChg_real'] = df.at[index, 'poChg']
                    pxSer = df.loc[index,pxColList]

                elif position != 0 and df.at[index, 'poChg'] == 0:
                    _return = ((df.loc[index, pxColList] / pxSer - 1) * position * longshortList * proportionList / margin).sum()
                    # df.at[index, '_return'] = _return
                    if _return > zhiying:
                        df.at[index, 'poChg_real'] = -position
                        position = 0
                        df.at[index, 'position'] = 0
                    elif _return < zhisun:
                        df.at[index, 'poChg_real'] = -position
                        position = 0
                        df.at[index, 'position'] = 0
                    else:
                        df.at[index, 'position'] = position
                        pass
                elif position != 0 and df.at[index, 'poChg'] == position:
                    pass
                elif position != 0 and df.at[index, 'poChg'] == -position:

                    # _return = ((df.loc[index, pxColList] / pxSer - 1) * position * longshortList * proportionList / margin).sum()
                    # df.at[index, '_return'] = _return

                    df.at[index, 'poChg_real'] = -position * 2 # 连续开仓
                    position = -position
                    df.at[index, 'position'] = position
                    pxSer = df.loc[index, pxColList]
            df['poChg'] = df['poChg_real']
            if df.iat[-1,df.columns.get_loc('position')] != 0.0:
                if abs(df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg']) == 2:
                    df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] / 2
                elif abs(df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg']) == 1:
                    df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0
            else:
                pass
            df.drop(['poChg_real','position'],axis=1,inplace=True)

        elif mode == 'long':
            # 如果首个开空，那么不开
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] == -1:
                df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] = 0
            else:
                pass

            position = 0
            df['poChg_real'] = 0.0
            # df['position'] = 0.0
            # df['_return'] = 0.0
            for index in df.index:
                if position == 0 and df.at[index, 'poChg'] == 0:
                    pass
                elif position == 0 and df.at[index, 'poChg'] == 1:
                    position = 1
                    df.at[index, 'poChg_real'] = 1
                    # df.at[index, 'position'] = position
                    pxSer = df.loc[index, pxColList]

                elif position != 0 and df.at[index, 'poChg'] == 0:
                    _return = ((df.loc[
                                    index, pxColList] / pxSer - 1) * position * longshortList * proportionList / margin).sum()
                    # df.at[index, '_return'] = _return
                    if _return > zhiying:
                        df.at[index, 'poChg_real'] = -position
                        position = 0
                        # df.at[index, 'position'] = position
                    elif _return < zhisun:
                        df.at[index, 'poChg_real'] = -position
                        position = 0
                        # df.at[index, 'position'] = position
                    else:
                        # df.at[index, 'position'] = position
                        pass
                elif position != 0 and df.at[index, 'poChg'] == position:
                    pass
                elif position != 0 and df.at[index, 'poChg'] == -position:

                    # _return = ((df.loc[
                    #                 index, pxColList] / pxSer - 1) * position * longshortList * proportionList / margin).sum()
                    # df.at[index, '_return'] = _return

                    df.at[index, 'poChg_real'] = -position
                    position = 0
                    # df.at[index, 'position'] = position
            df['poChg'] = df['poChg_real']
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] \
                    * df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] < 0:
                pass
            else:
                df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0
            df.drop('poChg_real', axis=1, inplace=True)

        elif mode == 'short':
            # 如果首个开多，那么不开
            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] == 1:
                df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] = 0
            else:
                pass

            position = 0
            df['poChg_real'] = 0.0
            # df['position'] = 0.0
            # df['_return'] = 0.0
            for index in df.index:
                if position == 0 and df.at[index, 'poChg'] == 0:
                    pass
                elif position == 0 and df.at[index, 'poChg'] == -1:
                    position = -1
                    df.at[index, 'poChg_real'] = -1
                    pxSer = df.loc[index, pxColList]
                    # df.at[index, 'position'] = position

                elif position != 0 and df.at[index, 'poChg'] == 0:
                    _return = ((df.loc[
                                    index, pxColList] / pxSer - 1) * position * longshortList * proportionList / margin).sum()
                    # df.at[index, '_return'] = _return
                    if _return > zhiying:
                        df.at[index, 'poChg_real'] = -position
                        position = 0
                        # df.at[index, 'position'] = position
                    elif _return < zhisun:
                        df.at[index, 'poChg_real'] = -position
                        position = 0
                        # df.at[index, 'position'] = position
                    else:
                        # df.at[index, 'position'] = position
                        pass
                elif position != 0 and df.at[index, 'poChg'] == position:
                    pass
                elif position != 0 and df.at[index, 'poChg'] == -position:

                    # _return = ((df.loc[
                    #                 index, pxColList] / pxSer - 1) * position * longshortList * proportionList / margin).sum()
                    # df.at[index, '_return'] = _return

                    df.at[index, 'poChg_real'] = -position  # 连续开仓
                    position = 0
                    # df.at[index, 'position'] = position
            df['poChg'] = df['poChg_real']

            if df.at[df.loc[df['poChg'] != 0.0].index[0], 'poChg'] \
                    * df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] < 0:
                pass
            else:
                df.at[df.loc[df['poChg'] != 0.0].index[-1], 'poChg'] = 0
            df.drop('poChg_real', axis=1, inplace=True)


#################### 产生信号函数的使用范例 ##############################################################################
if __name__ == '__main__':
    # 这个是up_cross的使用例子
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2
    # 生成买卖标记
    up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal

    del df


if __name__ == '__main__':
    # 这个是long_short_signal的使用例子
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2

    # 这个例子是MACD大于0的时候开仓，MACD小于0的时候平仓
    df['long'] = df['MACD'] > 0
    df['short'] = df['MACD'] < 0
    long_short_signal(df, ('long','short'))

    del df


if __name__ == '__main__':
    # 这个是change_step的使用例子
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    df['RSI'] = ta.RSI(df['CLOSE'])
    df.dropna(axis=0, inplace=True)

    # 变动大于5多，小于-5空
    change_step(df, 'RSI', 5, -5,)
    long_short_signal(df,('buy','sell'))

    del df


if __name__ == '__main__':
    # @ todo 这个是daxiao_liandong的例子
    pass


#################### 信号变成仓位的例子 ##################################################################################
if __name__ == '__main__':
    # 这个是signal_transform_single的使用例子
    import talib as ta
    from tools.data import fetch
    df = fetch.index_one('000001.SH', endTime='2010-12-31') # 短一点，避免卡
    # 计算DEA指标
    df['DIFF'], df['DEA'], df['MACD'] = ta.MACD(df['CLOSE'])
    df.dropna(axis=0, inplace=True)
    df['MACD'] = df['MACD'] * 2
    # 生成买卖标记
    up_cross(df, fastline='DIFF', slowline='DEA') # 多了一列signal

    # 原始信号变成仓位
    # at_once=False信号下一期开仓
    signal2position_single(df, signalCol='signal', at_once=True, mode='longshort')
    signal2position_single(df, signalCol='signal', at_once=False, mode='longshort')

    # longshort是多空连续，long是只开多仓，short是只开空仓，都是没有止盈止损
    signal2position_single(df, signalCol='signal', at_once=True, mode='longshort')
    signal2position_single(df, signalCol='signal', at_once=True, mode='long')
    signal2position_single(df, signalCol='signal', at_once=False, mode='short')

    # 加入止盈止损
    signal2position_single(df, signalCol='signal', at_once=True, mode='longshort', zhiying=0.05, zhisun=-0.08)
    signal2position_single(df, signalCol='signal', at_once=True, mode='long', zhiying=0.05, zhisun=-0.08)
    signal2position_single(df, signalCol='signal', at_once=False, mode='short', zhiying=0.05, zhisun=-0.08)

    # 加入保证金比例
    signal2position_single(df, signalCol='signal', at_once=True, mode='longshort', zhiying=0.05, zhisun=-0.08, margin=0.3)
    signal2position_single(df, signalCol='signal', at_once=True, mode='long', zhiying=0.05, zhisun=-0.08, margin=0.3)
    signal2position_single(df, signalCol='signal', at_once=False, mode='short', zhiying=0.05, zhisun=-0.08, margin=0.3)


if __name__ == '__main__':
    # @todo 这个是signal_transform_multi的使用例子
    pass