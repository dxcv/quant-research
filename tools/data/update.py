
import pyodbc
import pandas as pd
import numpy as np
import pandas.io.sql as sql
import const
from tools.data import fetch
from dateutil.parser import parse
import datetime as dt
from sqlalchemy import create_engine
from WindPy import w
import time

# --------------------内部函数-------------------------------------------------------------------------------------------
def _winddb_command_df(queryStr,
                     host='v-wind', user='trade', passwd='trade',
                     db='wind_quant'):
    '''
    从winddb中取数据的便利函数，将连接过程便捷化
    :param queryStr: 查询的sql命令字符串
    :param host:
    :param user:
    :param passwd:
    :param db:
    :return: 返回数据df
    '''
    odbcConn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};DATABASE=%s;SERVER=%s;UID=%s;PWD=%s'
                              % (db, host, user, passwd), autocommit=True, charset='utf8')
    rstDf = sql.read_sql(queryStr, odbcConn)
    odbcConn.close()
    return rstDf


def _to_sql(df,tableName,DATABASE='stock_day', **to_sql_kwarg):
    engine = create_engine("mysql+mysqldb://{USER}:{PASSWORD}@{HOST}/{DATABASE}?charset=utf8".format(
        USER=const.USER, PASSWORD=const.PASSWORD, HOST=const.HOST, DATABASE=DATABASE)
                          )  # 会有warning
    df.to_sql(tableName, engine, **to_sql_kwarg)
    engine.dispose()


def _convert_2_capital(df):
    df.rename(columns=dict(zip(df.columns, [colName.upper() for colName in df.columns])), inplace=True)


def _exclude_DQ(df):
    df.rename(columns=dict(zip(df.columns, [colName.replace('S_DQ_', '') for colName in df.columns])), inplace=True)


def _convert_datetime_code(df):
    df.rename(columns={'S_INFO_WINDCODE': 'CODE', 'TRADE_DT': 'DATETIME'}, inplace=True)
    df['DATETIME'] = df['DATETIME'].apply(lambda x: parse(x))
    df.set_index(['DATETIME', 'CODE'], inplace=True)
    df.sort_index(level=['DATETIME', 'CODE'], inplace=True)


def _downsample_2_n(group1, n, methodAgg):
    '''
    根据传入的时间序列进行降采样
    :param group1: 行情数据，index为date类型的日期，columns为各个指标名称，用于聚合的columns用methodAgg来定义
    :param n: 降采样所采用的周期倍数, eg. n = 5，当markeData是一分钟行情数据时，表示降采样为5分钟行情数据
    :param methodAgg: marketData中各个指标的聚合方法，
           eg. methodAgg = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum', 'Position': 'sum'}
    :return: 经过降采样之后的行情数据，格式与marketData相同
    '''
    # grouped = minDF.groupby('date', group_keys=False)
    # group1 = grouped.get_group(dt.date(2017,11,24))
    group1.set_index('DATETIME',inplace=True)
    # 生成降采样之后的时间索引在原数据中的位置索引
    downSampleIndex = (([False] * (n - 1) + [True]) * (len(group1.index) // n)) + \
                      ([False] * (len(group1.index) % n))
    # 如果除不尽的，将最后那个改成True
    downSampleIndex[-1] = True

    groupIndex = group1.index[downSampleIndex].repeat(n)[:len(group1.index)]  # 生成用于groupby的分组标志
    downsampleData = group1.groupby(by=groupIndex).agg(methodAgg)  # 将聚合方法应用到各组
    return downsampleData


def min2Nmin(minDFOri,n):
    '''
    这个是将1min数据聚合成n分钟数据的实质操作函数。算法是基于n条线聚合在一起。每天末尾不够的就不够了
    :param minDFOri:
    :param n:
    :return:
    '''
    # minDF = df1Min.reset_index()
    minDF = minDFOri.reset_index() # 为了不改变原来的df
    stockCode = minDF.iloc[0,minDF.columns.get_loc('CODE')]
    columnName = list(minDF.columns)
    methodAgg = dict(zip(list(minDF.columns[2:]), ['first', 'max', 'min', 'last'] + ['sum'] * len(minDF.columns[6:])))
    minDF.drop(['CODE'], axis=1, inplace=True)
    minDF['date'] = minDF['DATETIME'].apply(lambda x: x.date())

    minDF[minDF['date'] == dt.date(2017,11,24)].sort_values('DATETIME')

    result = minDF.groupby('date', group_keys=False).apply(lambda x: _downsample_2_n(x, n, methodAgg))
    result['CODE'] = stockCode
    result.reset_index(inplace=True)
    result = result[columnName]
    result.set_index(['DATETIME','CODE'],inplace=True)
    return result


def converge_call_auction(df1Min):

    '''
    将输入的df集合竞价那条线集合到9：31分那里。
    :param df:
    :return: 没有返回，只是对df进行处理。
    '''
    df1Min.reset_index(inplace=True)
    df1Min['time'] = df1Min['DATETIME'].apply(lambda x: x.time())
    # 将9：30那根归到9：31那里去
    for k in df1Min[df1Min['time'] == dt.time(9, 30, 0)].index:
        df1Min.iat[k + 1, df1Min.columns.get_loc('OPEN')] = df1Min.iat[k, df1Min.columns.get_loc('OPEN')]
        df1Min.iat[k + 1, df1Min.columns.get_loc('HIGH')] \
            = max(df1Min.iat[k + 1, df1Min.columns.get_loc('HIGH')],
                  df1Min.iat[k, df1Min.columns.get_loc('HIGH')]
                  )
        df1Min.iat[k + 1, df1Min.columns.get_loc('LOW')] \
            = min(df1Min.iat[k + 1, df1Min.columns.get_loc('LOW')],
                  df1Min.iat[k, df1Min.columns.get_loc('LOW')]
                  )
        df1Min.iat[k + 1, df1Min.columns.get_loc('VOLUME')] \
            = df1Min.iat[k + 1, df1Min.columns.get_loc('VOLUME')] + df1Min.iat[k, df1Min.columns.get_loc('VOLUME')]
        df1Min.iat[k + 1, df1Min.columns.get_loc('AMOUNT')] \
            = df1Min.iat[k + 1, df1Min.columns.get_loc('AMOUNT')] + df1Min.iat[k, df1Min.columns.get_loc('AMOUNT')]

    # 去掉0930
    rst = df1Min[df1Min['time'] != dt.time(9, 30, 0)]
    rst.drop('time', axis=1, inplace=True)
    rst.set_index(['DATETIME', 'CODE'], inplace=True)
    return rst


# --------------------EOD数据-------------------------------------------------------------------------------------------
def trade_dates(endTime=const.TODAY):
    '''
    这个是更新交易日序列的便利函数
    :param endTime:
    :return:
    '''
    print('AShareTradeDate开始更新！')
    latestDateInTdate = fetch.latest_date('AShareTradeDate')
    if parse(endTime).date() <= latestDateInTdate.date():
        print('AShareTradeDate不需要更新！\n')
        return
    elif latestDateInTdate == dt.datetime(2005,1,1):
        nextDateStr = '20050101'
    else:
        startTime = (latestDateInTdate+dt.timedelta(1)).strftime('%Y%m%d')
        endTime = endTime.replace('-','')
        queryStr = f'select TRADE_DAYS from AShareCalendar where S_INFO_EXCHMARKET = \'SSE\' ' \
                   f'and TRADE_DAYS >=\'{startTime}\' and TRADE_DAYS <=\'{endTime}\' order by TRADE_DAYS asc '
        tradeDateDf = _winddb_command_df(queryStr)
        tradeDateDf.rename(columns={'TRADE_DAYS':'DATETIME'},inplace=True)
        tradeDateDf['DATETIME'] = tradeDateDf['DATETIME'].apply(lambda x:parse(x))
        _to_sql(tradeDateDf, 'AShareTradeDate', if_exists='append',index=False)
        del tradeDateDf
    print(f'AShareTradeDate更新结束！期间是{startTime}至{endTime}！\n')
    return


def eod_ashare(endTime=const.TODAY):
    '''
    这个是每天全A股行情数据的便利函数
    :param endTime:
    :return:
    '''
    print('eod_ashare开始更新！')
    latestDate = fetch.latest_date('eod_ashare')
    if parse(endTime).date() <= latestDate.date():
        print('eod_ashare不需要更新！\n')
        return
    else:
        nextDateStr = (latestDate + dt.timedelta(1)).strftime('%Y%m%d')
        endTime = endTime.replace('-', '')
        command_str = f'select S_INFO_WINDCODE, TRADE_DT, ' \
                      f'S_DQ_PRECLOSE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE, ' \
                      f'S_DQ_ADJPRECLOSE, S_DQ_ADJOPEN, S_DQ_ADJHIGH, S_DQ_ADJLOW, S_DQ_ADJCLOSE, ' \
                      f'S_DQ_ADJFACTOR, S_DQ_VOLUME, S_DQ_AMOUNT, S_DQ_AVGPRICE, S_DQ_TRADESTATUS ' \
                      f'from AShareEODPrices ' \
                      f'WHERE TRADE_DT>=\'{nextDateStr}\' and TRADE_DT<=\'{endTime}\''
        df = _winddb_command_df(command_str)
        if df.empty:
            print('eod_ashare不需要更新！\n')
            return

        _convert_datetime_code(df)
        _exclude_DQ(df)
        _to_sql(df, 'eod_ashare', index=True, if_exists='append')
        del df
        print(f'eod_ashare更新结束！期间是{nextDateStr}至{endTime}！\n')
    return


def eod_indexes(endTime=const.TODAY):
    '''
    这是更新各个指数行情数据的便利函数。
    :param endTime:
    :return:
    '''
    updateDict = {'eod_index_000300SH': '000300.SH',
                  'eod_index_000016SH': '000016.SH',
                  'eod_index_000905SH': '000905.SH',
                  'eod_index_000001SH': '000001.SH',
                  'eod_index_399001SZ': '399001.SZ',
                  'eod_index_399005SZ': '399005.SZ',
                  'eod_index_399006SZ': '399006.SZ',
                  'eod_index_000906SH': '000906.SH',
                  'eod_index_881001WI': '881001.WI',}
    for tableName in updateDict:
        # tableName = 'eod_index_000906SH'
        print(f'{tableName}开始更新！')
        latestDate = fetch.latest_date(tableName)
        if latestDate is None:
            latestDate = dt.datetime(2005,1,1)

        if parse(endTime).date() <= latestDate.date():
            print(f'{tableName}不需要更新！\n')
            continue
        else:
            nextDateStr = (latestDate + dt.timedelta(1)).strftime('%Y%m%d')
            endTime = endTime.replace('-', '')

            queryStr = f'select S_INFO_WINDCODE,trade_dt, ' \
                       f's_dq_preclose,s_dq_open,s_dq_high,s_dq_low,s_dq_close,s_dq_pctchange, ' \
                       f's_dq_volume,s_dq_amount ' \
                       f'from AIndexEODPrices where S_INFO_WINDCODE = \'{updateDict[tableName]}\' ' \
                       f'and TRADE_DT >=\'{nextDateStr}\' and TRADE_DT <=\'{endTime}\'' \
                       f'order by TRADE_DT asc'
            df = _winddb_command_df(queryStr)
            _convert_2_capital(df)
            _convert_datetime_code(df)
            _exclude_DQ(df)
            _to_sql(df,tableName,index=True,if_exists='append')
            del df
            print(f'{tableName}更新结束！期间是{nextDateStr}至{endTime}！\n')
    return


def eod_induIndex_citics(endTime=const.TODAY):
    '''
    这个是更新中信行业指数的日行情的便利函数。
    值得注意的是中信行业会进行调整，例如2014年6月30日之后CI005365这个指数就没有了。
    在交叉比对名字和代码的时候，采用的对照表是最新的，所以没有CI005365对应中文名字。
    这个可以用来标识被取消了的中信指数。
    :param endTime:
    :return:
    '''
    latestDate = fetch.latest_date('eod_induIndex_citics')
    print('开始更新eod_induIndex_citics！')
    if parse(endTime).date() <= latestDate.date():
        print('eod_induIndex_citics不需要更新！\n')
        return
    else:
        nextDateStr = (latestDate + dt.timedelta(1)).strftime('%Y%m%d')
        endTime = endTime.replace('-','')

        nameCMD = f'select s_info_indexcode,s_info_name,s_info_industrycode,s_info_industryname ' \
                  f'from IndexContrastSector where S_INFO_INDEXCODE like \'CI005%WI\' ' \
                  f'order by S_INFO_INDEXCODE'
        nameDF = _winddb_command_df(nameCMD)
        _convert_2_capital(nameDF)

        def _citics_level(str):
            if str[5] == '0':
                return 1
            elif str[5] == '1':
                return 2
            elif str[5] == '2' or str[5] == '3':
                return 3
            else:
                return None
        nameDF['level'] = nameDF['S_INFO_INDEXCODE'].apply(_citics_level)

        queryStr = f'select * ' \
                   f'from AIndexIndustriesEODCITICS where TRADE_DT >=\'{nextDateStr}\' and TRADE_DT <=\'{endTime}\' ' \
                   f'order by TRADE_DT asc'
        df = _winddb_command_df(queryStr)
        df.drop(['OBJECT_ID'], inplace=True, axis=1)
        df.drop(['OPMODE', 'OPDATE'], inplace=True, axis=1)
        df.drop(['CRNCY_CODE'], inplace=True, axis=1)
        _exclude_DQ(df)
        _convert_datetime_code(df)
        df.reset_index(inplace=True)

        df = pd.merge(df,nameDF,how='left',left_on='CODE',right_on='S_INFO_INDEXCODE')
        df.drop(['S_INFO_INDEXCODE','S_INFO_INDUSTRYNAME'],axis=1,inplace=True)
        _to_sql(df,'eod_induIndex_citics',index=False,if_exists='append')
        print(f'eod_induIndex_citics更新完毕！期间是{nextDateStr}至{endTime}！\n')
        del df, nameDF
        return None


def eod_index_wind(endTime=const.TODAY):
    '''
    这个是用来更新wind行业、wind主题和wind概念指数行情的便利函数。
    :param endTime:
    :return:
    '''
    updateDict = {'eod_induIndex_wind': '行业',
                  'eod_conceptIndex_wind': '概念',
                  'eod_themeIndex_wind': '主题'}
    for tableName in updateDict:
        # tableName = 'eod_induIndex_wind'
        if updateDict[tableName] == '行业':
            windPrefix = '882'
        elif updateDict[tableName] == '概念':
            windPrefix = '884'
        elif updateDict[tableName] == '主题':
            windPrefix = '886'
        print(f'开始更新wind{updateDict[tableName]}指数！')
        latestDate = fetch.latest_date(tableName)
        if parse(endTime).date() <= latestDate.date():
            print(f'wind{updateDict[tableName]}指数不需要更新！\n')
            continue
        else:
            nextDateStr = (latestDate + dt.timedelta(1)).strftime('%Y%m%d')
            endTime = endTime.replace('-','')

            nameCMD = f'select s_info_indexcode,s_info_name,s_info_industrycode,s_info_industryname ' \
                      f'from IndexContrastSector where S_INFO_INDEXCODE like \'{windPrefix}%\' ' \
                      f'order by S_INFO_INDEXCODE'
            nameDF = _winddb_command_df(nameCMD)
            _convert_2_capital(nameDF)

            if updateDict[tableName] == '主题':
                nameDF.set_value(len(nameDF),
                                 ['S_INFO_INDEXCODE','S_INFO_NAME','S_INFO_INDUSTRYCODE','S_INFO_INDUSTRYNAME'],
                                 ['886068.WI','工程机械指数',None,'工程机械指数'])
                nameDF.set_value(len(nameDF),
                                 ['S_INFO_INDEXCODE', 'S_INFO_NAME', 'S_INFO_INDUSTRYCODE', 'S_INFO_INDUSTRYNAME'],
                                 ['886069.WI', '石油化工指数', None, '石油化工指数'])
            elif updateDict[tableName] == '行业':
                def wind_level(str):
                    if str[3] == '0':
                        return 1
                    elif str[3] == '1':
                        return 2
                    elif str[3] == '2':
                        return 3
                    elif str[3] == '4' or str[3] == '5' or str[3] == '6':
                        return 4
                    else:
                        return None
                nameDF['level'] = nameDF['S_INFO_INDEXCODE'].apply(wind_level)

            queryStr = f'select S_INFO_WINDCODE, TRADE_DT, S_DQ_PRECLOSE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE, ' \
                       f'S_DQ_PCTCHANGE, S_DQ_VOLUME, S_DQ_AMOUNT ' \
                       f'from AIndexWindIndustriesEOD where S_INFO_WINDCODE like \'{windPrefix}%\' ' \
                       f'and TRADE_DT >=\'{nextDateStr}\' and TRADE_DT <=\'{endTime}\' ' \
                       f'order by TRADE_DT asc'
            df = _winddb_command_df(queryStr)
            _exclude_DQ(df)
            _convert_datetime_code(df)

            df.reset_index(inplace=True)
            df = pd.merge(df,nameDF,how='left',left_on='CODE',right_on='S_INFO_INDEXCODE')
            df.drop(['S_INFO_INDEXCODE','S_INFO_INDUSTRYNAME'],axis=1,inplace=True)
            _to_sql(df,tableName,if_exists='append',index=False)
            del df, nameDF
            print(f'wind{updateDict[tableName]}更新结束！期间是{nextDateStr}至{endTime}！\n')
    return None


def eod_citics_industry_type(endTime=const.TODAY):
    '''
    这个是更新每日行业指数组成的便利的函数，形式是每日各个股票的中信行业分类
    :param endTime:
    :return:
    '''
    print('开始更新日频中信行业分类！')
    latestDate = fetch.latest_date('eod_ashare_industry_type_citics')
    if parse(endTime).date() <= latestDate.date():
        print('日频中信行业分类不需要更新！\n')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        nextDateStr = '2005-01-01'
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y-%m-%d')

    from WindPy import w
    tradeDates = fetch.trade_dates(startTime=nextDateStr,endTime=endTime)

    dfName = fetch._db_command_2_df('select distinct CODE, S_INFO_NAME, level from eod_induindex_citics')
    dfName['S_INFO_NAME'] = dfName['S_INFO_NAME'].apply(lambda x: None if x is None else x[:-4])
    dictLevel1 = dict(zip(dfName.loc[dfName['level'] == 1.0,'S_INFO_NAME'],dfName.loc[dfName['level'] == 1.0,'CODE']))
    dictLevel2 = dict(
        zip(dfName.loc[dfName['level'] == 2.0, 'S_INFO_NAME'], dfName.loc[dfName['level'] == 2.0, 'CODE']))
    dictLevel3 = dict(
        zip(dfName.loc[dfName['level'] == 3.0, 'S_INFO_NAME'], dfName.loc[dfName['level'] == 3.0, 'CODE']))

    w.start()
    for day in tradeDates['DATETIME']:
        # day = tradeDates['DATETIME'].iat[0]
        AllACodes = w.wset("sectorconstituent",
                           f"date={day.strftime('%Y%m%d')};sectorid=a001010100000000")
        AllACodesStr = ','.join(AllACodes.Data[1])

        MAllANextTDay = w.wss(AllACodesStr, "industry_citic",
                              f"tradeDate={day.strftime('%Y%m%d')};industryType=4")
        dfIndu = pd.DataFrame(MAllANextTDay.Data, index=MAllANextTDay.Fields, columns=MAllANextTDay.Codes).T
        dfIndu.reset_index(inplace=True)
        dfIndu.rename(columns={'index':'CODE'},inplace=True)
        dfIndu = dfIndu.loc[dfIndu['CODE'].apply(
            lambda x:True if x[:2] == '00' else (True if x[:2] == '30' else (True if x[:2] == '60' else False))
                                                )
                           ]
        dfIndu['DATETIME'] = day
        dfIndu['INDUSTRY_CITIC_1'] = dfIndu['INDUSTRY_CITIC'].apply(lambda x:x.split('-')[0])
        dfIndu['INDUSTRY_CITIC_CODE_1'] = dfIndu['INDUSTRY_CITIC_1'].map(dictLevel1)
        dfIndu['INDUSTRY_CITIC_2'] = dfIndu['INDUSTRY_CITIC'].apply(lambda x: x.split('-')[1])
        dfIndu['INDUSTRY_CITIC_CODE_2'] = dfIndu['INDUSTRY_CITIC_2'].map(dictLevel2)
        dfIndu['INDUSTRY_CITIC_3'] = dfIndu['INDUSTRY_CITIC'].apply(lambda x: x.split('-')[2])
        dfIndu['INDUSTRY_CITIC_CODE_3'] = dfIndu['INDUSTRY_CITIC_3'].map(dictLevel3)
        dfIndu.drop(['INDUSTRY_CITIC'],axis=1,inplace=True)

        _to_sql(dfIndu,'eod_ashare_industry_type_citics',if_exists='append',index=False)

    w.stop()
    del dictLevel1,dictLevel2,dictLevel3,dfName,dfIndu
    print(f'日频中信行业分类更新完毕！期间是{nextDateStr}至{endTime}！\n')
    return None


def eod_index_constitution(endTime=const.TODAY):
    '''
    更新日频的普通指数的成份股，表里面只有三列，DATETIME，CODE和中股票的中文名。
    :param endTime:
    :return:
    '''
    tableAndSectorDict = {'eod_index_constitution_sz50' : '1000000087000000',
                          'eod_index_constitution_zz800': '1000011893000000',
                          'eod_index_constitution_hs300': '1000000090000000',
                          'eod_index_constitution_zz500': '1000008491000000'}
    for tableName in tableAndSectorDict:
        # tableName = 'eod_index_constitution_sz50'
        latestDate = fetch.latest_date(tableName)
        print(f'开始更新日频{tableName}！')
        if parse(endTime).date() <= latestDate.date():
            print(f'日频{tableName}不需要更新！\n')
            continue
        elif latestDate == dt.datetime(2005,1,1):
            nextDateStr = '2005-01-01'
        else:
            nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y-%m-%d')

        w.start()
        tdays = fetch.trade_dates(startTime=nextDateStr,endTime=endTime)
        for day in tdays['DATETIME']:
            # day = tdays['DATETIME'].iat[0]
            dataWind = w.wset("sectorconstituent","date={};sectorid={}".format(
                                            day.strftime('%Y-%m-%d'),tableAndSectorDict[tableName]
                                                                              ))
            if dataWind.Data == []:
                continue
            dfEodIndex = pd.DataFrame(dataWind.Data, index=dataWind.Fields, columns=dataWind.Codes).T
            dfEodIndex.rename(columns={'date':'DATETIME','wind_code':'CODE'},inplace=True)
            dfEodIndex['DATETIME'] = dfEodIndex['DATETIME'].apply(lambda x:x.replace(microsecond=0))
            dfEodIndex = dfEodIndex.loc[dfEodIndex['CODE'].apply(lambda x: x[:2] == '60' or x[:2] == '30' or x[:2] == '00')]
            dfEodIndex.set_index(['DATETIME','CODE'],inplace=True)
            _to_sql(dfEodIndex,tableName,index=True,if_exists='append')
            del dfEodIndex
        print(f'日频{tableName}更新完毕！期间是{nextDateStr}至{endTime}！\n')
        w.stop()
    return None


# --------------------分钟数据更新函数------------------------------------------------------------------------------------
def _get_stock_min_windAPI(code, startTime, endTime=const.TODAY, barSize=1):
    '''
    这个是从windAPI取分钟线数据并且进行相应规整处理的函数。
    :param code:
    :param startTime:
    :param endTime:
    :param barSize:
    :return:
    '''
    # 从windAPI中取数据
    w.start()
    dfWind = w.wsi(code, "open,high,low,close,volume,amt",
                   startTime + " 09:00:00", endTime + " 16:00:00", f"BarSize={barSize}")
    w.stop()

    # windAPI错误处理
    if dfWind.ErrorCode == -40520007:
        print(f'{code}在这段时间内没有数据，跳过')
        return None
    elif dfWind.ErrorCode == -40521004:
        print('无法发送请求，30s后重连')
        time.sleep(30)
        w.start()
        dfWind = w.wsi(code, "open,high,low,close,volume,amt",
                       startTime + " 09:00:00", endTime + " 16:00:00", f"BarSize={barSize}")
        w.stop()
        if dfWind.ErrorCode == -40521004:
            raise ValueError(f'WindError: {dfWind.ErrorCode} {dfWind.Data[0][0]}')
    elif dfWind.ErrorCode == -40522017:
        raise ValueError('数据超限')
    elif dfWind.ErrorCode != 0:
        raise ValueError(f'错误代码{dfWind.ErrorCode}')

    df = pd.DataFrame(dfWind.Data,index=dfWind.Fields,columns=dfWind.Times).T
    df.reset_index(inplace=True)
    df.rename(columns={'index':'DATETIME'},inplace=True)
    _convert_2_capital(df)
    df['DATETIME'] = df['DATETIME'].apply(lambda x:x.replace(microsecond=0))
    df['DATETIME'] = df['DATETIME'] + dt.timedelta(minutes=1) # 调整为右标记

    df['time'] = df['DATETIME'].apply(lambda x:x.time())

    # 处理0931
    for k in df[df['time'] == dt.time(9, 31, 0)].index: # 开盘集合竞价
        try:
            if df.iat[k - 1, 7] <= dt.time(9, 30, 0) and not(np.isnan(df.iat[k - 1, 1])):
                df.iat[k - 1, 0] = df.iat[k - 1, 0].replace(minute=30)
                df.iat[k - 1, 7] = dt.time(9, 30, 0)
            elif df.iat[k - 1, 7] <= dt.time(9, 30, 0) and np.isnan(df.iat[k - 1, 1]):
                df.iat[k - 1, 0] = df.iat[k - 1, 0].replace(minute=30)
                df.iat[k - 1, 7] = dt.time(9, 30, 0)
                df.iat[k - 1, 1] = df.iat[k, 1] # 没有集合竞价则用连续竞价第一笔
                df.iat[k - 1, 1] = df.iat[k, 1]
                df.iat[k - 1, 1] = df.iat[k, 1]
                df.iat[k - 1, 1] = df.iat[k, 1]
                df.iat[k - 1, 5] = 0.0
                df.iat[k - 1, 6] = 0.0
        except:
            pass

    if code[-2:] == 'SH' or code[-2:] == 'sh':
        # 处理1501，上海的是大宗交易
        df['VOLUME'].fillna(0, inplace=True)
        df['AMOUNT'].fillna(0, inplace=True)
        df.fillna(method='ffill', inplace=True)
        for k in df[df['time'] == dt.time(15, 00, 0)].index:
            try:
                if df.iat[k + 1, 7] >= dt.time(15, 1, 0) and not(np.isnan(df.iat[k + 1, 1])):
                    df.iat[k, 5] = df.iat[k, 5] + df.iat[k + 1, 5]
                    df.iat[k, 6] = df.iat[k, 6] + df.iat[k + 1, 6]
                    df.iat[k, 4] = df.iat[k + 1, 4]
                    df.iat[k, 2] = max(df.iat[k, 2], df.iat[k + 1, 2])
                    df.iat[k, 3] = min(df.iat[k, 3], df.iat[k + 1, 3])
            except:
                pass
            # 以防后面还有一根K线
            try:
                if df.iat[k + 2, 7] >= dt.time(15, 1, 0) and not(np.isnan(df.iat[k + 2, 1])):
                    df.iat[k, 5] = df.iat[k, 5] + df.iat[k + 2, 5]
                    df.iat[k, 6] = df.iat[k, 6] + df.iat[k + 2, 6]
                    df.iat[k, 4] = df.iat[k + 2, 4]
                    df.iat[k, 2] = max(df.iat[k, 2], df.iat[k + 2, 2])
                    df.iat[k, 3] = min(df.iat[k, 3], df.iat[k + 2, 3])
            except:
                pass
    # 处理深圳的1501，大宗和尾盘集合竞价都在1501这根线
    elif code[-2:] == 'SZ' or code[-2:] == 'sz':
        for k in df[df['time'] == dt.time(15, 00, 0)].index:
            try:
                # 将1501那一根直接搬过来
                if df.iat[k + 1, 7] >= dt.time(15, 1, 0) and not(np.isnan(df.iat[k + 1, 1])):
                    df.iat[k, 5] = df.iat[k + 1, 5]
                    df.iat[k, 6] = df.iat[k + 1, 6]
                    df.iat[k, 4] = df.iat[k + 1, 4]
                    df.iat[k, 2] = df.iat[k + 1, 2]
                    df.iat[k, 3] = df.iat[k + 1, 3]
                    df.iat[k, 1] = df.iat[k + 1, 1]
            except:
                pass
            # 以防后面还有一根K线
            try:
                if df.iat[k + 2, 7] >= dt.time(15, 1, 0) and not(np.isnan(df.iat[k + 2, 1])):
                    df.iat[k, 5] = df.iat[k, 5] + df.iat[k + 2, 5]
                    df.iat[k, 6] = df.iat[k, 6] + df.iat[k + 2, 6]
                    df.iat[k, 4] = df.iat[k + 2, 4]
                    df.iat[k, 2] = max(df.iat[k, 2], df.iat[k + 2, 2])
                    df.iat[k, 3] = min(df.iat[k, 3], df.iat[k + 2, 3])
            except:
                pass

    # 去掉0926,1501等
    df = df[((df['time'] >= dt.time(9, 30, 0)) & (df['time'] <= dt.time(11, 30, 00))) |
            ((df['time'] >= dt.time(13, 1, 0)) & (df['time'] <= dt.time(15, 0, 00)))
            ]

    df.index = range(len(df.index))

    # 填补空白K线，包括尾盘集合竞价缺的
    for k in df[np.isnan(df['OPEN'])].index:
        df.iat[k, 1] = df.iat[k - 1, 4]
        df.iat[k, 2] = df.iat[k - 1, 4]
        df.iat[k, 3] = df.iat[k - 1, 4]
        df.iat[k, 4] = df.iat[k - 1, 4]
        df.iat[k, 5] = 0.0
        df.iat[k, 6] = 0.0

    df.drop('time',axis=1,inplace=True)
    df['CODE'] = code
    df.set_index(['DATETIME','CODE'],inplace=True)

    return df


# code = 'IF00.CFE'
def _get_future_stock_index_min_windAPI(code, startTime, endTime=const.TODAY, barSize=1):
    # windAPI的分钟线数据是左标记，左包含的。就是说09:41:00-09:41:59的数据归在09:41:00这根分钟线这里
    # 从windAPI中取数据
    w.start()
    dfWind = w.wsi(code, "open,high,low,close,volume,amt,oi",
                   startTime, endTime, f"BarSize={barSize}")
    w.stop()

    # windAPI错误处理
    if dfWind.ErrorCode == -40520007:
        print(f'{code}在这段时间内没有数据，跳过')
        return None
    elif dfWind.ErrorCode == -40521004:
        print('无法发送请求，30s后重连')
        time.sleep(30)
        w.start()
        dfWind = w.wsi(code, "open,high,low,close,volume,amt,chg,pct_chg,oi",
                       startTime, endTime, f"BarSize={barSize}")
        w.stop()
        if dfWind.ErrorCode == -40521004:
            raise ValueError(f'WindError: {dfWind.ErrorCode} {dfWind.Data[0][0]}')
    elif dfWind.ErrorCode == -40522017:
        raise ValueError('数据超限')
    elif dfWind.ErrorCode != 0:
        raise ValueError(f'错误代码{dfWind.ErrorCode}')

    # 变成df
    df = pd.DataFrame(dfWind.Data,index=dfWind.Fields,columns=dfWind.Times).T
    df.reset_index(inplace=True)
    df.rename(columns={'index':'DATETIME'},inplace=True)

    df['DATETIME'] = df['DATETIME'].apply(lambda x: x.replace(microsecond=0))
    df['DATETIME'] = df['DATETIME'] + dt.timedelta(minutes=1)  # 调整为右标记

    df.rename(columns={'index': 'DATETIME'}, inplace=True)
    _convert_2_capital(df)

    df['time'] = df['DATETIME'].apply(lambda x: x.time())
    # 处理0931
    for k in df[df['time'] == dt.time(15, 1, 0)].index:  # 尾盘最后1秒放进去
        try:
            if not np.nan(df.iat[k, df.columns.get_loc('VOLUME')]) and df.iat[k, df.columns.get_loc('VOLUME')] != 0.0:
                df.iat[k - 1, df.columns.get_loc('VOLUME')] \
                    = df.iat[k - 1, df.columns.get_loc('VOLUME')] + df.iat[k, df.columns.get_loc('VOLUME')]
                df.iat[k - 1, df.columns.get_loc('AMOUNT')] \
                    = df.iat[k - 1, df.columns.get_loc('AMOUNT')] + df.iat[k, df.columns.get_loc('AMOUNT')]
                df.iat[k - 1, df.columns.get_loc('POSITION')] = df.iat[k, df.columns.get_loc('POSITION')]

                df.iat[k - 1, df.columns.get_loc('HIGH')] \
                    = max(df.iat[k - 1, df.columns.get_loc('HIGH')], df.iat[k, df.columns.get_loc('HIGH')])
                df.iat[k - 1, df.columns.get_loc('LOW')] \
                    = min(df.iat[k - 1, df.columns.get_loc('LOW')], df.iat[k, df.columns.get_loc('LOW')])
                df.iat[k - 1, df.columns.get_loc('CLOSE')] \
                    = df.iat[k, df.columns.get_loc('HIGH')]
        except:
            pass

    # 去掉0926,1501等
    df = df[((df['time'] >= dt.time(9, 30, 0)) & (df['time'] <= dt.time(11, 30, 00))) |
            ((df['time'] >= dt.time(13, 1, 0)) & (df['time'] <= dt.time(15, 0, 00)))
            ]

    df.index = range(len(df.index))

    # 填补空白K线，包括没有成交和熔断的
    for k in df[np.isnan(df['OPEN'])].index:
        df.iat[k, df.columns.get_loc('OPEN')] = df.iat[k - 1, df.columns.get_loc('CLOSE')]
        df.iat[k, df.columns.get_loc('HIGH')] = df.iat[k - 1, df.columns.get_loc('CLOSE')]
        df.iat[k, df.columns.get_loc('LOW')] = df.iat[k - 1, df.columns.get_loc('CLOSE')]
        df.iat[k, df.columns.get_loc('CLOSE')] = df.iat[k - 1, df.columns.get_loc('CLOSE')]
        df.iat[k, df.columns.get_loc('POSITION')] = df.iat[k - 1, df.columns.get_loc('POSITION')]

        df.iat[k, df.columns.get_loc('VOLUME')] = 0.0
        df.iat[k, df.columns.get_loc('AMOUNT')] = 0.0

    df.drop('time', axis=1, inplace=True)
    df['CODE'] = code
    df.set_index(['DATETIME', 'CODE'], inplace=True)

    # 处理一下数据类型，节省空间
    df['OPEN'] = df['OPEN'].astype(np.float16)
    df['HIGH'] = df['HIGH'].astype(np.float16)
    df['LOW'] = df['LOW'].astype(np.float16)
    df['CLOSE'] = df['OPEN'].astype(np.float16)
    df['VOLUME'] = df['VOLUME'].astype(np.uint16)
    df['AMOUNT'] = df['AMOUNT'].astype(np.uint64)
    df['POSITION'] = df['POSITION'].astype(np.uint32)

    return df


def index_1min(indexCode, endTime=const.TODAY):
    # indexCode = '399006.SZ'
    tableName = indexCode.replace('.','').lower() + '_1min'
    latestDate = fetch.latest_date(tableName,db='stock_min')
    print(f'{tableName}开始更新！')

    # 根据相应日期从windAPI下载数据
    if parse(endTime).date() <= latestDate.date():
        print(f'{tableName}不需要更新！\n')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        df = _get_stock_min_windAPI(indexCode, startTime='2014-01-01', endTime=endTime)
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y-%m-%d')
        df = _get_stock_min_windAPI(indexCode, startTime=nextDateStr, endTime=endTime)

    if df is None:
        print(f'{indexCode}没有数据，跳过！\n')
        return None
    else:
        _to_sql(df, indexCode.replace('.', '').lower() + '_1min', DATABASE='stock_min', index=True, if_exists='append')
        print(f'{tableName}更新完毕，期间是{nextDateStr}至{endTime}！\n')


def stock_1min_API(code, endTime=const.TODAY):
    tableName = code.replace('.','').lower() + '_1min'
    latestDate = fetch.latest_date(tableName,db='stock_min')
    print(f'{tableName}开始更新')

    # 根据相应日期从windAPI下载数据
    if parse(endTime).date() <= latestDate.date():
        print(f'{tableName}不需要更新！\n')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        nextDateStr = '2014-01-01'
        df = _get_stock_min_windAPI(code, startTime='2014-01-01', endTime=endTime)
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y-%m-%d')
        df = _get_stock_min_windAPI(code, startTime=nextDateStr, endTime=endTime)

    if df is None:
        print(f'{code}没有数据，跳过！\n')
        return None
    else:
        _to_sql(df, code.replace('.', '').lower() + '_1min', DATABASE='stock_min', index=True, if_exists='append')
        print(f'{tableName}更新完毕，期间是{nextDateStr}至{endTime}！\n')


def n_mins_fromDB(stockCode,n,endTime=const.TODAY):
    '''
    这是用来更新指数或者个股n分钟数据的便利函数
    :param targetStocks: str
    :param n: int
    :param endTime:
    :return:
    '''
    print(f'{stockCode}的{n}分钟数据开始更新！')
    latestDateInNmin = fetch.latest_date('{stock}_{n}min'.format(stock=stockCode.replace('.',''),n=n),
                                         db='stock_min')
    if latestDateInNmin == dt.datetime(2005, 1, 1):
        df1Min = fetch.n_min(stockCode,n=1)
        if df1Min.empty:
            print('源数据这段时间没有数据！\n')
            return None

        df1Min = converge_call_auction(df1Min)
        dfNMin = min2Nmin(df1Min,n)

        _to_sql(dfNMin, '{}_{}min'.format(stockCode.replace('.', ''),n),
                DATABASE='stock_min', index=True, if_exists='append')
        print(f'{stockCode}的{n}分钟数据更新完毕！')
    else:
        nextDateStr = (latestDateInNmin + dt.timedelta(1)).strftime('%Y-%m-%d')
        df1Min = fetch.n_min(stockCode, startTime=nextDateStr,endTime=endTime)
        if df1Min.empty:
            print(f'{stockCode}的{n}分钟数据不需要更新！')
            return None
        else:
            df1Min = converge_call_auction(df1Min)
            dfNMin = min2Nmin(df1Min, n)
            _to_sql(dfNMin, '{}_{}min'.format(stockCode.replace('.', ''), n),
                    DATABASE='stock_min', index=True, if_exists='append')
            print(f'{stockCode}的{n}分钟数据更新完毕！更新时间是{nextDateStr}到{endTime}')


# -------------------事件数据--------------------------------------------------------------------------------------------
# 龙虎榜
def strange_trade(endTime=const.TODAY):
    latestDate = fetch.latest_date('ashare_strangetrade', db='stock_day')
    print('开始更新A股龙虎榜！')
    if latestDate >= parse(endTime):
        print('A股龙虎榜不需要更新！')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        nextDateStr = '20000101'
        endTime = endTime.replace('-','')
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y%m%d')
        endTime = endTime.replace('-','')
    introCmd = f'select s_info_windcode, s_strange_bgdate, s_strange_enddate, ' \
               f's_strange_range, s_strange_volume, s_strange_amount, ' \
               f's_strange_tradername, s_strange_traderamount, s_strange_buyamount, s_strange_sellamount ' \
               f'from AShareStrangeTrade where s_strange_enddate >= \'{nextDateStr}\' and s_strange_enddate <= \'{endTime}\' ' \
               f'order by s_strange_enddate asc'
    introDF = _winddb_command_df(introCmd)
    # 变大写
    introDF.rename(columns=dict(zip(introDF.columns, [colName.upper() for colName in introDF.columns])), inplace=True)
    introDF.rename(columns={'S_INFO_WINDCODE': 'CODE'}, inplace=True)
    introDF.rename(columns=dict(zip(introDF.columns, [colName.replace('S_STRANGE_', '') for colName in introDF.columns])), inplace=True)
    introDF['BGDATE'] = introDF['BGDATE'].apply(lambda x: x if x is None else parse(x))
    introDF['ENDDATE'] = introDF['ENDDATE'].apply(lambda x: parse(x))
    introDF.rename(columns={'ENDDATE':'DATETIME'},inplace=True)

    _to_sql(introDF, 'ashare_strangetrade', index=False, if_exists='append')
    print(f'A股龙虎榜信息更新完毕！更新日期是{nextDateStr}到{endTime}')
    del introDF
    return None


# 大宗交易
def block_trade(endTime=const.TODAY):
    latestDate = fetch.latest_date('ashare_blocktrade',db='stock_day')
    print('开始更新A股大宗交易！')
    if latestDate >= parse(endTime):
        print('A股大宗交易不需要更新！')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        nextDateStr = '20050101'
        endTime = endTime.replace('-','')
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y%m%d')
        endTime = endTime.replace('-','')
    blockTCmd = f'select * ' \
               f'from AShareBlockTrade where trade_dt >= \'{nextDateStr}\' and trade_dt <= \'{endTime}\' ' \
               f'order by trade_dt asc'
    blockTDF = _winddb_command_df(blockTCmd)
    _exclude_DQ(blockTDF)
    blockTDF.drop(['OBJECT_ID', 'OPDATE', 'OPMODE'], axis=1, inplace=True)
    blockTDF.drop(['CRNCY_CODE'], axis=1, inplace=True)
    blockTDF.rename(columns={'S_INFO_WINDCODE': 'CODE', 'TRADE_DT': 'DATETIME'}, inplace=True)
    blockTDF.rename(columns=dict(zip(blockTDF.columns, [colName.replace('S_', '') for colName in blockTDF.columns])), inplace=True)
    _to_sql(blockTDF, 'ashare_blocktrade', index=False, if_exists='append')
    print(f'A股大宗交易更新完毕！更新时间是{nextDateStr}到{endTime}')
    del blockTDF
    return None


# 主要股东增减持
def major_holder_trade(endTime=const.TODAY):
    latestDate = fetch.latest_date('ashare_mjrholdertrade',db='stock_day')
    print('开始更新A股主要股东增减持！')
    if latestDate >= parse(endTime):
        print('A股主要股东增减持不需要更新！')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        nextDateStr = '20050101'
        endTime = endTime.replace('-','')
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y%m%d')
        endTime = endTime.replace('-','')
    mjrHolderTradeCmd = f'select * ' \
               f'from AShareMjrHolderTrade where ANN_DT >= \'{nextDateStr}\' and ANN_DT <= \'{endTime}\' ' \
               f'order by ANN_DT asc, S_INFO_WINDCODE asc'
    mjrHolderTradeDF = _winddb_command_df(mjrHolderTradeCmd)
    _exclude_DQ(mjrHolderTradeDF)
    mjrHolderTradeDF.drop(['OBJECT_ID', 'OPDATE', 'OPMODE'], axis=1, inplace=True)
    mjrHolderTradeDF.rename(columns={'ANN_DT':'DATETIME'},inplace=True)
    mjrHolderTradeDF.rename(columns={'S_INFO_WINDCODE': 'CODE'}, inplace=True)
    mjrHolderTradeDF['DATETIME'] = mjrHolderTradeDF['DATETIME'].apply(lambda x:parse(x))
    mjrHolderTradeDF['TRANSACT_STARTDATE'] = mjrHolderTradeDF['TRANSACT_STARTDATE'].apply(lambda x: x if x is None else parse(x))
    mjrHolderTradeDF['TRANSACT_ENDDATE'] = mjrHolderTradeDF['TRANSACT_ENDDATE'].apply(lambda x: x if x is None else parse(x))
    # mjrHolderTradeDF.columns
    _holderTypeDict = {'1':'个人',
                       '2':'公司',
                       '3':'高管'}
    mjrHolderTradeDF['HOLDER_TYPE'] = mjrHolderTradeDF['HOLDER_TYPE'].map(_holderTypeDict)
    mjrHolderTradeDF.drop(['TRADE_DETAIL'],axis=1,inplace=True)
    _to_sql(mjrHolderTradeDF,'ashare_mjrholdertrade',index=False, if_exists='append')
    print(f'A股主要股东增减持更新完毕！更新时间{nextDateStr}到{endTime}')
    del mjrHolderTradeDF
    return None


# 资金流向,endTime = '2017-12-31'
def money_flow(endTime=const.TODAY):
    latestDate = fetch.latest_date('ashare_moneyflow',db='stock_day')
    print('开始更新A股资金流向！')
    if latestDate >= parse(endTime):
        print('A股资金流向数据不需要更新！')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        nextDateStr = '20050101'
        endTime = endTime.replace('-','')
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y%m%d')
        endTime = endTime.replace('-','')
    moneyFlowCmd = f'select * ' \
               f'from AShareMoneyflow where trade_dt >= \'{nextDateStr}\' and trade_dt <= \'{endTime}\' ' \
               f'order by trade_dt asc, s_info_windcode asc'
    moneyFlowDF = _winddb_command_df(moneyFlowCmd)
    _exclude_DQ(moneyFlowDF)
    moneyFlowDF.drop(['OBJECT_ID', 'OPDATE', 'OPMODE'], axis=1, inplace=True)
    moneyFlowDF.rename(columns={'S_INFO_WINDCODE': 'CODE', 'TRADE_DT': 'DATETIME'}, inplace=True)
    moneyFlowDF.rename(columns=dict(zip(moneyFlowDF.columns, [colName.replace('S_', '') for colName in moneyFlowDF.columns])), inplace=True)
    moneyFlowDF['DATETIME'] = moneyFlowDF['DATETIME'].apply(lambda x: parse(x))
    _to_sql(moneyFlowDF,'ashare_moneyflow',index=False, if_exists='append')
    print(f'A股资金流向数据更新完毕！更新时间{nextDateStr}到{endTime}')
    del moneyFlowDF, moneyFlowCmd
    return None


# 业绩快报
# 按要求，公司如果已经汇总完成当期财务数据，但因为年报尚没有编制完成，可以先行对外披露业绩快报
# 业绩预告是出现重大情况是强制提前公告的
def profit_express(endTime=const.TODAY):
    latestDate = fetch.latest_date('ashare_profitexpress',db='stock_day')
    print('开始更新A股业绩快报！')
    if latestDate >= parse(endTime):
        print('A股业绩快报不需要更新！')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        nextDateStr = '20050101'
        endTime = endTime.replace('-','')
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y%m%d')
        endTime = endTime.replace('-','')
    profitExpressCmd = f'select * ' \
               f'from AShareProfitExpress where ann_dt >= \'{nextDateStr}\' and ann_dt <= \'{endTime}\' ' \
               f'order by ann_dt asc, s_info_windcode asc'
    profitExpressDF = _winddb_command_df(profitExpressCmd)
    _exclude_DQ(profitExpressDF)
    profitExpressDF.drop(['OBJECT_ID', 'OPDATE', 'OPMODE'], axis=1, inplace=True)
    profitExpressDF.rename(columns={'S_INFO_WINDCODE': 'CODE', 'ANN_DT': 'DATETIME'}, inplace=True)
    profitExpressDF['DATETIME'] = profitExpressDF['DATETIME'].apply(lambda x: parse(x))
    profitExpressDF['REPORT_PERIOD'] = profitExpressDF['REPORT_PERIOD'].apply(lambda x: parse(x))
    _to_sql(profitExpressDF,'ashare_profitexpress',index=False, if_exists='append')
    print(f'A股业绩快报更新完毕！更新时间{nextDateStr}到{endTime}')
    del profitExpressDF, profitExpressCmd

    # 业绩预告
    latestDate = fetch.latest_date('ashare_profitnotice',db='stock_day')
    print('开始更新A股业绩预告！')
    if latestDate >= parse(endTime):
        print('A股业绩预告不需要更新！')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        nextDateStr = '20050101'
        endTime = endTime.replace('-','')
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y%m%d')
        endTime = endTime.replace('-','')
    ProfitNoticeCmd = f'select * ' \
               f'from AShareProfitNotice where s_profitnotice_date >= \'{nextDateStr}\' and s_profitnotice_date <= \'{endTime}\' ' \
               f'order by s_profitnotice_date asc, s_info_windcode asc'
    ProfitNoticeDF = _winddb_command_df(ProfitNoticeCmd)

    _exclude_DQ(ProfitNoticeDF)
    ProfitNoticeDF.drop(['OBJECT_ID', 'OPDATE', 'OPMODE'], axis=1, inplace=True)
    ProfitNoticeDF.rename(columns={'S_INFO_WINDCODE': 'CODE'}, inplace=True)
    ProfitNoticeDF.rename(columns=dict(zip(ProfitNoticeDF.columns, [colName.replace('S_', '') for colName in ProfitNoticeDF.columns])), inplace=True)
    ProfitNoticeDF['PROFITNOTICE_DATE'] = ProfitNoticeDF['PROFITNOTICE_DATE'].apply(lambda x:parse(x))
    ProfitNoticeDF['PROFITNOTICE_PERIOD'] = ProfitNoticeDF['PROFITNOTICE_PERIOD'].apply(lambda x: parse(x))
    ProfitNoticeDF['PROFITNOTICE_FIRSTANNDATE'] = ProfitNoticeDF['PROFITNOTICE_FIRSTANNDATE'].apply(lambda x: x if x is None else parse(x))
    ProfitNoticeDF.rename(columns={'PROFITNOTICE_DATE':'DATETIME'},inplace=True)

    _chgDict = {454001000.0:'不确定',
                454002000.0:'略减',
                454003000.0:'略增',
                454004000.0:'扭亏',
                454005000.0:'其他',
                454006000.0:'首亏',
                454007000.0:'续亏',
                454008000.0:'续盈',
                454009000.0:'预减',
                454010000.0:'预增' }
    ProfitNoticeDF['PROFITNOTICE_STYLE'] = ProfitNoticeDF['PROFITNOTICE_STYLE'].map(_chgDict)
    ProfitNoticeDF['PROFITNOTICE_SIGNCHANGE'] = ProfitNoticeDF['PROFITNOTICE_SIGNCHANGE'].map({'1':1,'0':0})
    _to_sql(ProfitNoticeDF,'ashare_profitnotice',index=False, if_exists='append')
    print(f'A股业绩预告更新完毕！更新时间{nextDateStr}到{endTime}')
    del ProfitNoticeDF, ProfitNoticeCmd

    return None


# 盈利预测明细
def earning_estimate(endTime=const.TODAY):
    latestDate = fetch.latest_date('ashare_earningest',db='stock_day')
    print('开始更新A股盈利预测明细！')
    if latestDate >= parse(endTime):
        print('A股盈利预测明细不需要更新！')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        nextDateStr = '20050101'
        endTime = endTime.replace('-','')
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y%m%d')
        endTime = endTime.replace('-','')
    EarningEstCmd = f'select * ' \
               f'from AShareEarningEst where EST_DT >= \'{nextDateStr}\' and EST_DT <= \'{endTime}\' ' \
               f'order by EST_DT asc, s_info_windcode asc'
    EarningEstDF = _winddb_command_df(EarningEstCmd)

    EarningEstDF.drop(['OBJECT_ID', 'OPDATE', 'OPMODE'], axis=1, inplace=True)
    EarningEstDF.rename(columns={'S_INFO_WINDCODE': 'CODE'}, inplace=True)
    EarningEstDF.drop(['WIND_CODE','COLLECT_TIME','FIRST_OPTIME'],axis=1,inplace=True)
    EarningEstDF['EST_DT'] = EarningEstDF['EST_DT'].apply(lambda x:parse(x))
    EarningEstDF['REPORTING_PERIOD'] = EarningEstDF['REPORTING_PERIOD'].apply(lambda x: parse(x))
    EarningEstDF.rename(columns={'EST_DT':'DATETIME'},inplace=True)
    EarningEstDF.drop('')

    _to_sql(EarningEstDF,'ashare_earningest',index=False, if_exists='append')
    print(f'A股盈利预测明细更新完毕！更新时间{nextDateStr}到{endTime}')
    del EarningEstDF, EarningEstCmd
    return None


# 盈利预测汇总,endTime = '2007-12-31'
def earningest_consensusdata(endTime=const.TODAY):
    latestDate = fetch.latest_date('ashare_earningest_consensusdata',db='stock_day')
    print('开始更新A股盈利预测汇总！')
    if latestDate >= parse(endTime):
        print('A股盈利预测汇总不需要更新！')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        nextDateStr = '20050101'
        endTime = endTime.replace('-','')
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y%m%d')
        endTime = endTime.replace('-','')
    EarningEst_ConsensusDataCmd = f'select * ' \
               f'from AShareConsensusData where EST_DT >= \'{nextDateStr}\' and EST_DT <= \'{endTime}\' ' \
               f'order by EST_DT asc, s_info_windcode asc'
    EarningEst_ConsensusDataDF = _winddb_command_df(EarningEst_ConsensusDataCmd)

    EarningEst_ConsensusDataDF.drop(['OBJECT_ID', 'OPDATE', 'OPMODE'], axis=1, inplace=True)
    EarningEst_ConsensusDataDF.rename(columns={'S_INFO_WINDCODE': 'CODE'}, inplace=True)
    EarningEst_ConsensusDataDF.drop('WIND_CODE',axis=1,inplace=True)
    EarningEst_ConsensusDataDF['EST_DT'] = EarningEst_ConsensusDataDF['EST_DT'].apply(lambda x:parse(x))
    EarningEst_ConsensusDataDF['EST_REPORT_DT'] = EarningEst_ConsensusDataDF['EST_REPORT_DT'].apply(lambda x: parse(x))
    EarningEst_ConsensusDataDF.rename(columns={'EST_DT':'DATETIME'},inplace=True)
    _chgDict = {'263001000':'30天',
                '263002000':'90天',
                '263003000':'180天',
                '263004000':'大事后180天'}
    EarningEst_ConsensusDataDF['CONSEN_DATA_CYCLE_TYP'] = EarningEst_ConsensusDataDF['CONSEN_DATA_CYCLE_TYP'].map(_chgDict)
    _to_sql(EarningEst_ConsensusDataDF,'ashare_earningest_consensusdata',index=False, if_exists='append')
    print(f'A股盈利预测汇总更新完毕！更新时间{nextDateStr}到{endTime}')
    del EarningEst_ConsensusDataDF, EarningEst_ConsensusDataCmd
    return None


# A股投资评级明细 @todo 这个表后面没有再更新了，不确定可以跑通
def stock_rating(endTime=const.TODAY):
    latestDate = fetch.latest_date('ashare_stockrating',db='stock_day')
    print('开始更新A股投资评级明细！')
    if latestDate >= parse(endTime):
        print('A股投资评级明细不需要更新！')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        nextDateStr = '20050101'
        endTime = endTime.replace('-','')
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y%m%d')
        endTime = endTime.replace('-','')
    StockRatingCmd = f'select * ' \
                     f'from AShareStockRating ' \
                     f'where s_est_estnewtime_inst >= \'{nextDateStr}\' and s_est_estnewtime_inst <= \'{endTime}\' ' \
                     f'order by s_est_estnewtime_inst asc, s_info_windcode asc'
    StockRatingDF = _winddb_command_df(StockRatingCmd)

    StockRatingDF.drop(['OBJECT_ID', 'OPDATE', 'OPMODE'], axis=1, inplace=True)
    StockRatingDF.rename(columns={'S_INFO_WINDCODE': 'CODE'}, inplace=True)
    StockRatingDF.rename(columns=dict(zip(StockRatingDF.columns, [colName.replace('S_', '') for colName in StockRatingDF.columns])), inplace=True)
    StockRatingDF['ANN_DT'].fillna('0',inplace=True)
    StockRatingDF['ANN_DT'] = np.where(StockRatingDF['ANN_DT'] == '0', StockRatingDF['EST_ESTNEWTIME_INST'], StockRatingDF['ANN_DT'])
    StockRatingDF['EST_ESTNEWTIME_INST'] = StockRatingDF['EST_ESTNEWTIME_INST'].apply(lambda x:parse(x))
    StockRatingDF['ANN_DT'] = StockRatingDF['ANN_DT'].apply(lambda x:parse(x))
    StockRatingDF.rename(columns={'ANN_DT':'DATETIME'},inplace=True)
    _chgDict = {260001000.0:'公司研究',
                260003000.0:'行业研究',
                260004000.0:'晨会纪要',
                260005000.0:'策略研究',
                260007000.0:'其他报告',
                806004001.0:'晨会纪要',
                806004002.0:'公司研究',
                806004003.0:'行业研究',
                806004005.0:'策略研究',
                806004007.0:'市场综述'}
    StockRatingDF['EST_REPORT_TYPE'] = StockRatingDF['EST_REPORT_TYPE'].map(_chgDict)

    _to_sql(StockRatingDF,'aShare_StockRating',index=False, if_exists='append')
    print(f'A股投资评级明细更新完毕！更新时间{nextDateStr}到{endTime}')
    del StockRatingDF, StockRatingCmd
    return None


# A股投资评级汇总 @todo 这个表后面没有再更新了，不确定可以跑通
def stock_rating_consus(endTime=const.TODAY):
    latestDate = fetch.latest_date('ashare_stockratingconsus', db='stock_day')
    print('开始更新A股投资评级汇总！')
    if latestDate >= parse(endTime):
        print('A股投资评级汇总不需要更新！')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        nextDateStr = '20050101'
        endTime = endTime.replace('-','')
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y%m%d')
        endTime = endTime.replace('-','')
    StockRatingConsusCmd = f'select * ' \
               f'from AShareStockRatingConsus where rating_dt >= \'{nextDateStr}\' and rating_dt <= \'{endTime}\' ' \
               f'order by rating_dt asc, s_info_windcode asc'
    StockRatingConsusDF = _winddb_command_df(StockRatingConsusCmd)

    StockRatingConsusDF.drop(['OBJECT_ID', 'OPDATE', 'OPMODE'], axis=1, inplace=True)
    StockRatingConsusDF.rename(columns={'S_INFO_WINDCODE': 'CODE'}, inplace=True)

    StockRatingConsusDF.rename(columns=dict(zip(StockRatingConsusDF.columns, [colName.replace('S_', '') for colName in StockRatingConsusDF.columns])), inplace=True)
    StockRatingConsusDF['RATING_DT'] = StockRatingConsusDF['RATING_DT'].apply(lambda x:parse(x))
    StockRatingConsusDF.rename(columns={'RATING_DT':'DATETIME'},inplace=True)
    _chgDict = {'263001000':'30天',
                '263002000':'90天',
                '263003000':'180天'}
    StockRatingConsusDF['WRATING_CYCLE'] = StockRatingConsusDF['WRATING_CYCLE'].map(_chgDict)

    _to_sql(StockRatingConsusDF,'ashare_stockratingconsus',index=False, if_exists='append')
    print(f'A股投资评级汇总更新完毕！更新时间{nextDateStr}到{endTime}')
    del StockRatingConsusDF, StockRatingConsusCmd
    return None


# A股重大事件，endTime='2005-12-31'
def major_event(endTime=const.TODAY):
    latestDate = fetch.latest_date('ashare_majorevent', db='stock_day')
    print('开始更新A股重大事件！')
    if latestDate >= parse(endTime):
        print('A股重大事件不需要更新！')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        nextDateStr = '20050101'
        endTime = endTime.replace('-','')
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y%m%d')
        endTime = endTime.replace('-','')
    majoreventCmd = f'select * ' \
               f'from AShareMajorEvent where s_event_anncedate >= \'{nextDateStr}\' and s_event_anncedate <= \'{endTime}\' ' \
               f'order by s_event_anncedate asc, s_info_windcode asc'
    majoreventDF = _winddb_command_df(majoreventCmd)


def ashare_info():
    '''
    # 下面是更新A股的基础信息，例如名称，省市等。
    :return:
    '''
    latestDate = fetch.latest_date('name_ashare_index')
    print('开始更新代码名称对应表！')
    if latestDate != dt.datetime.now().replace(hour=0,minute=0,second=0,microsecond=0):
        # 更新股票名称代码对应
        ashareNameCmd = 'select S_INFO_WINDCODE,S_INFO_NAME ' \
                     'from AShareDescription ' \
                     'where S_INFO_WINDCODE like \'00%.SZ\' ' \
                     'or S_INFO_WINDCODE like \'30%.SZ\' ' \
                     'or S_INFO_WINDCODE like \'60%.SH\' ' \
                     'order by S_INFO_WINDCODE asc'
        desDF = _winddb_command_df(ashareNameCmd)
        desDF.rename(columns={'S_INFO_WINDCODE':'CODE','S_INFO_NAME':'NAME'},inplace=True)

        # 更新行业指数的代码和中文名
        induNameCmd = 'select s_info_indexcode, s_info_name ' \
                      'from IndexContrastSector ' \
                      'where S_INFO_INDEXCODE like \'0%\' ' \
                      'or S_INFO_INDEXCODE like \'8%\' ' \
                      'or S_INFO_INDEXCODE like \'3%\' ' \
                      'or S_INFO_INDEXCODE like \'6%\' ' \
                      'or S_INFO_INDEXCODE like \'CI%\' ' \
                      'order by S_INFO_INDEXCODE asc'
        induNameDF = _winddb_command_df(induNameCmd)
        induNameDF.rename(columns={'s_info_indexcode':'CODE',
                                   's_info_name':'NAME'},
                          inplace=True)
        induNameDF.set_value(len(induNameDF),
                             ['CODE','NAME'],
                             ['399006.SZ','创业板指'])

        rstDF = pd.concat([desDF, induNameDF])
        rstDF['DATETIME'] = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        rstDF = rstDF[['DATETIME','CODE','NAME']]

        _to_sql(rstDF,'name_ashare_index',if_exists='append',index=False)
        del rstDF, desDF, induNameDF
        print('代码名称对应表更新完毕！\n')
    else:
        print('代码名称对应表不需要更新！\n')
        return None

    return None


# --------------------期货行情数据---------------------------------------------------------------------------------------
def future_stock_index_min(indexCode, endTime=const.TODAY):
    # indexCode = 'IF00.CFE'
    tableName = indexCode.replace('.','').lower() + '_1min'
    latestDate = fetch.latest_date(tableName, db='future_min')
    print(f'{tableName}开始更新！')

    # 根据相应日期从windAPI下载数据
    if parse(endTime).date() <= latestDate.date():
        print(f'{tableName}不需要更新！\n')
        return None
    elif latestDate == dt.datetime(2005,1,1):
        df = _get_future_stock_index_min_windAPI(indexCode, startTime='2016-01-01', endTime=endTime)
    else:
        nextDateStr = (latestDate + dt.timedelta(days=1)).strftime('%Y-%m-%d')
        df = _get_stock_min_windAPI(indexCode, startTime=nextDateStr, endTime=endTime)

    if df is None:
        print(f'{indexCode}没有数据，跳过！\n')
        return None
    else:
        _to_sql(df, indexCode.replace('.', '').lower() + '_1min', DATABASE='future_min', index=True, if_exists='append')
        print(f'{tableName}更新完毕，至{endTime}！\n')


# 期货类每日日线行情
def future_stock_index_day(endTime=const.TODAY):
    print('eod_stock_index_future开始更新！')
    latestDate = fetch.latest_date('eod_stock_index_future', db='future_day')
    if parse(endTime).date() <= latestDate.date():
        print('eod_stock_index_future不需要更新！\n')
        return
    else:
        nextDateStr = (latestDate + dt.timedelta(1)).strftime('%Y%m%d')
        endTime = endTime.replace('-', '')
        command_str = f'select S_INFO_WINDCODE, TRADE_DT, ' \
                      f'S_DQ_PRESETTLE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE, S_DQ_SETTLE, ' \
                      f'S_DQ_VOLUME, S_DQ_AMOUNT, S_DQ_OI ' \
                      f'from CIndexFuturesEODPrices ' \
                      f'WHERE TRADE_DT>=\'{nextDateStr}\' and TRADE_DT<=\'{endTime}\' order by trade_dt asc'
        df = _winddb_command_df(command_str)
        if df.empty:
            print('eod_stock_index_future不需要更新！\n')
            return


        _convert_datetime_code(df)
        _exclude_DQ(df)
        _to_sql(df, 'eod_stock_index_future', index=True, if_exists='append', DATABASE='future_day')
        del df
        print(f'eod_stock_index_future更新结束！期间至{endTime}！\n')
    return


# 估值期货前20持仓买卖龙虎榜
def SPIF_top20_disclose(endTime=const.TODAY):
    print('stock_index_future_top20开始更新！')
    latestDate = fetch.latest_date('stock_index_future_top20', db='future_day')
    if parse(endTime).date() <= latestDate.date():
        print('stock_index_future_top20不需要更新！\n')
        return
    else:
        nextDateStr = (latestDate + dt.timedelta(1)).strftime('%Y%m%d')
        endTime = endTime.replace('-', '')
        command_str = f'select S_INFO_WINDCODE, TRADE_DT, ' \
                      f'fs_info_membername, fs_info_type, fs_info_positionsnum, fs_info_rank, s_oi_positionsnumc ' \
                      f'from CIndexFuturesPositions ' \
                      f'WHERE TRADE_DT>=\'{nextDateStr}\' and TRADE_DT<=\'{endTime}\' order by trade_dt asc'
        df = _winddb_command_df(command_str)
        if df.empty:
            print('stock_index_future_top20不需要更新！\n')
            return

        _convert_2_capital(df)
        _convert_datetime_code(df)
        # _exclude_DQ(df)
        _to_sql(df, 'stock_index_future_top20', index=True, if_exists='append', DATABASE='future_day')
        del df
        print(f'stock_index_future_top20更新结束！期间至{endTime}！\n')
    return


# 将指数数据补充到最早
if __name__ == '__main__':
    nextDateStr = '1990-12-01'
    endTime = '2004-12-31'

    updateDict = {'eod_index_000300SH': '000300.SH',
                  'eod_index_000016SH': '000016.SH',
                  'eod_index_000905SH': '000905.SH',
                  'eod_index_000001SH': '000001.SH',
                  'eod_index_399001SZ': '399001.SZ',
                  'eod_index_399005SZ': '399005.SZ',
                  'eod_index_399006SZ': '399006.SZ',
                  'eod_index_000906SH': '000906.SH',
                  'eod_index_881001WI': '881001.WI',}
    for tableName in updateDict:

        queryStr = f'select S_INFO_WINDCODE,trade_dt, ' \
                   f's_dq_preclose,s_dq_open,s_dq_high,s_dq_low,s_dq_close,s_dq_pctchange, ' \
                   f's_dq_volume,s_dq_amount ' \
                   f'from AIndexEODPrices where S_INFO_WINDCODE = \'{updateDict[tableName]}\' ' \
                   f'and TRADE_DT >=\'{nextDateStr}\' and TRADE_DT <=\'{endTime}\'' \
                   f'order by TRADE_DT asc'
        df = _winddb_command_df(queryStr)
        _convert_2_capital(df)
        _convert_datetime_code(df)
        _exclude_DQ(df)
        _to_sql(df, tableName, index=True, if_exists='append', DATABASE='stock_day')