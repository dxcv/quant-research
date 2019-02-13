
import pandas.io.sql as sql
import const
import MySQLdb
import pyodbc
import datetime as dt
import pandas as pd
from tools.tinytools import stock_related

# --------------------内部函数-------------------------------------------------------------------------------------------
def _db_command_2_df(queryStr,
                     host=const.HOST, port=3306, user=const.USER, passwd=const.PASSWORD,
                     db='stock_day'):
    '''
    提供读取数据库的便利函数。
    :param queryStr: 字符串，查询数据库的指令字符串
    :return: df，返回的df中有'DATETIME','CODE'，那么则将其设为index。否则Index是int
    '''
    conn = MySQLdb.Connect(
        host=host,
        port=port,
        user=user,
        passwd=passwd,
        db=db,
        charset='utf8')
    rstDf = sql.read_sql(queryStr, conn)
    conn.close()
    if 'DATETIME' in rstDf.columns and 'CODE' in rstDf.columns:
        rstDf.set_index(['DATETIME','CODE'],inplace=True)
    return rstDf


def _convert_fields(fieldStr):
    if fieldStr == '*':
        return '*'
    elif fieldStr == '不复权':
        return 'DATETIME, CODE, PRECLOSE, OPEN, HIGH, LOW, CLOSE, VOLUME, AMOUNT, AVGPRICE, ADJFACTOR, TRADESTATUS'
    elif fieldStr == '前复权':
        return 'DATETIME, CODE, PRECLOSE, OPEN, HIGH, LOW, CLOSE, VOLUME, AMOUNT, AVGPRICE, ADJFACTOR, TRADESTATUS!!!'
    elif fieldStr == '后复权':
        return 'DATETIME, CODE, ADJPRECLOSE, ADJOPEN, ADJHIGH, ADJLOW, ADJCLOSE, ' \
               'VOLUME, AMOUNT, AVGPRICE, ADJFACTOR, TRADESTATUS'
    else:
        return fieldStr


def _cal_badj(group1):
    rst = group1.copy()
    rst['BADJPRECLOSE'] = rst['BADJPRECLOSE'] / rst.iat[-1, rst.columns.get_loc('ADJFACTOR')]
    rst['BADJOPEN'] = rst['BADJOPEN'] / rst.iat[-1, rst.columns.get_loc('ADJFACTOR')]
    rst['BADJHIGH'] = rst['BADJHIGH'] / rst.iat[-1, rst.columns.get_loc('ADJFACTOR')]
    rst['BADJLOW'] = rst['BADJLOW'] / rst.iat[-1, rst.columns.get_loc('ADJFACTOR')]
    rst['BADJCLOSE'] = rst['BADJCLOSE'] / rst.iat[-1, rst.columns.get_loc('ADJFACTOR')]
    return rst


# --------------------日期相关-------------------------------------------------------------------------------------------
def trade_dates(startTime = const.DB_START, endTime = const.TODAY):
    '''
    便利函数，取出一段时间内的交易日df，含头含尾。
    :param startTime: 字符串，格式为：'2005-01-04'
    :param endTime: 字符串，格式为：'2005-01-04'
    :return: df，index为int，只有一列DATETIME，内容为交易日序列
    '''
    queryStr = f'select DATETIME from AShareTradeDate ' \
               f'where DATETIME >= \'{startTime}\' and DATETIME <= \'{endTime}\' ' \
               f'order by DATETIME asc'
    df = _db_command_2_df(queryStr)
    return df


def latest_date(tableName,db='stock_day'):
    conn = MySQLdb.connect(host=const.HOST, port=3306, user=const.USER, passwd=const.PASSWORD, db=db, charset="utf8")
    command_str = f'SELECT MAX(DATETIME) FROM {tableName}'
    cursor = conn.cursor()
    try:
        cursor.execute(command_str)
        conn.commit()
        latestDate = cursor.fetchall()[0][0].replace(minute=0,hour=0,second=0)
    except:
        print(f'表{tableName}不存在，最近时间为2005-01-01')
        latestDate = dt.datetime(2005,1,1)
    finally:
        cursor.close()
        conn.close()

    return latestDate


#--------------------取股票数据------------------------------------------------------------------------------------------
def stock_one(code, fields='*', startTime=const.DB_START, endTime=const.TODAY):
    '''
    取一个股票行情数据的便利函数
    :param code: 字符串，例如'600036'或者'600036.SH'都可以
    :param fields: 字符串，例如'*'，如果要具体的字段，必须补上'DATETIME'和'CODE'。
                   除此之外，提供复权方式参数：'不复权'，'前复权'，'后复权'。其中前复权用BADJ为前缀，后复权后ADJ为前缀。
    :param startTime: 字符串，格式为：'2005-01-04'
    :param endTime: 字符串，格式为：'2005-01-04'
    :return: df，index为'DATETIME'和'CODE'，其他列为行情数据
    '''
    code = stock_related.convert_2_normalcode(code)
    actFields = _convert_fields(fields)
    if actFields[-3:] != '!!!':
        queryStr = f'select {actFields} from eod_ashare ' \
                   f'where DATETIME >= \'{startTime}\' and DATETIME <= \'{endTime}\' ' \
                   f'and code = \'{code}\' order by DATETIME asc'
        df = _db_command_2_df(queryStr)
    else:
        queryStr = f'select {actFields[:-3]} from eod_ashare ' \
                   f'where DATETIME >= \'{startTime}\' and DATETIME <= \'{endTime}\' ' \
                   f'and code = \'{code}\' order by DATETIME asc'
        df = _db_command_2_df(queryStr)
        df['BADJPRECLOSE'] = df['PRECLOSE'] * df['ADJFACTOR'] / df.iat[-1, df.columns.get_loc('ADJFACTOR')]
        df['BADJOPEN'] = df['OPEN'] * df['ADJFACTOR'] / df.iat[-1, df.columns.get_loc('ADJFACTOR')]
        df['BADJHIGH'] = df['HIGH'] * df['ADJFACTOR'] / df.iat[-1, df.columns.get_loc('ADJFACTOR')]
        df['BADJLOW'] = df['LOW'] * df['ADJFACTOR'] / df.iat[-1, df.columns.get_loc('ADJFACTOR')]
        df['BADJCLOSE'] = df['CLOSE'] * df['ADJFACTOR'] / df.iat[-1, df.columns.get_loc('ADJFACTOR')]
        df.drop(['PRECLOSE','OPEN','HIGH','LOW','CLOSE'],axis=1,inplace=True)
        df = df[['BADJPRECLOSE','BADJOPEN','BADJHIGH','BADJLOW','BADJCLOSE','VOLUME','AMOUNT','AVGPRICE',
                 'ADJFACTOR','TRADESTATUS']]
    return df


def stock_index(indexCode, fields='*', startTime=const.DB_START, endTime=const.TODAY):
    '''
    取出某个指数所有股票的行情数据，可以是hs300等大指数，或者是中信行业指数
    :param indexCode:只能是'hs300','sz50','zz500','zz800'其中一个
    :param fields:字符串，例如'*'，如果要具体的字段，必须补上'DATETIME'和'CODE'。
                  除此之外，提供复权方式参数：不复权，前复权，后复权。其中前复权用BADJ为前缀，后复权后ADJ为前缀。
    :param startTime:字符串，格式为：'2005-01-04'
    :param endTime:字符串，格式为：'2005-01-04'
    :return:
    '''
    if indexCode in ['hs300','sz50','zz500','zz800']:
        actFields = _convert_fields(fields)
        indexTableDict = {'hs300':'eod_index_constitution_hs300',
                          'HS300': 'eod_index_constitution_hs300',
                          'sz50':'eod_index_constitution_sz50',
                          'SZ50': 'eod_index_constitution_sz50',
                          'zz500':'eod_index_constitution_zz500',
                          'ZZ500': 'eod_index_constitution_zz500',
                          'zz800':'eod_index_constitution_zz800',
                          'ZZ800': 'eod_index_constitution_zz800'}
        indexTable = indexTableDict[indexCode]
        actFields = 'eod_ashare.' + ', eod_ashare.'.join(actFields.replace(' ','').split(','))
        if actFields[-3:] != '!!!':
            queryStr = f'select {actFields} ' \
                       f'from eod_ashare right join {indexTable} ' \
                       f'on eod_ashare.DATETIME = {indexTable}.DATETIME ' \
                       f'and eod_ashare.CODE = {indexTable}.CODE ' \
                       f'where {indexTable}.DATETIME >= \'{startTime}\' ' \
                       f'and {indexTable}.DATETIME <= \'{endTime}\' ' \
                       f'order by eod_ashare.DATETIME asc , eod_ashare.CODE asc'
            df = _db_command_2_df(queryStr)
        else:
            queryStr = f'select {actFields[:-3]} ' \
                       f'from eod_ashare right join {indexTable} ' \
                       f'on eod_ashare.DATETIME = {indexTable}.DATETIME ' \
                       f'and eod_ashare.CODE = {indexTable}.CODE ' \
                       f'where {indexTable}.DATETIME >= \'{startTime}\' ' \
                       f'and {indexTable}.DATETIME <= \'{endTime}\' ' \
                       f'order by eod_ashare.DATETIME, eod_ashare.CODE'
            df = _db_command_2_df(queryStr)
            df['BADJPRECLOSE'] = df['PRECLOSE'] * df['ADJFACTOR']
            df['BADJOPEN'] = df['OPEN'] * df['ADJFACTOR']
            df['BADJHIGH'] = df['HIGH'] * df['ADJFACTOR']
            df['BADJLOW'] = df['LOW'] * df['ADJFACTOR']
            df['BADJCLOSE'] = df['CLOSE'] * df['ADJFACTOR']
            df.drop(['PRECLOSE', 'OPEN', 'HIGH', 'LOW', 'CLOSE'], axis=1, inplace=True)
            df = df[['BADJPRECLOSE', 'BADJOPEN', 'BADJHIGH', 'BADJLOW', 'BADJCLOSE', 'VOLUME', 'AMOUNT', 'AVGPRICE',
                     'ADJFACTOR', 'TRADESTATUS']]

            df = df.groupby(level=1).apply(_cal_badj)

        return df
    else:
        raise ValueError('hs300, sz50, zz500 and zz800 are the only supported!')


def stock_industry_citic(indexCode, level, fields='*', startTime=const.DB_START, endTime=const.TODAY):
    '''
    取中信某个行业指数中所有股票的行情数据，需要指令行业指数的级别，是一级行业还是二级或三级行业。
    :param indexCode: str，需要以CI开头的指数。
    :param level: 行业指数的级别。
    :param fields: 字符串，例如'*'，如果要具体的字段，必须补上'DATETIME'和'CODE'。
                  除此之外，提供复权方式参数：不复权，前复权，后复权。其中前复权用BADJ为前缀，后复权后ADJ为前缀。
    :param startTime:
    :param endTime:
    :return:
    '''
    actFields = _convert_fields(fields)
    actFields = 'eod_ashare.' + ', eod_ashare.'.join(actFields.replace(' ', '').split(','))
    if actFields[-3:] != '!!!':
        queryStr = f'select {actFields}, ' \
                   f'eod_ashare_industry_type_citics.INDUSTRY_CITIC_CODE_{level} as INDUSTRY_CITIC_CODE_{level}, ' \
                   f'eod_ashare_industry_type_citics.INDUSTRY_CITIC_{level} as INDUSTRY_CITIC_{level} ' \
                   f'from eod_ashare ' \
                   f'inner join eod_ashare_industry_type_citics ' \
                   f'on eod_ashare.DATETIME = eod_ashare_industry_type_citics.DATETIME ' \
                   f'and eod_ashare.CODE = eod_ashare_industry_type_citics.CODE ' \
                   f'where eod_ashare_industry_type_citics.INDUSTRY_CITIC_CODE_{level} = \'{indexCode}\' ' \
                   f'and eod_ashare_industry_type_citics.DATETIME >= \'{startTime}\' ' \
                   f'and eod_ashare_industry_type_citics.DATETIME <= \'{endTime}\' ' \
                   f'order by eod_ashare_industry_type_citics.DATETIME asc'
        df = _db_command_2_df(queryStr)
    else:
        queryStr = f'select {actFields[:-3]}, ' \
                   f'eod_ashare_industry_type_citics.INDUSTRY_CITIC_CODE_{level} as INDUSTRY_CITIC_CODE_{level}, ' \
                   f'eod_ashare_industry_type_citics.INDUSTRY_CITIC_{level} as INDUSTRY_CITIC_{level} ' \
                   f'from eod_ashare ' \
                   f'inner join eod_ashare_industry_type_citics ' \
                   f'on eod_ashare.DATETIME = eod_ashare_industry_type_citics.DATETIME ' \
                   f'and eod_ashare.CODE = eod_ashare_industry_type_citics.CODE ' \
                   f'where eod_ashare_industry_type_citics.INDUSTRY_CITIC_CODE_{level} = \'{indexCode}\' ' \
                   f'and eod_ashare_industry_type_citics.DATETIME >= \'{startTime}\' ' \
                   f'and eod_ashare_industry_type_citics.DATETIME <= \'{endTime}\' ' \
                   f'order by eod_ashare_industry_type_citics.DATETIME asc'
        df = _db_command_2_df(queryStr)
        df['BADJPRECLOSE'] = df['PRECLOSE'] * df['ADJFACTOR']
        df['BADJOPEN'] = df['OPEN'] * df['ADJFACTOR']
        df['BADJHIGH'] = df['HIGH'] * df['ADJFACTOR']
        df['BADJLOW'] = df['LOW'] * df['ADJFACTOR']
        df['BADJCLOSE'] = df['CLOSE'] * df['ADJFACTOR']
        df.drop(['PRECLOSE', 'OPEN', 'HIGH', 'LOW', 'CLOSE'], axis=1, inplace=True)
        df = df[['BADJPRECLOSE', 'BADJOPEN', 'BADJHIGH', 'BADJLOW', 'BADJCLOSE', 'VOLUME', 'AMOUNT', 'AVGPRICE',
                 'ADJFACTOR', 'TRADESTATUS', f'INDUSTRY_CITIC_CODE_{level}',f'INDUSTRY_CITIC_{level}']]

        df = df.groupby(level=1).apply(_cal_badj)

    return df


def stock_list(listOfStock, fields='*', startTime=const.DB_START, endTime=const.TODAY):
    '''
    取出list中所有股票的行情数据
    :param listOfStock:
    :param fields:字符串，例如'*'，如果要具体的字段，必须补上'DATETIME'和'CODE'。
                  除此之外，提供复权方式参数：不复权，前复权，后复权。其中前复权用BADJ为前缀，后复权后ADJ为前缀。
    :param startTime:
    :param endTime:
    :return:
    '''
    actFields = _convert_fields(fields)
    codeConditions = [stock_related.convert_2_normalcode(code) for code in listOfStock]
    codeConditions = 'code = \'' + '\' or code = \''.join(codeConditions) + '\''
    if actFields[-3:] != '!!!':
        queryStr = f'select {actFields} from eod_ashare ' \
                   f'where DATETIME >= \'{startTime}\' and DATETIME <= \'{endTime}\' ' \
                   f'and ({codeConditions}) ' \
                   f'order by DATETIME asc, code asc'
        df = _db_command_2_df(queryStr)
    else:
        queryStr = f'select {actFields[:-3]} from eod_ashare ' \
                   f'where DATETIME >= \'{startTime}\' and DATETIME <= \'{endTime}\' ' \
                   f'and ({codeConditions}) ' \
                   f'order by DATETIME asc, code asc'
        df = _db_command_2_df(queryStr)
        df['BADJPRECLOSE'] = df['PRECLOSE'] * df['ADJFACTOR']
        df['BADJOPEN'] = df['OPEN'] * df['ADJFACTOR']
        df['BADJHIGH'] = df['HIGH'] * df['ADJFACTOR']
        df['BADJLOW'] = df['LOW'] * df['ADJFACTOR']
        df['BADJCLOSE'] = df['CLOSE'] * df['ADJFACTOR']
        df.drop(['PRECLOSE','OPEN','HIGH','LOW','CLOSE'],axis=1,inplace=True)
        df = df[['BADJPRECLOSE','BADJOPEN','BADJHIGH','BADJLOW','BADJCLOSE','VOLUME','AMOUNT','AVGPRICE',
                 'ADJFACTOR','TRADESTATUS']]
        df = df.groupby(level=1).apply(_cal_badj)
    return df


def stock_all(fields='*', startTime=const.DB_START, endTime=const.TODAY):
    '''
    取所有股票行情数据的便利函数
    :param fields: 字符串，例如'*'，如果要具体的字段，必须补上'DATETIME'和'CODE'。
                   除此之外，提供复权方式参数：不复权，前复权，后复权。其中前复权用BADJ为前缀，后复权后ADJ为前缀。
    :param startTime: 字符串，格式为：'2005-01-04'
    :param endTime: 字符串，格式为：'2005-01-04'
    :return: df，index为'DATETIME'和'CODE'，其他列为行情数据
    '''
    actFields = _convert_fields(fields)
    if actFields[-3:] != '!!!':
        queryStr = f'select {actFields} from eod_ashare ' \
                   f'where DATETIME >= \'{startTime}\' and DATETIME <= \'{endTime}\' ' \
                   f'order by DATETIME asc'
        df = _db_command_2_df(queryStr)
    else:
        queryStr = f'select {actFields[:-3]} from eod_ashare ' \
                   f'where DATETIME >= \'{startTime}\' and DATETIME <= \'{endTime}\' ' \
                   f'order by DATETIME asc, code asc'
        df = _db_command_2_df(queryStr)
        df['BADJPRECLOSE'] = df['PRECLOSE'] * df['ADJFACTOR']
        df['BADJOPEN'] = df['OPEN'] * df['ADJFACTOR']
        df['BADJHIGH'] = df['HIGH'] * df['ADJFACTOR']
        df['BADJLOW'] = df['LOW'] * df['ADJFACTOR']
        df['BADJCLOSE'] = df['CLOSE'] * df['ADJFACTOR']
        df.drop(['PRECLOSE','OPEN','HIGH','LOW','CLOSE'],axis=1,inplace=True)
        df = df[['BADJPRECLOSE','BADJOPEN','BADJHIGH','BADJLOW','BADJCLOSE','VOLUME','AMOUNT','AVGPRICE',
                 'ADJFACTOR','TRADESTATUS']]
        df = df.groupby(level=1).apply(_cal_badj)
    return df


# --------------------取指数数据-----------------------------------------------------------------------------------------
def index_one(indexCode, type='normal', startTime=const.DB_START, endTime=const.TODAY):
    '''
    这个是去单个指数的便利函数，可以取普通指数，包括上证指数等。
    或者行业指数，包括中信，wind，主题和概念，
    :param indexCode: 字符串。CI开头是中信，884是概念，882是万得行业，886是主题
    :param type: 字符串。normal是普通指数；citics是中信；wind是万得行业指数；theme是万得主题；concept是万得概念。
    :param startTime:
    :param endTime:
    :return:
    '''
    if (indexCode[:2] == 'CI' or indexCode[:2] == '88') and (indexCode[:6] != '881001'):
        queryStr = f'select * from {const.InduTypeTableDict[type]} ' \
                   f'where code = \'{indexCode}\' ' \
                   f'and datetime >= \'{startTime}\' ' \
                   f'and datetime <= \'{endTime}\' ' \
                   f'order by datetime desc'
        df = _db_command_2_df(queryStr)
    else:
        if indexCode[:2] == 'hs' or indexCode[:2] == 'sz' or indexCode[:2] == 'zz' \
                or indexCode[:2] == 'HS' or indexCode[:2] == 'SZ' or indexCode[:2] == 'ZZ':
            indexCode = stock_related.indexAbrv_2_code(indexCode)
        elif indexCode[-2:] == 'SH' or indexCode[-2:] == 'SZ' or indexCode[-2:] == 'WI':
            pass
        else:
            raise ValueError('The input index code is not supported!')

        queryStr = f'select * from {const.IndexTableDict[indexCode]} ' \
                   f'where DATETIME >= \'{startTime}\' and DATETIME <= \'{endTime}\' ' \
                   f'order by datetime asc'
        df = _db_command_2_df(queryStr)

    if df.empty:
        raise ValueError('代码列表和类型不一致！')
    return df


def index_list(listOfCode, startTime=const.DB_START, endTime=const.TODAY):
    '''
    这是个取普通指数的便利函数。
    :param listOfCode:
    :param startTime:
    :param endTime:
    :return:
    '''
    tableList = [const.IndexTableDict[stock_related.indexAbrv_2_code(indexCode)] for indexCode in listOfCode]
    queryBaseStr = f'select * from ' \
                   + f' where datetime >= \'{startTime}\' and datetime <= \'{endTime}\' union select * from '.join(tableList) \
                   + f' where datetime >= \'{startTime}\' and datetime <= \'{endTime}\' order by datetime asc, code asc'
    df = _db_command_2_df(queryBaseStr)
    return df


def index_industry_list(listOfCode, type='citics', citicslevel=None, startTime=const.DB_START, endTime=const.TODAY):
    '''
    这是个取某几个同类型行业指数的便利函数。
    :param listOfCode: CI开头是中信，884是概念，882是万得行业，886是主题。all是取该类型所有指数。
    :param type: 字符串。normal是普通指数；citics是中信；wind是万得行业指数；theme是万得主题；concept是万得概念。
    :param startTime:
    :param endTime:
    :return:
    '''

    if listOfCode == 'all':
        queryStr = f'select * from {const.InduTypeTableDict[type]} ' \
                   f'where datetime >= \'{startTime}\' and datetime <= \'{endTime}\''
        if citicslevel == None:
            pass
        elif citicslevel != 1 and citicslevel != 2 and citicslevel != 3:
            raise ValueError('citicslevel必须是1，2，3或者None')
        else:
            queryStr = queryStr + f' and level = {citicslevel} order by datetime asc, code asc'
        df = _db_command_2_df(queryStr)

    else:
        codeConditions = 'code = \'' + '\' or code = \''.join(listOfCode) + '\''
        queryStr = f'select * from {const.InduTypeTableDict[type]} ' \
                   f'where DATETIME >= \'{startTime}\' and DATETIME <= \'{endTime}\' ' \
                   f'and ({codeConditions}) ' \
                   f'order by DATETIME asc, code asc'
        df = _db_command_2_df(queryStr)

    if df.empty:
        raise ValueError('代码列表和类型不一致！')
    return df


# --------------------取分钟数据-----------------------------------------------------------------------------------------
def n_min(code, n=1,startTime=const.DB_START, endTime=const.TODAY):
    code = stock_related.convert_2_normalcode(code)
    tableName = code.replace('.','') + f'_{n}min'
    if len(startTime) <= 11:
        queryStr = f'select * from {tableName} ' \
                   f'where datetime >= \'{startTime} 09:00:00\' and datetime <= \'{endTime} 16:00:00\' ' \
                   f'order by datetime asc, code asc'
    else:
        queryStr = f'select * from {tableName} ' \
                   f'where datetime >= \'{startTime}\' and datetime <= \'{endTime}\' ' \
                   f'order by datetime asc, code asc'
    return _db_command_2_df(queryStr,db='stock_min')


# --------------------取tick数据-----------------------------------------------------------------------------------------
def stock_tick_windDB(stockCode, dateStr):

    stockCode = stock_related.convert_2_normalcode(stockCode)[:6]
    if stockCode[0] == '6':
        mktType = 'SH'
    elif stockCode[0] == '3' or stockCode[0] == '0':
        mktType = 'SZ'
    else:
        print('请检查输入的代码！')

    dateStr = dateStr.replace('-','')
    if len(dateStr) != 8:
        print('请检查输入的日期，格式为\'20161124\'或者\'2016-11-24\'形式！')
    # select * from dbo.f_getSHL1Market('20181031', '601857')
    fetchCmd = f'SELECT SecurityID,TradeTime,PreClosePx,OpenPx,HighPx,LowPx,LastPx,' \
               f'BidSize1, BidPx1, BidSize2, BidPx2, BidSize3, BidPx3, BidSize4, BidPx4, BidSize5, BidPx5, ' \
               f'OfferSize1,OfferPx1,OfferSize2,OfferPx2,OfferSize3,OfferPx3,OfferSize4,OfferPx4,OfferSize5,OfferPx5,' \
               f'NumTrades,TotalVolumeTrade,TotalValueTrade ' \
               f'FROM dbo.f_get{mktType}L1Market(\'{dateStr}\',\'{stockCode}\')'

    HIGHmssqlConn = pyodbc.connect('DSN=HIGH; PWD=password', charset='gbk')
    df = sql.read_sql(fetchCmd,HIGHmssqlConn)
    HIGHmssqlConn.close()

    df.rename(columns=dict(zip(df.columns,[colName.upper() for colName in df.columns])), inplace=True)
    df.rename(columns={'SecurityID'.upper():'CODE'},inplace=True)
    df = df[df['TRADETIME'] != 0.0]

    df['DATETIME'] = df['TradeTime'.upper()].\
        apply(lambda x:dt.datetime.strptime(dateStr+':%06d'%x,'%Y%m%d:%H%M%S'))
    df['CODE'] = df['CODE'] + '.' + mktType

    from collections import Counter
    # 有些时间点是重复，去掉那些时间点。
    counterDF = pd.DataFrame(pd.Series(dict(Counter(df['TRADETIME']))))
    counterDF.reset_index(inplace=True)
    counterDF.rename(columns={0: 'counter'}, inplace=True)
    rstDF = pd.merge(counterDF, df, how='outer', left_on='index', right_on='TRADETIME')
    del counterDF

    # 将重复的而且交易量不为0的那些单独拿出来，和其他不重复的组合成新的
    rstDF2 = rstDF[(rstDF['counter'] != 1.0) & (rstDF['NUMTRADES'] != 0.0)]
    rstDF2 = pd.concat([rstDF[(rstDF['counter'] == 1.0)], rstDF2], axis=0)
    rstDF2 = rstDF2[rstDF2['TRADETIME'] <= 150059.0]

    # 开盘前值保留最后一根tick
    mediumDF = rstDF2[rstDF2['TRADETIME'] <= 93000.0]
    mediumDF = mediumDF[mediumDF['TRADETIME'] == mediumDF['TRADETIME'].max()]
    rstDF2 = pd.concat([mediumDF,rstDF2[rstDF2['TRADETIME'] > 93000.0]])

    if rstDF2.empty:
        print(f'{dateStr} {stockCode}没有数据，将输出空DataFrame！')

    rstDF2.drop(labels = ['counter','index','TRADETIME'],axis = 1,inplace=True)
    rstDF2.sort_values(by=['DATETIME'],inplace=True)
    rstDF2 = rstDF2[['DATETIME'] + list(rstDF2.columns[:-1])]
    rstDF2.drop_duplicates(inplace=True)
    rstDF2.set_index(['DATETIME','CODE'],inplace=True)
    rstDF2.sort_index(level=0,inplace=True)
    return rstDF2


# --------------------取期货数据-----------------------------------------------------------------------------------------
def future_stockindex_day_one(futureCode,startTime=const.DB_START, endTime=const.TODAY):
    queryStr = f'select * from eod_stock_index_future ' \
               f'where datetime >= \'{startTime}\' and datetime <= \'{endTime}\' ' \
               f'and code = \'{futureCode}\' ' \
               f'order by datetime asc, code asc'
    return _db_command_2_df(queryStr,db='future_day')


def future_n_min(code, n=1,startTime=const.DB_START, endTime=const.TODAY):
    tableName = code.replace('.','') + f'_{n}min'
    if len(startTime) <= 11:
        queryStr = f'select * from {tableName} ' \
                   f'where datetime >= \'{startTime} 00:00:00\' and datetime <= \'{endTime} 24:00:00\' ' \
                   f'order by datetime asc, code asc'
    else:
        queryStr = f'select * from {tableName} ' \
                   f'where datetime >= \'{startTime}\' and datetime <= \'{endTime}\' ' \
                   f'order by datetime asc, code asc'
    return _db_command_2_df(queryStr,db='future_min')

# --------------------通用读取函数---------------------------------------------------------------------------------------
def general_one(table, code, fields='*', condition='',startTime=const.DB_START, endTime=const.TODAY, db='stock_day'):
    '''
    这是在数据库中通用取函数的便利的函数，取一个股票的情形，
    :param table:
    :param code:
    :param fields: 不需要输入CODE和DATETIME
    :param condition: str，必须是and开头
    :param startTime:
    :param endTime:
    :param db:
    :return:
    '''
    code = stock_related.convert_2_normalcode(code)
    if fields == '*':
        actFields = '*'
    else:
        actFields = 'CODE, DATETIME ' + fields

    queryStr = f'select {actFields} from {table} ' \
               f'where DATETIME >= \'{startTime}\' and DATETIME <= \'{endTime}\' ' \
               f'and code = \'{code}\' {condition} ' \
               f'order by DATETIME asc, CODE asc'
    df = _db_command_2_df(queryStr,db=db)
    return df


def general_all(table, fields='*', condition='', startTime=const.DB_START, endTime=const.TODAY, db='stock_day'):
    '''
    这是去表中所有代码的数据的便利函数
    :param table:
    :param fields: 不需要输入CODE和DATETIME
    :param condition: str，必须是and开头
    :param startTime:
    :param endTime:
    :param db:
    :return:
    '''
    if fields == '*':
        actFields = '*'
    else:
        actFields = 'CODE, DATETIME ' + fields

    queryStr = f'select {actFields} from {table} ' \
               f'where DATETIME >= \'{startTime}\' and DATETIME <= \'{endTime}\' {condition} ' \
               f'order by DATETIME asc, CODE asc'
    df = _db_command_2_df(queryStr, db=db)
    return df


# --------------------例子-----------------------------------------------------------------------------------------------
if __name__ == '__main__':
    # 日期相关的
    tdates = trade_dates()
    lastDate = latest_date('eod_ashare')

    del tdates, lastDate


if __name__ == '__main__':
    # 股票相关的便利函数
    df = stock_one('601318',startTime='2017-01-01')
    df = stock_one('601318', fields='不复权', startTime='2017-01-01')
    df = stock_one('601318', fields='前复权', startTime='2017-01-01')
    df = stock_one('601318', fields='后复权', startTime='2017-01-01')
    df = stock_one('601318', 'DATETIME, CODE, CLOSE', startTime='2017-01-01')

    df = stock_index('hs300', fields='不复权', startTime='2017-01-01')
    df = stock_index('hs300', fields='前复权', startTime='2017-01-01')
    df = stock_index('hs300', fields='后复权', startTime='2017-01-01')
    df = stock_index('hs300', fields='DATETIME, CODE, CLOSE', startTime='2017-01-01')

    df = stock_industry_citic('CI005022.WI', 1, fields='不复权',startTime='2017-01-01')
    df = stock_industry_citic('CI005022.WI', 1, fields='前复权',startTime='2017-01-01')
    df = stock_industry_citic('CI005022.WI', 1, fields='后复权',startTime='2017-01-01')
    df = stock_industry_citic('CI005022.WI', 1, fields='DATETIME, CODE, CLOSE',startTime='2017-01-01')

    df = stock_list(['601318.SH', '601336.SH','002341.SZ'], fields='不复权', startTime='2017-01-01')
    df = stock_list(['601318.SH', '601336.SH','002341.SZ'], fields='前复权', startTime='2017-01-01')
    df = stock_list(['601318.SH', '601336.SH','002341.SZ'], fields='后复权', startTime='2017-01-01')
    df = stock_list(['601318.SH', '601336.SH','002341.SZ'], fields='DATETIME, CODE, CLOSE', startTime='2017-01-01')

    df = stock_all(fields='不复权', startTime='2018-01-01')
    df = stock_all(fields='前复权', startTime='2018-01-01')
    df = stock_all(fields='后复权', startTime='2018-01-01')
    df = stock_all(fields='DATETIME, CODE, CLOSE', startTime='2018-01-01')

    del df


if __name__ == '__main__':
    # 指数相关的便利函数
    df = index_one('sz50', startTime='2017-01-01')
    df = index_one('000001.SH', startTime='2017-01-01')
    df = index_one('CI005022.WI', type='citics', startTime='2017-01-01')
    df = index_one('882118.WI', type='wind', startTime='2017-01-01')
    df = index_one('884176.WI', type='concept', startTime='2017-01-01')
    df = index_one('886035.WI', type='theme', startTime='2017-01-01')

    df = index_list(['000001.SH', '000016.SH'], startTime='2017-01-01') # 只能是这几个的list

    df = index_industry_list('all', type='citics', citicslevel=1, startTime='2017-01-01')  # 取所有中信一级指数
    df = index_industry_list('all', type='citics', startTime='2017-01-01')  # 取所有中信指数
    df = index_industry_list(['CI005022.WI', 'CI005021.WI'], type='citics', startTime='2017-01-01')
    df = index_industry_list(['882118.WI', '882116.WI'], type='wind', startTime='2017-01-01')
    df = index_industry_list(['884176.WI', '882225.WI'], type='concept', startTime='2017-01-01')
    df = index_industry_list(['886035.WI', '886046.WI'], type='theme', startTime='2017-01-01')

    del df


if __name__ == '__main__':
    dfMin = n_min('000001.SH', 5, startTime='2017-01-01')
    dfMin = n_min('000001.SZ', 1, startTime='2017-01-01')

    del dfMin


if __name__ == '__main__':
    df = general_one('ashare_strangetrade', '000001.SZ', startTime='2017-01-01')
    df = general_all('ashare_strangetrade', startTime='2017-01-01')

    del df

if __name__ == '__main__':
    df = future_stockindex_day_one('IF00.CFE',startTime='2018-01-01')
    dfmin = future_n_min('IF00.CFE',endTime='2016-01-15')