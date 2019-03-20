
from tools.data import update
from tools.data import fetch
import const

endTime = const.TODAY

# ----------------更新eod数据（每日都有的数据）----------------------------------------------------------------------------
update.trade_dates(endTime=endTime)
update.eod_ashare(endTime=endTime)
update.eod_indexes(endTime=endTime)  # 881001只到2017-11-24
update.eod_induIndex_citics(endTime=endTime)
update.eod_index_wind(endTime=endTime)
update.eod_citics_industry_type(endTime=endTime)  # @ todo 出问题了
# update.eod_index_constitution(endTime=endTime) # @todo 这个是到20171030就没有数据了

update.future_stock_index_day(endTime=endTime)
update.SPIF_top20_disclose(endTime=endTime)

# ----------------更新事件类数据-----------------------------------------------------------------------------------------
update.strange_trade(endTime=endTime)
update.block_trade(endTime=endTime)
update.major_holder_trade(endTime=endTime)
update.money_flow(endTime=endTime)
update.profit_express(endTime=endTime)
update.earning_estimate(endTime=endTime)
# update.earningest_consensusdata(endTime=endTime)


# ----------------更新指数1分钟数据 -------------------------------------------------------------------------------------
codes = ['000001.SH', '000300.SH', '000905.SH', '000906.SH', '000016.SH',
         '399001.SZ', '399005.SZ', '399006.SZ', ]
for code in codes:
    update.index_1min(code,endTime=endTime)

for code in codes:
    update.n_mins_fromDB(code, 5, endTime=endTime)
    update.n_mins_fromDB(code, 30,endTime=endTime)
    update.n_mins_fromDB(code, 120, endTime=endTime)


# ----------------更新期货行情1分钟数据-----------------------------------------------------------------------------------
codes = ['IF00.CFE', 'IC00.CFE', 'IH00.CFE', ]
for code in codes:
    update.future_stock_index_min(code,endTime=endTime)


# ----------------更新个股1分钟数据 -------------------------------------------------------------------------------------

from tools.data import update
from tools.data import fetch
import const
stockCodes = fetch.stock_all(fields='DATETIME, CODE, CLOSE', startTime='2018-10-18', endTime='2018-10-18')
stockCodes = stockCodes.reset_index()['CODE'].tolist()

for code in stockCodes:
    try:
        update.stock_1min_API(code,endTime='2018-10-18')
    except:
        update.stock_1min_API(code, endTime='2018-10-18')
        import pymysql
        import pandas as pd
        connection = pymysql.connect(host=const.HOST,
                                     user=const.USER,
                                     password=const.PASSWORD,
                                     db='stock_min',
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            # cursor = connection.cursor()
            newcode = code.lower().replace('.', '')

            sqlStr1 = f'show columns from {newcode}_1min'
            cursor.execute(sqlStr1)
            data = cursor.fetchall()
            df = pd.DataFrame(data)
            df.set_index('Field', inplace=True)

            # 改code的类型
            if df.at['CODE', 'Type'] == 'char(10)' and df.at['DATETIME', 'Null'] == 'YES':
                sqlStr2 = f'ALTER TABLE `stock_min`.`{newcode}_1min` CHANGE COLUMN `DATETIME` `DATETIME` DATETIME NOT NULL;'
                cursor.execute(sqlStr2)
            elif df.at['CODE', 'Type'] != 'char(10)' and df.at['DATETIME', 'Null'] == 'YES':
                sqlStr2 = f'ALTER TABLE `stock_min`.`{newcode}_1min` CHANGE COLUMN `CODE` `CODE` CHAR(10) NOT NULL;'
                cursor.execute(sqlStr2)
                sqlStr2 = f'ALTER TABLE `stock_min`.`{newcode}_1min` CHANGE COLUMN `DATETIME` `DATETIME` DATETIME NOT NULL;'
                cursor.execute(sqlStr2)
            else:
                print(f'{code}不需要改CODE')

            # 改开高低收的数据类型
            for col in ['OPEN', 'HIGH', 'LOW', 'CLOSE']:
                if df.at[col, 'Type'] == 'double':
                    sqlStr2 = f'ALTER TABLE `stock_min`.`{newcode}_1min` CHANGE COLUMN `{col}` `{col}` FLOAT NULL DEFAULT NULL;'
                    cursor.execute(sqlStr2)

            # 改index
            sqlStr1 = f'show index from {newcode}_1min'
            cursor.execute(sqlStr1)
            data = cursor.fetchall()
            if data == ():
                sqlStrIndex = f'ALTER TABLE `stock_min`.`{newcode}_1min` ' \
                              f'ADD INDEX `ix_{newcode}_1min_DATETIME_CODE` (`DATETIME` ASC, `CODE` ASC);'
                cursor.execute(sqlStrIndex)
            else:
                df = pd.DataFrame(data)
                sqlStrIndex = f'ALTER TABLE `stock_min`.`{newcode}_1min` ' \
                              f'DROP INDEX `{df.at[0,"Key_name"]}`, ' \
                              f'ADD INDEX `ix_{newcode}_1min_DATETIME_CODE` (`DATETIME` ASC, `CODE` ASC);'
                cursor.execute(sqlStrIndex)

        connection.close()


# ----------------下面只需不定期更新即可 ---------------------------------------------------------------------------------
update.ashare_info()

