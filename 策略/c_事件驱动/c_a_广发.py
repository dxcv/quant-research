
from tools.data import fetch

eventNameDict = {'龙虎榜': 'ashare_strangetrade',
                 '大宗交易': 'ashare_blocktrade',
                 '主要股东增减持': 'ashare_mjrholdertrade',
                 '资金流向': 'ashare_moneyflow',
                 '业绩快报': 'ashare_profitexpress',
                 '业绩预告': 'ashare_profitnotice',
                 '盈利预测明细': 'ashare_earningest',
                 '盈利预测汇总': 'ashare_earningest_consensusdata',
                 '投资评级明细': 'ashare_stockrating',
                 '投资评级汇总': 'ashare_stockratingconsus'}

############################################### 1、高管增持事件 ##########################################################
# @todo 这个搜集的数据的winddb里面的，源头是增持减持完之后的公告，高管增持之前还会有公告的。

if __name__ == '__main__':
    dfzengchiTotal = fetch.general_all(eventNameDict['主要股东增减持'],condition='and transact_type = \'增持\'')
    dfzengchiTotal.to_csv('D:\\tempdata\\dfzengchiTotal.csv',encoding='gbk')
    dfgaoguan = fetch.general_all(eventNameDict['主要股东增减持'],
                                  condition='and holder_type = \'高管\' and transact_type = \'增持\'')

if __name__ == '__main__':
    # 每个类挑出前三的股票，公布交易量前五的营业部名称
    # 上海
    # 日收盘价格涨幅偏离值达到7%
    # 日收盘价格跌幅偏离值达到7%
    # 日价格振幅达到15%
    # 日换手率达到20%
    # 非ST、*ST和S证券连续三个交易日内收盘价格涨幅偏离值累计达到20%的证券:
    # 非ST、*ST和S证券连续三个交易日内收盘价格跌幅偏离值累计达到20%的证券:
    # ST、*ST和S证券连续三个交易日内收盘价格涨幅偏离值累计达到15%的证券:
    # ST、*ST和S证券连续三个交易日内收盘价格跌幅偏离值累计达到15 % 的证券:
    # 深圳：
    # 日价格涨幅偏离值超过7%
    # 日价格涨幅偏离值低于-7%
    # 日换手率达到20%
    # 异常期间价格跌幅偏离值累计达到-15%
    # 异常期间价格跌幅偏离值累计达到15%
    dflonghubang = fetch.general_all(eventNameDict['龙虎榜'],startTime='2018-01-01')


if __name__ == '__main__':
    dfblockTrade = fetch.general_all(eventNameDict['大宗交易'],startTime='2018-01-01')
    dfStock = fetch.stock_one('000001',startTime='2018-06-30')
    dfStock.to_csv('D:\\tempdata\\000001.csv',encoding='gbk')
