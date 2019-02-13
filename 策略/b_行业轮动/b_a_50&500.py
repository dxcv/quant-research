
if __name__ == '__main__':
    import pandas as pd
    from tools.data import fetch
    from tools.backtest import timing, signal

    dfzz500 = fetch.index_one('000905.SH',startTime='2007-01-15')
    dfsz50 = fetch.index_one('000016.SH',startTime='2007-01-15')

    df = pd.DataFrame()
    df['target'] = dfsz50.reset_index(level=1)['PCTCHANGE'] - dfzz500.reset_index(level=1)['PCTCHANGE']
    df['sz50'] = dfsz50.reset_index(level=1)['CLOSE']
    df['zz500'] = dfzz500.reset_index(level=1)['CLOSE']
    df['return_sz50'] = dfsz50.reset_index(level=1)['PCTCHANGE'] / 100
    df['return_zz500'] = dfzz500.reset_index(level=1)['PCTCHANGE'] / 100


    signal.up_cross(df, 'target')
    signal.signal2position_multi(df, signalCol='signal',
                                 mode='longshort', )

    timing.multi_proportion_backtest(df,['sz50','zz500'],[1,-1],[0.5,0.5],tCost=0.0002,)
    detailDf = timing.multi_trade_details(df,originSignal='signal',pxColList=['sz50','zz500'],longshortList=[1,-1],proportionList=[0.5,0.5])
    evalSer = timing.overall_eval(df,detailDf)
    yearDf = timing.year_month_analysis(detailDf)


