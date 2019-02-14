
import itertools
import pandas as pd
import re


def find_synchron_trade(srcFilepath, rstFilepath):
    df = pd.read_excel(srcFilepath, encoding='gbk', parse_dates=[1, ])

    df.reset_index(drop=True)

    # df.columns
    df['各基金占比'] = df['各基金占比'].apply(lambda x: re.sub(r'\(.+?\)', '', x))
    df['各基金占比'] = df['各基金占比'].apply(lambda x: set(x.split(';')[:-1]))

    # 得到全集
    wholeSet = set()
    for index in df.index:
        for elem in df.at[index, '各基金占比']:
            if elem not in wholeSet:
                wholeSet.add(elem)

    # 统计两个的出现频率
    rst2 = {}
    comb2 = itertools.combinations(wholeSet, 2)

    for comb in comb2:
        _ = len(df[df['各基金占比'].apply(lambda x: True if set(comb) == (set(comb) & x) else False)])
        if _ != 0:
            rst2[comb] = _
    rstdf2 = pd.Series(rst2)
    rstdf2.sort_values(ascending=False, inplace=True)

    # 统计三个出现的频率
    rst3 = {}
    comb3 = itertools.combinations(wholeSet, 3)

    for comb in comb3:
        _ = len(df[df['各基金占比'].apply(lambda x: True if set(comb) == (set(comb) & x) else False)])
        if _ != 0:
            rst3[comb] = _
    rstdf3 = pd.Series(rst3)
    rstdf3.sort_values(ascending=False, inplace=True)

    # 统计四个出现的频率
    df4 = df[df['各基金占比'].apply(len) >= 4]

    rst4 = {}
    comb4 = df4[df4['各基金占比'].apply(len) == 4]['各基金占比'].apply(tuple).unique()
    for comb in comb4:
        _ = len(df4[df4['各基金占比'].apply(lambda x: True if set(comb) == (set(comb) & x) else False)])
        if _ != 0:
            rst4[comb] = _
    rstdf4 = pd.Series(rst4)
    rstdf4.sort_values(ascending=False, inplace=True)

    # 统计五个出现的频率
    df5 = df[df['各基金占比'].apply(len) >= 5]
    rst5 = {}
    comb5 = df5[df5['各基金占比'].apply(len) == 5]['各基金占比'].apply(tuple).unique()
    for comb in comb5:
        _ = len(df5[df5['各基金占比'].apply(lambda x: True if set(comb) == (set(comb) & x) else False)])
        if _ != 0:
            rst5[comb] = _
    rstdf5 = pd.Series(rst5)
    rstdf5.sort_values(ascending=False, inplace=True)

    # 统计六个出现的频率
    df6 = df[df['各基金占比'].apply(len) >= 6]
    rst6 = {}
    comb6 = df6[df6['各基金占比'].apply(len) == 6]['各基金占比'].apply(tuple).unique()
    for comb in comb6:
        _ = len(df6[df6['各基金占比'].apply(lambda x: True if set(comb) == (set(comb) & x) else False)])
        if _ != 0:
            rst6[comb] = _
    rstdf6 = pd.Series(rst6)
    rstdf6.sort_values(ascending=False, inplace=True)

    writer = pd.ExcelWriter(rstFilepath)
    rstdf2.to_excel(writer, startcol=0)
    rstdf3.to_excel(writer, startcol=4)
    rstdf4.to_excel(writer, startcol=9)
    rstdf5.to_excel(writer, startcol=15)
    rstdf6.to_excel(writer, startcol=22)
    writer.save()


if __name__ == '__main__':

    # 需要将文件放到 D:\srcdata\大额成交统计 里面，需要输入文件名
    # 文件需要将下面的次数统计删掉
    srcFilepath = 'D:\\srcdata\\大额成交统计\\201901.xlsx'
    rstFilepath = 'D:\\rstdata\\30%大额成交组合一致性分析\\一致性分析201901.xlsx'

    find_synchron_trade(srcFilepath, rstFilepath)