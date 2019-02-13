
if __name__ == '__main__':
    import itertools
    import pandas as pd

    df = pd.read_excel('D:\\srcdata\\大额成交统计\\30%大额成交组合.xlsx', encoding='gbk', parse_dates=[1,])
    df.reset_index(drop=True)
    df.drop(['Unnamed: 8', 'Unnamed: 9', 'Unnamed: 10', 'Unnamed: 11'], axis=1, inplace=True)

    # df.columns

    df['各基金占比'] = df['各基金占比'].apply(lambda x: set(x.split(';')[:-1]))

    # 得到全集
    wholeSet = set()
    for index in df.index:
        for elem in df.at[index, '各基金占比']:
            if elem not in wholeSet:
                wholeSet.add(elem)

    # len(wholeSet)

    # 统计两个的出现频率
    rst2 = {}
    comb2 = itertools.combinations(wholeSet, 2)

    for comb in comb2:
        _ = len(df[df['各基金占比'].apply(lambda x: True if set(comb) == (set(comb) & x) else False)])
        if  _ != 0:
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

    # rstdf2.to_csv('D:\\tempdata\\rstdf2.csv', encoding='gbk')
    # rstdf3.to_csv('D:\\tempdata\\rstdf3.csv', encoding='gbk')

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
    rstdf4.to_csv('D:\\tempdata\\rstdf4.csv', encoding='gbk')

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
    rstdf5.to_csv('D:\\tempdata\\rstdf5.csv', encoding='gbk')

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
    rstdf6.to_csv('D:\\tempdata\\rstdf6.csv', encoding='gbk')


# 2018Q4
if __name__ == '__main__':
    import itertools
    import pandas as pd
    import re

    df = pd.read_excel('D:\\srcdata\\大额成交统计\\30%大额成交组合_2018q4.xlsx', encoding='gbk', parse_dates=[1,])
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

    # len(wholeSet)

    # 统计两个的出现频率
    rst2 = {}
    comb2 = itertools.combinations(wholeSet, 2)

    for comb in comb2:
        _ = len(df[df['各基金占比'].apply(lambda x: True if set(comb) == (set(comb) & x) else False)])
        if  _ != 0:
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

    # rstdf2.to_csv('D:\\tempdata\\rstdf2.csv', encoding='gbk')
    # rstdf3.to_csv('D:\\tempdata\\rstdf3.csv', encoding='gbk')

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
    # rstdf4.to_csv('D:\\tempdata\\rstdf4.csv', encoding='gbk')

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

    writer = pd.ExcelWriter('D:\\rstdata\\30%大额成交组合一致性分析_2018q4.xlsx')
    rstdf2.to_excel(writer, startcol=0)
    rstdf3.to_excel(writer, startcol=4)
    rstdf4.to_excel(writer, startcol=9)
    rstdf5.to_excel(writer, startcol=15)
    rstdf6.to_excel(writer, startcol=22)
    writer.save()


# 2018
if __name__ == '__main__':
    import itertools
    import pandas as pd
    import re

    df = pd.read_excel('D:\\srcdata\\大额成交统计\\30%大额成交组合2018.xlsx', encoding='gbk', parse_dates=[1,])
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

    # len(wholeSet)

    # 统计两个的出现频率
    rst2 = {}
    comb2 = itertools.combinations(wholeSet, 2)

    for comb in comb2:
        _ = len(df[df['各基金占比'].apply(lambda x: True if set(comb) == (set(comb) & x) else False)])
        if  _ != 0:
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

    writer = pd.ExcelWriter('D:\\rstdata\\30%大额成交组合一致性分析_2018.xlsx')
    rstdf2.to_excel(writer, startcol=0)
    rstdf3.to_excel(writer, startcol=4)
    rstdf4.to_excel(writer, startcol=9)
    rstdf5.to_excel(writer, startcol=15)
    rstdf6.to_excel(writer, startcol=22)
    writer.save()
