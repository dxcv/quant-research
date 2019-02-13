'''
这个模块是一些杂七杂八的小函数
'''

def convert_2_normalcode(shortCode):
    '''
    将6位的股票代码转变成带后缀的股票代码。如果输入的是带后缀就不变。
    :param shortCode: 字符串，'600036' --> '600036.SH'
    :return: 字符串，'600036.SH'
    '''
    if len(shortCode) == 9 \
            and (shortCode[-3:] == '.SZ' or shortCode[-3:] == '.SH'):
        return shortCode
    elif len(shortCode) == 6:
        if shortCode[0] == '6':
            return shortCode + '.SH'
        elif shortCode[0] == '3' or shortCode[0] == '0':
            return shortCode + '.SZ'
        else:
            raise ValueError('wrong stock code!')
    else:
        raise ValueError('wrong stock code!')


def indexAbrv_2_code(indexAbrv):
    '''
    将hs300等变成000300.SH。如果不是这几个之一，那么返回输入的字符串。
    :param indexAbrv: str，'hs300'等
    :return: '000300.SH'，如果不是这几个之一，那么返回输入的字符串。
    '''
    indexAbrvDict = {'hs300': '000300.SH',
                     'HS300': '000300.SH',
                     'sz50': '000016.SH',
                     'SZ50': '000016.SH',
                     'zz500': '000905.SH',
                     'ZZ500': '000905.SH',
                     'ZZ800': '000906.SH',
                     'zz800': '000906.SH'}
    if indexAbrv in indexAbrvDict:
        return indexAbrvDict[indexAbrv]
    else:
        return indexAbrv

