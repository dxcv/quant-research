
import datetime as dt

def to_str(input, tyPe, foRmAt):
    '''
    这个函数的作用是将各种格式变成字符串
    :param input:str，输入的字符串。
    :param tyPe: 输入的类型，一定要和inPut匹配
    :param foRmAt: 需要转换成的格式。%是转换才百分数，其他就按照python的字符串格式化来进行处理。
    :return:
    '''
    if isinstance(input, tyPe) and not isinstance(input, dt.datetime):
        if foRmAt == '%':
            return '{:.2f}%'.format(input * 100)
        else:
            _format = '{:' + foRmAt + '}'
            return _format.format(input)
    elif isinstance(input, tyPe) and isinstance(input, dt.datetime):
        return input.strftime(foRmAt)
    else:
        return input