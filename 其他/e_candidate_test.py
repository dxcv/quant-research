if __name__ == '__main__1':
    def cal_max_drawdown(net_value):
        max_here = net_value.expanding(min_periods=1).max()
        drawdown_here = net_value / max_here - 1

        tmp = drawdown_here.sort_values().head(1)
        max_dd = float(tmp.values)

        return max_dd


    import pandas as pd

    ser = pd.read_excel('C:\\srcdata\\equity.xlsx', index_col=[0, 1])['equity']
    dd = cal_max_drawdown(ser)

    # 陈光达
    import pandas as pd

    ser = pd.read_excel('D:\\srcdata\\equity.xlsx', index_col=[0, 1])['equity']
    ser.diff()

    pd.expanding_max(ser)
    max(-ser / pd.expanding_max(ser) + 1)

    ser.iloc[1:2]

# 这个是拉平list的那道题
if __name__ == '__main__':

    # def flattern(sample):
    #     if isinstance(sample, list):
    #         result = []
    #         for item in sample:
    #             result += flattern(item)
    #     else:
    #         return [sample]
    #
    #     return result

    inputList = [1, [2, [3, [8888, 666], [4, 6, [45, 78, [99]], [45, 67, 89, [999, [4543, 90]]]]], 45]]


    # 扶禄城
    def flatten_list(x):
        y = []
        for i in range(len(x)):
            if not isinstance(x[i], list):
                y.append(x[i])
            else:
                flatten_list(x[i])
        return y


    flatten_list(inputList)


    # 赖秋睿
    def reset_list(l):
        i = 0
        res = []

        while i <= len(l) - 1:
            if not isinstance(l[i], list):
                res.append(l[i])
            else:
                reset_list(l[i])
            i += 1

        return res


    reset_list(inputList)


    #
    def main(input_list):
        output = []
        for i in range(len(input_list)):
            if isinstance(input_list[i], list):
                new = main(input_list[i])
                output.extend(new)
            else:
                output.append(input_list[i])
        return output


    main(inputList)


    # 姚子
    def list_split(mylist):
        def once_split(l):
            a = []
            for s in l:
                if isinstance(s, list):
                    for i in range(len(s)):
                        a.append(s[i])
                else:
                    a.append(s)
            return a

        def test(l):
            b = [None, ] * (len(l) + 1)
            for i, subList in enumerate(l):
                b[i] = type(subList)
            if list in b:
                return False
            else:
                return True

        while not test(mylist):
            mylist = once_split(mylist)

        return mylist


    a = list_split(inputList)

    l = [len(inputList[i]) for i in range(len(inputList))]
    type(inputList)


    # 胡泊
    def destruct(input):
        output = []
        for x in input:
            if not isinstance(x, list):
                output = output + [x]
            else:
                y = destruct(x)
                output = output + y
        return output


    destruct(inputList)

    new = []


    def change_into_one_dim(lis, new):
        for c in lis:
            if type(c) is list:
                change_into_one_dim(c, new)
            else:
                new.append(c)
        return new


    change_into_one_dim(inputList, new)

# 第四题
if __name__ == '__main__':
    x = []


    def change(a):
        x.append(1)
        return x


    change(x)