

def cal_pct_change(group1):
    return group1.iat[-1, group1.columns.get_loc('CLOSE')] / group1.iat[0, group1.columns.get_loc('PRECLOSE')] - 1


def cal_last(group1, col='CLOSE'):
    try:
        return group1.iat[-1, group1.columns.get_loc(col)]
    except:
        return group1.reset_index().iat[-1, group1.columns.get_loc(col)]