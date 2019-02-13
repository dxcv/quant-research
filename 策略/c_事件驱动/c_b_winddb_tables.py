
from tools.data import update

querySql = 'select * from AShareSEO order by OPDATE desc, s_info_windcode asc'
dingzeng = update._winddb_command_df(querySql)
dingzeng.drop('OBJECT_ID',axis=1,inplace=True)
dingzeng.to_csv('D:\\tempdata\\dingzeng.csv',encoding='gbk')

zhihuanSql = 'select * from AShareStockSwap order by OPDATE desc, transferer_windcode asc'
zhihuan = update._winddb_command_df(zhihuanSql)
zhihuan.drop('OBJECT_ID',axis=1,inplace=True)
zhihuan.to_csv('D:\\tempdata\\zhihuan.csv',encoding='gbk')

chongzuSql = 'select * from AShareRestructuringEvents order by OPDATE desc, s_info_windcode asc'
chongzu = update._winddb_command_df(chongzuSql)
chongzu.drop('OBJECT_ID',axis=1,inplace=True)
chongzu.to_csv('D:\\tempdata\\chongzu.csv',encoding='gbk')

binggouSql = 'select * from MergerEvent order by OPDATE desc'
binggou = update._winddb_command_df(binggouSql)
binggou.drop('OBJECT_ID',axis=1,inplace=True)
binggou.to_csv('D:\\tempdata\\binggou.csv',encoding='utf-8')

mergeIntelSql = 'select * from MergerIntelligence order by OPDATE desc'
mergeIntel = update._winddb_command_df(mergeIntelSql)
mergeIntel.drop('OBJECT_ID',axis=1,inplace=True)
mergeIntel.to_csv('D:\\tempdata\\mergeIntel.csv',encoding='utf-8')

