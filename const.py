import datetime as dt
import pandas as pd

HOST = 'localhost'
USER = 'root'
PASSWORD = '123321'
DB_START = '1990-01-01'
TODAY = dt.datetime.today().strftime('%Y-%m-%d')
YESTODAY = (dt.datetime.today() - dt.timedelta(1)).strftime('%Y-%m-%d')

# 普通指数和表明之间的对应
IndexTableDict = {'000300.SH':'eod_index_000300SH',
                 '000016.SH':'eod_index_000016SH',
                 '000905.SH':'eod_index_000905SH',
                 '000001.SH':'eod_index_000001SH',
                 '399001.SZ':'eod_index_399001SZ',
                 '399005.SZ':'eod_index_399005SZ',
                 '399006.SZ':'eod_index_399006SZ',
                 '000906.SH':'eod_index_000906SH',
                 '881001.WI':'eod_index_881001wi'}
# 类型和行业指数之间的对应
InduTypeTableDict = {'citics': 'eod_induindex_citics',
                     'wind': 'eod_induindex_wind',
                     'theme': 'eod_themeindex_wind',
                     'concept': 'eod_conceptindex_wind'}

# 画图时候的颜色和标记，不超过7个。
COLORS = ['white','yellow','cyan','lightgreen','orange','royalblue','plum']
MARKERS = ['o','d','*','h','+','D','x']

# 中信行业指数
CITICSCODE = pd.read_csv(r'D:\py36 projects\quant-research\tools\data\csvs\citicsCodes.csv',encoding='gbk')