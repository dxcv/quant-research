
import pymongo
# 建立数据库连接，指定ip和端口号，没有账号密码
client = pymongo.MongoClient("localhost", 27017)
# 指定mydb数据库，如果没有的就生成一个叫mydb的数据库
mydb = client.runoob
# 指定mydb数据库里user集合
collection = mydb.test
# 插入数据,以下为两个文档，相当与关系型数据库里的两行（条）数据
data1 = {"age":24, "userName":"zuofanixu"}
data2 = {"age":26, "userName":"yanghang"}
collection.insert_one(data1)
collection.insert_one(data2)
# 查询内容
result = collection.find()
print(result)