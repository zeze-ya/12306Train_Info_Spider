import pymongo

class MongoDBHandle(object):
    """定义MongoDB操作类"""

    def __init__(self, url, port, database):
        """初始化数据库信息并创建数据库操作对象"""
        self.url = url
        self.port = port
        self.client = pymongo.MongoClient(self.url, self.port)
        self.db = self.client[database]

    def insert(self, info, table_name):
        """将数据插入数据库"""
        if self.db[table_name].insert_one(info):
            print("存入数据库成功")
        else:
            print('存入数据库失败', info)
