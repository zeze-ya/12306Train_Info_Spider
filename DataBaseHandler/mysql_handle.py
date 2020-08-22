from queue import Queue

import pymysql
from loguru import logger

class MySQL(object):
    """定义MySQL操作类
        使用单例模式
        构建链接池
    """
    instance = None

    def __new__(cls, *args, **kwargs):
        """
        对new方法进行重写，实现单例模式
        :param args:
        :param kwargs:
        """
        if cls.instance is None:
            cls.instance = super().__new__(cls)

        return cls.instance

    def __init__(self, host, username, password, database, port, maxconn=5):
        """初始化数据库信息并创建数据库链接池"""
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.maxconn = maxconn
        self.pool = Queue(maxconn)

        try:
            for x in range(maxconn):
                conn = pymysql.connect(self.host, self.username, self.password, self.database, self.port, charset='utf8')
                self.pool.put(conn)
        except Exception as e:
            raise IOError(e)

    @logger.catch
    def de_duplication(self, data, table_name):
        """去重操作，数据存在则更新，不存在则插入，要求传入的类型为字典"""
        conn = self.pool.get()
        cursor = conn.cursor()

        keys = ','.join(data.keys())
        values = ','.join(['%s'] * len(data))

        sql = 'INSERT INTO {table}({keys}) VALUES ({values}) ON DUPLICATE KEY UPDATE '.format(
            table=table_name, keys=keys, values=values
        )
        update = ','.join([" {key} =%s".format(key=key) for key in data])
        sql += update

        try:
            if cursor.execute(sql, tuple(data.values())*2):
                conn.commit()
                logger.info("数据存入 {} 成功", table_name)
        except pymysql.Error as e:
            logger.error('存入数据库 {} 失败，数据： {} ', table_name, data)
            conn.rollback()
        finally:
            cursor.close()
            self.pool.put(conn)

    def query_for_stats(self):
        """
        为统计数据查询
        :return:
        """
        sql = 'SELECT start_station_name FROM train'
        conn = self.pool.get()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            result_list = []
            for (result,) in results:
                result_list.append(result)

            return result_list
        except pymysql.Error:
            logger.error('查询数据出错')
        finally:
            cursor.close()
            self.pool.put(conn)