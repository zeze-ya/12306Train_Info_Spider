import pandas as pd
import matplotlib.pyplot as plt
from DataBaseHandler.config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT
from DataBaseHandler.mysql_handle import MySQL
plt.rcParams['font.sans-serif'] = ['SimHei']
# 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False
# 用来正常显示负号

def stats():
    """

    :return:
    """
    mysql = MySQL(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT)
    result = mysql.query_for_stats()
    print(result)
    data = pd.Series(result)
    print('........\n')
    value_counts = data.value_counts()
    # for i in range(10):
    #     value_counts = value_counts[int(i) != value_counts.values]
    print(data.value_counts()[0:10])
    data.value_counts()[0:10].plot.bar()
    plt.title("车站出现平次统计直方图")
    plt.xlabel("车站名")
    plt.ylabel("频次")
    plt.show()


if __name__ == '__main__':
    stats()