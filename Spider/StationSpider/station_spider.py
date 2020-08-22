import requests
import re
from requests import RequestException

from DataBaseHandler.config import MONGO_URL, MONGO_PORT, MONGO_DB, MONGO_TABLE_STATION
from DataBaseHandler.mongodb_handle import MongoDBHandle
from Spider.config import *

class StationSpider(object):
    """
        爬取全国火车站
    """

    def __init__(self):
        """
            初始化信息
        """
        self.db_handle = MongoDBHandle(MONGO_URL, MONGO_PORT, MONGO_DB)

    # 获取网页原始信息
    def get_stations_info(self):
        try:
            stations_info = requests.get(STATION_URL)
            # print(stations_info.text)
            stations_info.raise_for_status()
            return stations_info.text
        except RequestException as e:
            print('爬取车站信息失败')

    # 获取每个车站的信息
    def get_station_name(self, stations_info):
        try:
            station_name_patten = re.compile('([\u4e00-\u9fa5]+\|[A-Z]+\|[a-z]+\|[a-z]+)')
            stations_name = re.findall(station_name_patten, stations_info)
            print(stations_name)
            for items in stations_name:
                item = items.split('|', 3)
                yield {
                    'c_name': item[0],
                    'code': item[1],
                    # 'e_name': item[2],
                    # 's_e_name': item[3]
                }
            # _dict = {}
            # for items in stations_name:
            #     item = items.split('|', 3)
            #     _dict[item[0]] = item[1]
            # print(_dict)
        except requests.HTTPError:
            print('提取车站信息失败')

    # 存入数据库
    def save_to_mongo(self, station_name):
        self.db_handle.insert(station_name, MONGO_TABLE_STATION)
