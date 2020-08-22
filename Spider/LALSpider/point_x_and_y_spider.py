import time

import requests
import urllib.parse

from DataBaseHandler.config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT
from DataBaseHandler.mysql_handle import MySQL
from Spider.config import POINTX_Y_URL, POINTX_Y_TABLE, STATION_DICT
import re

class PointXY(object):
    """
        经纬度爬虫
    """
    def __init__(self):
        """
        初始化信息
        """
        self.my_sql = MySQL(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT)

    def generate_point_url(self, location_name):
        """
        生成地点经纬度url
        :param location_name:地点名
        :return:腾讯经纬度api地址
        """
        location_name_url = urllib.parse.quote(location_name)
        url = POINTX_Y_URL.format(location_name_url)
        return url

    def get_url_response(self, url):
        """
        获取url响应值
        :param url: 腾讯经纬度api地址
        :return:
        """
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.text
        except:
            print('抓取出错')

    def parse_response(self, html, name):
        """
        解析响应
        :param html:返回的响应值
        :param name:地点名
        :return:
        """
        pointx_pat = re.compile('pointx\"\:\"(.*?)\"')
        pointy_pat = re.compile('pointy\"\:\"(.*?)\"')

        pointx = re.findall(pointx_pat, html)
        pointy = re.findall(pointy_pat, html)

        if pointx and pointy:
            result = {
                'name': name,
                'long': pointx[0],
                'wide': pointy[0]
            }
            return result



    def save_to_mysql(self, data):
        """
        保存到数据库中
        :param data:
        :return:
        """
        self.my_sql.de_duplication(data, POINTX_Y_TABLE)

    def run(self):
        """
        程序入口
        :return:
        """
        for location in STATION_DICT.keys():
            time.sleep(1)
            url = self.generate_point_url(location)
            response = self.get_url_response(url)
            result = self.parse_response(response, location)
            print(result)


if __name__ == '__main__':
    point_xy = PointXY()
    point_xy.run()
