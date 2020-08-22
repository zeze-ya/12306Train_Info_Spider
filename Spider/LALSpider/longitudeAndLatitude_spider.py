import requests
from bs4 import BeautifulSoup

from DataBaseHandler.config import MONGO_URL, MONGO_PORT, MONGO_DB
from Spider.config import *
from DataBaseHandler import mongodb_handle


class LALSpider(object):

    def __init__(self):
        self.db_handle = mongodb_handle.MongoDBHandle(MONGO_URL, MONGO_PORT, MONGO_DB)

    def get_longAndLat_url_response(self):
        """获取经纬索引页的响应"""
        try:
            headers = {
                'authority': 'www.d1xz.net',
                'cache-control': 'no-cach',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
                'sec-fetch-dest': 'document',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'sec-fetch-site': 'cross-site',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-user': '?1',
                'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'cookie': 'Hm_lvt_d96f3af1d84a28ce7598db6f236958bc=1586310268,1586329924; SERVERID=a13f2a4293a447418bafd52b7e1102e6^|1586332057^|1586329923; Hm_lpvt_d96f3af1d84a28ce7598db6f236958bc=1586332058',
                # 'if-modified-since': 'Wed, 08 Apr 2020 07:45:40 GMT',
            }
            response = requests.get(LONGITUDE_LATITUDE_URL, headers=headers)
            response.raise_for_status()
            return response.text
        except requests.HTTPError:
            logger.error("经纬度索引链接获取异常")

    def get_longAndLat_url_info(self, html):
        """提取各省的经纬信息url"""
        try:
            soup = BeautifulSoup(html, 'lxml')
            table = soup.table
            for url in table.find_all('a'):
                yield url.get('href')
        except requests.HTTPError:
            print('提取各省经纬信息url失败')

    def get_location_info(self, url):
        """获取各省的位置经纬信息"""
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'lxml')
            table = soup.table
            items = table.find_all('tr')
            for i in range(1, len(items)):
                tds = items[i].find_all('td')
                yield {
                    'name': tds[0].string,
                    'longitude': tds[1].string,
                    'latitude': tds[2].string
                }
        except requests.HTTPError:
            print('获取位置经纬信息失败')
