from Spider.LALSpider.longitudeAndLatitude_spider import LALSpider
from Spider.config import JW_URL


class StartLALSpider(object):
    """
        启动爬取各地经纬度的爬虫
    """
    def __init__(self):
        """
            初始化
        """
        self.spider = LALSpider()

    def run(self):
        html = self.spider.get_longAndLat_url_response()
        for url in self.spider.get_longAndLat_url_info(html):
            url = JW_URL + url
            for location_info in self.spider.get_location_info(url):
                print(location_info)
                # 存入数据库
                # self.spider.db_handle.insert(location_info, MONGO_TABLE_JW)


if __name__ == '__main__':
    start_lal_spider = StartLALSpider()
    start_lal_spider.run()