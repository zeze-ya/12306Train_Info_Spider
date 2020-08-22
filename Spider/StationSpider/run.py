from Spider.StationSpider.station_spider import StationSpider


class StartStationSpider(object):
    """
        运行全国车站信息爬虫
    """
    
    def __init__(self):
        """
            初始化信息
        """
        self.spider = StationSpider()
    
    def run(self):
        """
            调度运行全国车站信息爬虫
        :return: 
        """
        html = self.spider.get_stations_info()
        for item in self.spider.get_station_name(html):
            self.spider.save_to_mongo(item)
            print(item)


if __name__ == '__main__':
    spider = StartStationSpider()
    spider.run()
