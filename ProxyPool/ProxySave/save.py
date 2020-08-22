from DataBaseHandler.redis_handle import Redis
from ProxyPool.ProxySpider.proxy_spider import Spider
from ProxyPool.config import PROXY_NUMBER_MAX
from loguru import logger

class Save(object):
    """
        将爬取代理存入数据库中
    """
    def __init__(self):
        self.redis = Redis()
        self.spider = Spider()

    def is_full(self):
        """
        代理池是否满
        :return:
        """
        return self.redis.get_proxy_count() >= PROXY_NUMBER_MAX

    def clear_useless_proxies(self):
        """
        清除代理池中的无效代理
        :return:
        """
        return self.redis.remove_useless_proxy()

    @logger.catch
    def run(self):
        if self.is_full():
            return
        else:
            for site_label_index in range(self.spider.__SpiderFuncCount__):
                site = self.spider.__SpiderName__[site_label_index]
                proxies = self.spider.get_proxies(site)
                for proxy in proxies:
                    self.redis.add_proxy_to_redis(proxy)


if __name__ == '__main__':
    save = Save()
    save.run()
