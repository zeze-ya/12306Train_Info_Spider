import time
import json
from loguru import logger
from ProxyPool.ProxySpider.page_spider import get_html
from ProxyPool.config import MAX_PAGE, PAGE_WAIT_TIME
from bs4 import BeautifulSoup

class ProxyMetaclass(type):
    """
        元类，在Spider类中加入
        __SpiderFunc__和__SpiderFuncCount__
        两个参数，分别表示爬虫函数，以及爬虫函数的数量
    """
    def __new__(mcs, name, bases, attrs):
        count = 0
        attrs['__SpiderFunc__'] = []
        attrs['__SpiderName__'] = []

        for k, v in attrs.items():
            if 'spider_' in k:
                attrs['__SpiderName__'].append(k)
                attrs['__SpiderFunc__'].append(v)
                count += 1

        for k in attrs['__SpiderName__']:
            attrs.pop(k)
        attrs['__SpiderFuncCount__'] = count
        return type.__new__(mcs, name, bases, attrs)

class Spider(object, metaclass=ProxyMetaclass):
    @logger.catch
    def get_proxies(self, site):
        """

        :param site:
        :return: list:[]
        """
        proxies = []
        print('Site', site)
        for func in self.__SpiderFunc__:
            if func.__name__ == site:
                this_page_proxies = func(self)
                for proxy in this_page_proxies:
                    logger.info("获取代理ip : {}, port : {},来源：{}", proxy.get('ip'), proxy.get('port'), site)
                    proxies.append(proxy)
        return proxies

    @logger.catch
    def spider_daili66(self):
        """
        获取代理66
        :return: 代理
        """
        base_url_daili66 = 'http://www.66ip.cn/{}.html'
        urls = [base_url_daili66.format(page) for page in range(1, MAX_PAGE + 1)]
        for url in urls:
            logger.info('正在从代理66获取代理url = {}', url)
            html = get_html(url)
            if html:
                soup = BeautifulSoup(html, 'lxml')
                table = soup.find_all('table')
                trs = table[2].find_all('tr')
                for index in range(1, len(trs)):
                    tds = trs[index].find_all('td')
                    yield {
                        'ip': tds[0].string,
                        'port': tds[1].string
                    }

    @logger.catch
    def spider_ip3366(self):
        """
        获取代理ip3366
        :return:代理
        """
        base_url_ip3366 = 'http://www.ip3366.net/?stype=1&page={}'
        urls = [base_url_ip3366.format(page) for page in range(1, MAX_PAGE + 1)]
        for url in urls:
            logger.info('正在从ip3366获取代理url = {}', url)
            html = get_html(url)
            if html:
                soup = BeautifulSoup(html, 'lxml')
                tables = soup.find_all('table')
                trs = tables[0].find_all('tr')
                for index in range(1, len(trs)):
                    tds = trs[index].find_all('td')
                    yield {
                        'ip': tds[0].string,
                        'port': tds[1].string
                    }

    @logger.catch
    def spider_kuaidaili(self):
        """
        获取快代理
        :param page_count: 页数
        :return: 代理
        """
        base_url_kuaidaili = 'https://www.kuaidaili.com/free/inha/{}/'
        urls = [base_url_kuaidaili.format(page) for page in range(1, MAX_PAGE + 1)]
        for url in urls:
            logger.info('正在从快代理获取代理url = {}', url)
            time.sleep(PAGE_WAIT_TIME)
            html = get_html(url)
            if html:
                soup = BeautifulSoup(html, 'lxml')
                tbody = soup.find_all('tbody')
                trs = tbody[0].find_all('tr')
                for tr in trs:
                    tds = tr.find_all('td')
                    yield {
                        'ip': tds[0].string,
                        'port': tds[1].string
                    }

    @logger.catch
    def spider_xicidaili(self):
        """
        获取西刺免费代理ip
        :return:
        """
        base_url_xicidaili = 'https://www.xicidaili.com/nn/{}'
        urls = [base_url_xicidaili.format(page) for page in range(1, MAX_PAGE+1)]
        for url in urls:
            logger.info('正在从西刺免费代理获取代理url = {}', url)
            html = get_html(url)
            if html:
                soup = BeautifulSoup(html, 'lxml')
                table = soup.find('table')
                trs = table.find_all('tr')
                for index in range(1, len(trs)):
                    tds = trs[index].find_all('td')
                    yield {
                        'ip': tds[1].string,
                        'port': tds[2].string
                    }

    @logger.catch
    def spider_89ip(self):
        """
        获取89免费代理ip
        :return:
        """
        base_url_89ip = 'http://www.89ip.cn/index_{}.html'
        urls = [base_url_89ip.format(page) for page in range(1,MAX_PAGE)]
        for url in urls:
            logger.info('正在从89代理获取代理url = {}', url)
            html = get_html(url)
            if html:
                soup = BeautifulSoup(html, 'lxml')
                tbody = soup.find_all('tbody')
                trs = tbody[0].find_all('tr')
                for tr in trs:
                    tds = tr.find_all('td')
                    yield {
                        'ip': tds[0].string.replace('\n', '').replace('\t', ''),
                        'port': tds[1].string.replace('\n', '').replace('\t', '')
                    }

    @logger.catch
    def spider_superfastip(self):
        """
        获取急速代理ip
        :return:
        """
        base_url_superfastip = 'https://api.superfastip.com/ip/freeip?page={}'
        urls = [base_url_superfastip.format(page) for page in range(1, MAX_PAGE + 1)]
        for url in urls:
            logger.info('正在从急速代理获取代理url = {}', url)
            html = get_html(url)
            if html:
                data = json.loads(html)
                if data and 'freeips' in data.keys():
                    ips = data.get('freeips')
                    for ip in ips:
                        yield {
                            'ip': ip.get('ip'),
                            'port': ip.get('port')
                        }


if __name__ == '__main__':
    spider = Spider()
    for item in spider.spider_kuaidaili():
        print(item)