import random

import requests
from loguru import logger
from ProxyPool.config import BASE_HEADERS, USER_AGENT_LIST

@logger.catch
def get_html(url):
    """
    对爬取网页的封装
    :param url: 待爬取网页链接
    :return: html页面
    """
    headers = BASE_HEADERS
    if 'User-Agent' not in headers.keys():
        headers['User-Agent'] = random.choice(USER_AGENT_LIST)

    try:
        html = requests.get(url, headers=headers)
        if html.status_code == 200:
            logger.debug('抓取页面 {} 成功'.format(url))
            return html.text
    except requests.ConnectionError or requests.RequestException or requests.HTTPError:
        logger.debug('抓取页面失败，url = {} ,等待处理', url)
