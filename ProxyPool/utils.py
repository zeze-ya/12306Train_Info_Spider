import re
import random
from datetime import date, timedelta

def is_valid_proxy(proxy):
    """
    判断代理格式是否合法
    :param proxy: 爬取代理封装的字典
    :return: True or False
    """
    data = proxy.get('ip') + ':' + proxy.get('port')
    return re.match('\d+\.\d+\.\d+\.\d+\:\d+', data)

def convert_proxy_or_proxies_to_dict(data):
    """
    将从数据库中取出的代理进行数据格式的转换
    :param data: 代理数据
    :return: 例如：dict = {'ip':8.8.8.8,'port':00}
    """
    if not data:
        return None
    # 列表
    if len(data) > 1:
        result = []
        for item in data:
            item = item.strip()
            info = item.split(':')
            _dict = {
                'ip': info[0],
                'port': info[1]
            }
            result.append(_dict)
        return result
    # 单个
    if len(data) == 1:
        info = data[0].strip().split(':')
        _dict = {
            'ip': info[0],
            'port': info[1]
        }
        return _dict

def choice_proxy(data):
    """
    随机返回一个代理字典
    :param data: 代理列表或代理字典
    :return: 代理字典
    """

    if not data:
        return
    # 如果是字典，则只有这一个
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        # index = random.randint(0, len(data) - 1)
        return random.choice(data)

def generate_tomorrow_date():
    tomorrow = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    return tomorrow

def change_proxies_to_string(proxies):
    """
    将查询获得的字典列表转换为字符串列表
    :param proxies:
    :return:
    """
    proxy_string_list = []
    for proxy in proxies:
        ip = proxy.get('ip')
        port = proxy.get('port')
        proxy_string = str(ip) + ':' + str(port)
        proxy_string_list.append(proxy_string)

    return proxy_string_list
