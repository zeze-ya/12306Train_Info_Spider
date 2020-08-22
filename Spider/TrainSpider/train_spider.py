import json
import random
import re
from urllib.parse import urlencode
import requests
from DataBaseHandler.config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT, \
    MYSQL_ROUTE_TABLE, MYSQL_TRAIN_TABLE
from DataBaseHandler.mysql_handle import MySQL
from DataBaseHandler.redis_handle import Redis
from ProxyPool.config import NONE_THING_WAIT
from Spider import utils
from Spider.config import *
import time
from loguru import logger


class TrainSpider(object):
    def __init__(self):
        """
        初始化信息
        """
        # MySQL数据库
        self.mysql_handle = MySQL(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT)
        self.redis_handle = Redis()
        self.proxy = {}

    def get_train_url(self, from_station_code, to_station_code, date):
        """
        构建用于查询车次信息的url
        :param from_station_code:发车地代码
        :param to_station_code: 到达地代码
        :param date: 火车日期
        :return:
        """
        # 查询车次的相关信息
        data = {
            'leftTicketDTO.train_date': str(date),
            'leftTicketDTO.from_station': from_station_code,
            'leftTicketDTO.to_station': to_station_code,
            'purpose_codes': 'ADULT'
        }
        # 构建url
        url = QUERY_URL + urlencode(data)
        logger.debug('车次查询url构建完成，url = {}', url)
        return url

    @logger.catch
    def get_url_response(self, url, re_times=0):
        """
        获取查询的url的响应值
        需要在重新爬取的代码中添加判断逻辑对获取的url进行判断是车次基本信息还是车次详细信息
        :param url: 查询链接
        :param re_times: 重新查询次数
        :return: 访问url的响应值
        """
        headers = BASE_HEADERS
        if 'User-Agent' not in headers.keys():
            headers['User-Agent'] = random.choice(USER_AGENT_LIST)
        time.sleep(WAIT_TIME)
        if re_times == 5:
            # 重新尝试超过五次
            logger.error('失败次数过多，已将url加载进入重新爬取队列中，待程序结束后重新爬取')
            logger.info('url={}', url)
            # 添加进入重新爬取队列
            self.redis_handle.push_url_to_queue(url)
            return
        try:
            # 首次爬取, 且本机ip未被封
            if re_times == 0 and not self.proxy:
                logger.info("未使用代理")
                response = requests.get(url, headers=headers, cookies=COOKIES, allow_redirects=False, timeout=TIMEOUT)
                if response.status_code == 200:
                    logger.info('访问车次查询url = {},成功', url)
                    return response.text
                else:
                    if response.status_code == 302:
                        logger.debug('访问出现跳转，ip被封，需更换代理进行访问，url = {}', url)
                        # 更换代理
                        re_times = self.change_proxy(re_times)
                        return self.get_url_response(url, re_times)
            else:
                # 非首次爬取或本机ip被封，需要使用代理
                data = self.proxy.get('ip') + ':' + self.proxy.get('port')
                proxy = {
                    "http": f'http://{data}',
                    "https": f'http://{data}'
                }
                logger.debug('更换代理为 {}', data)
                response = requests.get(url, headers=headers, cookies=COOKIES, allow_redirects=False, proxies=proxy,
                                        timeout=TIMEOUT)
                if response.status_code == 200:
                    logger.info('访问车次查询url = {},成功', url)
                    logger.info('使用代理为：{}', data)
                    return response.text
                else:
                    if response.status_code == 302:
                        logger.debug('访问出现跳转，ip被封，需更换代理进行访问，url = {}', url)
                        # 处理已使用代理,减分
                        if self.proxy:
                            self.redis_handle.decrease_proxy(self.proxy)
                        # 获取新的代理
                        re_times = self.change_proxy(re_times)
                        return self.get_url_response(url, re_times)
        except:
            logger.error("获取车次查询响应异常,url = {}", url)
            # 处理已使用代理,减分
            if self.proxy:
                self.redis_handle.decrease_proxy(self.proxy)
            # 获取新的代理
            re_times = self.change_proxy(re_times)
            return self.get_url_response(url, re_times)

    @logger.catch
    def parse_train_info(self, trains_url_response):
        """
        对返回的响应进行解析，整理
        :param trains_url_response: 访问车次网站的响应值
        :return: 车次信息所封装的字典
        """
        try:
            """
            12306中的动车组开头为G，C，D,分割字符串进行判断即可
            """
            if trains_url_response:
                trains_info = json.loads(trains_url_response)
                if trains_info and 'data' in trains_info.keys():
                    trains_info_data = trains_info.get('data')
                    if trains_info_data and 'result' in trains_info_data.keys():
                        info = trains_info_data.get('result')
                        for items in info:
                            item = items.split('|')
                            if item[16] == '01':
                                if item[1] == '预订':
                                    """判断是否为非停运状态"""
                                    if 'G' in item[3]:
                                        """判断是否是动车组"""
                                        logger.info('正在爬取车次： {} ', item[3])
                                        yield {
                                            '状态': item[1],
                                            '列车号': item[2],
                                            '车次名': item[3],
                                            '始发站': item[4],
                                            '终点站': item[5],
                                            '乘车站': item[6],
                                            '目标站': item[7],
                                            '发车时间': item[8],
                                            '到站时间': item[9],
                                            '历时': item[10],
                                            '始发站编号': item[16],
                                            '目标站编号': item[17],
                                            # '高级软卧': item[21],
                                            # '软卧': item[23],
                                            # '无座': item[26],
                                            # '硬卧': item[28],
                                            # '硬座': item[29],
                                            # '二等座': item[30],
                                            # '一等座': item[31],
                                            # '商务': item[25],
                                            # '座位类型': item[35]
                                        }
                                    else:
                                        logger.debug('列车{}非高铁车次，不爬取', item[3])
                                        time.sleep(NONE_THING_WAIT)
                                else:
                                    logger.debug('列车{}停运，不爬取', item[3])
                                    time.sleep(NONE_THING_WAIT)
                            else:
                                logger.debug('非始发站列车{}防止重复，不爬取', item[3])
                                time.sleep(NONE_THING_WAIT)
                    else:
                        logger.debug('返回信息异常，返回信息为 {} ', trains_url_response)
                        time.sleep(NONE_THING_WAIT)
                else:
                    logger.debug('返回信息异常，返回信息为 {} ', trains_url_response)
                    time.sleep(NONE_THING_WAIT)
            else:
                logger.debug('返回信息异常，返回信息为 {} ', trains_url_response)
                time.sleep(NONE_THING_WAIT)
        except:
            logger.error('json解析异常, json:{}', trains_url_response)

    def generate_station_info_url(self, train_no, from_station_telecode, to_station_telecode, depart_date):
        """
        生成车次详细信息的url
        :param train_no: 12306车次编码
        :param from_station_telecode: 起始站编码
        :param to_station_telecode: 终点站编码
        :param depart_date: 离开日期
        :return: 返回车次详细信息url
        """
        data = {
            'train_no': train_no,
            'from_station_telecode': from_station_telecode,
            'to_station_telecode': to_station_telecode,
            'depart_date': depart_date
        }

        url = STATION_INFO_URL + urlencode(data)
        logger.info('生成车次详细信息查询链接成功，url = {}', url)
        return url

    @logger.catch
    def parse_station_info_response(self, html, train_info={}):
        """
        解析车次详细信息
        :param html: 车次详细信息url响应值
        :param train_info: 车次基本信息
        :return: 车次详细信息
        """
        try:
            if html:
                items = json.loads(html)
                if items and items.get('data'):
                    item = items.get('data')
                    if item and item.get('data'):
                        data = item.get('data')
                        station_train_code = data[0].get('station_train_code')
                        # 存储车次信息
                        if train_info:
                            train_data = {
                                'id': train_info.get('列车号'),
                                'start_time': train_info.get('发车时间'),
                                'end_time': train_info.get('到站时间'),
                                'start_station_name': data[0].get('start_station_name'),
                                'end_station_name': data[0].get('end_station_name'),
                                'train_id': station_train_code,
                                'total_station_num': str(len(data)),
                                'train_class_name': data[0].get('train_class_name'),
                                'total_time': train_info.get('历时'),
                            }

                            logger.info('爬取车次 {} 成功，正在准备入库', train_data.get('train_id'))
                            self.save_to_mysql_train(train_data)
                        # 存储车次中转信息
                        for info in data:
                            result = {
                                'id': station_train_code + info.get('station_no'),
                                'train_id': station_train_code,
                                'station_name': info.get('station_name'),
                                'route_seq': info.get('station_no'),
                                'departure_time': info.get('start_time'),
                                'arrive_time': info.get('arrive_time'),
                                'stopover_time': info.get('stopover_time')
                            }
                            logger.info('爬取车次 {} 中转详细信息成功,即将入库....', station_train_code)
                            self.save_to_mysql_route(result)
                        logger.info('爬取车次 {} 中转详细信息成功', station_train_code)
                    else:
                        logger.debug('信息为空{}', item)
                        time.sleep(NONE_THING_WAIT)
                else:
                    logger.debug('信息为空{}', items)
                    time.sleep(NONE_THING_WAIT)
            else:
                logger.debug('信息为空{}', html)
                time.sleep(NONE_THING_WAIT)
        except:
            logger.error("解析车次信息失败, url访问返回值为 {} ", html)

    def save_to_mysql_route(self, data):
        """
        将中转信息存入mysql数据库中
        :param data: 存入数据库的数据
        :return:
        """
        logger.info('车次： {} 中转信息正在入库.....', data.get('train_id'))
        self.mysql_handle.de_duplication(data, MYSQL_ROUTE_TABLE)

    def save_to_mysql_train(self, data):
        """
        将车次信息存入mysql数据库中
        :return:
        """
        logger.info('车次： {} 信息正在入库.....', data.get('train_id'))
        self.mysql_handle.de_duplication(data, MYSQL_TRAIN_TABLE)

    def move_task_to_history(self, task_list):
        """
        将完成的任务从临时队列中放入history数据库中
        :param task_list:
        :return:
        """
        logger.info("正在将{}已完成的任务，存入历史记录中", len(task_list))
        self.redis_handle.move_task_to_history(task_list)

    @logger.catch
    def get_proxy_from_proxypoll(self):
        """
        从线程池中获取代理
        :return: proxy：{}
        """
        try:
            url = 'http://localhost:5555/random'
            response = requests.get(url)
            if response.text == '<h5>无有效代理</h5><br>':
                proxy = None
            else:
                proxy = json.loads(response.text)
        except requests.HTTPError:
            logger.error('获取代理失败')
        return proxy

    @logger.catch
    def deal_url_in_queue(self):
        """
        处理爬取失败的url链接
        1.使用re库判断链接类型
        分类型对链接重复进行爬取处理
        :return:
        """
        # 车次信息
        train_patten = re.compile('https://kyfw.12306.cn/otn/leftTicket/query')
        # 详细车次信息
        train_info_patten = re.compile('https://kyfw.12306.cn/otn/czxx/queryByTrainNo')

        # 如果url队列不为空
        while not self.redis_handle.is_url_queue_empty():
            logger.info('开始处理爬取失败的url链接.......')

            failed_urls = self.redis_handle.pop_url_from_queue()
            for failed_url in failed_urls:
                if re.match(train_patten, failed_url) is not None:
                    # 车次爬取流程
                    response = self.get_url_response(failed_url)
                    for info in self.parse_train_info(response):
                        url = self.generate_station_info_url(info.get('列车号'), info.get('始发站'), info.get('终点站'),
                                                               utils.generate_tomorrow_date())
                        html = self.get_url_response(url)
                        self.parse_station_info_response(html, info)
                        time.sleep(WAIT_TIME)
                if re.match(train_info_patten, failed_url) is not None:
                    # 详细信息爬取流程
                    html = self.get_url_response(failed_url)
                    self.parse_station_info_response(html)

    def change_proxy(self, re_times):
        """
        更换可用代理，
        如果没有可用代理，则将程序进行休眠
        :param re_times:
        :return:
        """
        if self.redis_handle.is_proxy_pool_empty():
            re_times = 0
            self.proxy = None
            logger.info('本机ip被封且无可用代理，程序休眠10分钟')
            time.sleep(WAIT_FOR_PROXY_TIME)
            logger.info('程序休眠结束，继续爬取信息')
            return re_times
        else:
            self.proxy = self.get_proxy_from_proxypoll()
            re_times += 1
            return re_times

    def add_task_to_temp(self, task_list):
        """
        将任务在开始前加入临时任务队列
        :param task_list: list
        :return:
        """
        logger.info("已将任务添加到临时队列中")
        self.redis_handle.temp_task_add(task_list)

    def check_temp_task(self, items):
        """
        任务开始前检测是否存在未完成任务，如有，则加入待完成队列
        :param items:任务队列
        :return: 任务队列
        """
        logger.info("开始检查临时任务队列中是否存在任务....")
        if self.redis_handle.is_incomplete_task():
            count = self.redis_handle.get_temp_task_count()
            logger.info('检查完毕，有未完成的任务，任务数为{}', count)
            logger.info('准备开始添加.......')
            task_list = self.redis_handle.get_task_from_temp()
            for task in task_list:
                items.append(task)
            logger.info('添加完成，正在返回任务队列')
            return items
        else:
            logger.info('检查完毕，无未完成任务，准备读取下一个任务队列.....')
            return items

    @logger.catch
    def parse_train_info_for_api(self, trains_url_response):
        """
        对返回的响应为api进行解析，整理
        :param trains_url_response: 访问车次网站的响应值
        :return: 车次信息所封装的字典
        """
        try:
            """
            12306中的动车组开头为G，C，D,分割字符串进行判断即可
            """
            if trains_url_response:
                trains_info = json.loads(trains_url_response)
                if trains_info and 'data' in trains_info.keys():
                    trains_info_data = trains_info.get('data')
                    if trains_info_data and 'result' in trains_info_data.keys():
                        info = trains_info_data.get('result')
                        for items in info:
                            item = items.split('|')
                            if item[1] == '预订':
                                """判断是否为非停运状态"""
                                if 'G' in item[3]:
                                    """判断是否是动车组"""
                                    logger.info('正在爬取车次： {} ', item[3])
                                    yield {
                                        '状态': item[1],
                                        '列车号': item[2],
                                        '车次名': item[3],
                                        '始发站': item[4],
                                        '终点站': item[5],
                                        '乘车站': item[6],
                                        '目标站': item[7],
                                        '发车时间': item[8],
                                        '到站时间': item[9],
                                        '历时': item[10],
                                        '出发站编号': item[16],
                                        '目标站编号': item[17],
                                        # '高级软卧': item[21],
                                        # '软卧': item[23],
                                        # '无座': item[26],
                                        # '硬卧': item[28],
                                        # '硬座': item[29],
                                        '二等座': item[30],
                                        '一等座': item[31],
                                        '商务': item[25],
                                        '座位类型': item[35]
                                    }
                                else:
                                    logger.debug('列车{}非高铁车次，不爬取', item[3])
                                    time.sleep(NONE_THING_WAIT)
                            else:
                                logger.debug('列车{}停运，不爬取', item[3])
                                time.sleep(NONE_THING_WAIT)
                    else:
                        logger.debug('返回信息异常，返回信息为 {} ', trains_url_response)
                        time.sleep(NONE_THING_WAIT)
                else:
                    logger.debug('返回信息异常，返回信息为 {} ', trains_url_response)
                    time.sleep(NONE_THING_WAIT)
            else:
                logger.debug('返回信息异常，返回信息为 {} ', trains_url_response)
                time.sleep(NONE_THING_WAIT)
        except:
            logger.error('json解析异常, json:{}', trains_url_response)

    @logger.catch
    def parse_station_info_response_for_api(self, html, train_start_seq_info, train_end_seq_info):
        """
        解析车次详细信息
        :param html: 车次详细信息url响应值
        :param train_start_seq_info: 车次出发站号信息
        :param train_end_seq_info: 车次目标站号信息
        :return: 车次详细信息
        """
        try:
            if html:
                items = json.loads(html)
                if items and items.get('data'):
                    item = items.get('data')
                    if item and item.get('data'):
                        data = item.get('data')
                        # 存储车次中转信息
                        train_seq_list = []
                        for info in data:
                            if train_start_seq_info <= info.get('station_no') <= train_end_seq_info:
                                result = {
                                    '车站名': info.get('station_name'),
                                    '车站次序': info.get('station_no'),
                                    '发车时间': info.get('start_time'),
                                    '到站时间': info.get('arrive_time'),
                                    '经停时间': info.get('stopover_time')
                                }
                                train_seq_list.append(result)
                        return train_seq_list
                    else:
                        logger.debug('信息为空{}', item)
                        time.sleep(NONE_THING_WAIT)
                else:
                    logger.debug('信息为空{}', items)
                    time.sleep(NONE_THING_WAIT)
            else:
                logger.debug('信息为空{}', html)
                time.sleep(NONE_THING_WAIT)
        except:
            logger.error("解析车次信息失败, url访问返回值为 {} ", html)

    def generate_train_price_url(self, train_no, from_station_no, to_station_no, date):
        """
        生成价格查询url
        :param train_no:火车编号
        :param from_station_no: 出发站编号
        :param to_station_no: 目的站编号
        :param date: 日期
        :return: 价格查询url
        """
        # 查询车次票价的相关信息
        data = {
            'train_no': train_no,
            'from_station_no': from_station_no,
            'to_station_no': to_station_no,
            'seat_types': 'OM9',
            'train_date': date
        }
        # 构建url
        url = PRICE_URL + urlencode(data)
        logger.debug('车次查询url构建完成，url = {}', url)
        return url

    def parse_price_response_for_api(self, price_response):
        """
        解析价格
        :param price_response: 价格响应
        :return: 价格字典
        """
        try:
            if price_response:
                items = json.loads(price_response)
                if items and items.get('data'):
                    item = items.get('data')
                    price = {
                        '二等座': item.get('O'),
                        '一等座': item.get('M'),
                        '特等座': item.get('A9')
                    }
                    return price
                else:
                    logger.debug("无票价信息, {}", items)
            else:
                logger.debug('响应为空')
        except:
            logger.debug("响应值有误，{}", price_response)


if __name__ == '__main__':
    spider = TrainSpider()
    # spider.change_proxy(1)
    # spider.deal_url_in_queue()
    #
    # response = spider.get_url_response(
    #     'https://kyfw.12306.cn/otn/leftTicket/query?leftTicketDTO.train_date=2020-05-23&leftTicketDTO.from_station=QNB&leftTicketDTO.to_station=DFT&purpose_codes=ADULT')
    # for item in spider.parse_train_info(response):
    #     print(item)
