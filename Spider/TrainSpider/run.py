import time

from Spider import utils
from loguru import logger
from multiprocessing import Pool
from Spider.TrainSpider.generate_task import Task
from Spider.TrainSpider.train_spider import TrainSpider
from Spider.config import WAIT_TIME, PROCESS_WAIT_TIME, TAKT_TIME, JION_WAIT_TIME


class StartTrainSpider(object):
    """
        运行火车车次信息爬虫
    """

    def __init__(self):
        """初始化操作信息"""
        self.spider = TrainSpider()
        self.task = Task()

    def is_task_in_queue(self):
        """
        判断当前任务队列中是否还有任务
        :return:
        """
        if self.task.is_queue_empty():
            logger.info("任务队列队列为空，爬取完成")
            return False
        else:
            return True

    def generate_info_dict_list(self):
        """
        生成爬虫所用的字典
        :return: list
        """
        task_list = self.task.get_task()
        date = utils.generate_tomorrow_date()
        info_list = []
        for info in task_list:
            infos = info.split(':')
            result = {
                'from_station': infos[0],
                'to_station': infos[1],
                'date': date,

            }
            info_list.append(result)
        return info_list


def start(items):
    """
    爬取入口函数
    :param items:车站相关信息
    :return:
    """
    spider = TrainSpider()
    # items = utils.generate_all_url_value()
    # for item in items:
    #     train_url = get_train_url(item.get('from_station'), item.get('to_station'), item.get('data'))
    #     response = get_train_url_response(train_url)
    #     for info in parse_train_info(response):
    #         url = generate_station_info_url(info.get('列车号'), info.get('始发站'), info.get('终点站'),
    #                                         utils.generate_tomorrow_date())
    #         html = get_station_info_response(url)
    #         parse_station_info_response(html, info)

    # """测试数据"""
    # url = get_train_url('BJP', 'TYV', '2020-04-21')
    # print(url)
    # response = get_train_url_response(url)
    # for item in parse_train_info(response):
    #     print(item)
    # 初始化信息
    items = spider.check_temp_task(items)
    spider.add_task_to_temp(items)
    count = 0
    for item in items:
        logger.debug("当前TrainSpider类信息:{}", spider)
        if isinstance(item, dict):
            from_station_name = utils.get_value_by_key(key=item.get('from_station'))
            to_station_name = utils.get_value_by_key(key=item.get('to_station'))

            logger.info('正在爬取 {} 到 {} 的车次', from_station_name, to_station_name)
            train_url = spider.get_train_url(item.get('from_station'), item.get('to_station'), item.get('date'))
            response = spider.get_url_response(train_url)
            for info in spider.parse_train_info(response):
                url = spider.generate_station_info_url(info.get('列车号'), info.get('始发站'), info.get('终点站'),
                                                       utils.generate_tomorrow_date())
                html = spider.get_url_response(url)
                spider.parse_station_info_response(html, info)
            logger.info('爬取 {} 到 {} 的车次已完成', from_station_name, to_station_name)
            count += 1
            if count % 100 == 0:
                if count - 100 > 0:
                    start_index = count - 100
                else:
                    start_index = 0
                spider.move_task_to_history(items[start_index:count])
        else:
            logger.info('数据非字典类型{}', item)
    logger.info('已完成{}个任务', len(items))


if __name__ == '__main__':
    # pool = Pool(processes=4)
    start_train_spider = StartTrainSpider()
    loop = 0
    while start_train_spider.is_task_in_queue():
        # 有间隔时间的加入
        time.sleep(JION_WAIT_TIME)
        info_dict_list = start_train_spider.generate_info_dict_list()
        start(info_dict_list)
    logger.info("开始处理爬取失败的url......")
    spider = TrainSpider()
    spider.deal_url_in_queue()
    logger.info("爬取车次信息完成")
