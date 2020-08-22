from loguru import logger

from DataBaseHandler.redis_handle import Redis
from Spider.config import STATION_DICT, TASK_QUERY_NUM


class Task(object):
    """
        创建任务
    """

    def __init__(self):
        """
            初始化基本信息
        """
        self.redis = Redis()

    def generate_task(self):
        """
        创建任务信息,并存入数据库
        :return: str: from_station:to_station
        """
        print("任务创建开始")
        for from_station in STATION_DICT.values():
            for to_station in STATION_DICT.values():
                if from_station != to_station:
                    task = str(from_station) + ':' + str(to_station)
                    self.redis.add_task(task)
        print("任务创建完成")

    def get_task(self):
        """
        获取任务队列中的一定任务数量，进行操作
        :return:
        """
        task_list = []
        num = self.get_task_num()
        logger.info('当前任务队列中有任务：{}', num)
        if num > TASK_QUERY_NUM:
            for i in range(TASK_QUERY_NUM):
                task_list.append(self.redis.get_task())
            return task_list
        else:
            for i in range(num):
                task_list.append(self.redis.get_task())
            return task_list

    def get_task_num(self):
        """
        获取队列中的任务数量
        :return:
        """
        return self.redis.get_task_count()

    def is_queue_empty(self):
        """
        队列是否为空
        :return:
        """
        if self.redis.get_task_count() == 0:
            return True
        else:
            return False


if __name__ == '__main__':
    task = Task()
    # task.generate_task()
    # task_list = task.get_task()
    # task.remove_task_to_history(task_list)
    # print("完成")
    task.generate_task()
    print("完成")
