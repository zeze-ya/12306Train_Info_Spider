import ast

from redis import StrictRedis
from loguru import logger
from DataBaseHandler.config import REDIS_HOST, REDIS_PORT, REDIS_DB, PROXY_SCORE_INIT, REDIS_PROXY_POOL_KEY, \
    PROXY_SCORE_MAX, PROXY_SCORE_MIN, PROXY_SCORE_DELETE, REDIS_FAIL_URL_QUEUE_KEY, URL_BATCH, REDIS_TASK_QUEUE_KEY, \
    REDIS_HISTORY_KEY, REDIS_TEMP_TASK_KEY
from ProxyPool.exception.empty import PoolEmptyException
from ProxyPool.utils import is_valid_proxy, convert_proxy_or_proxies_to_dict, choice_proxy


class Redis(object):
    """
    定义Redis操作类
    函数名有proxy的为代理池所用函数，使用set
    函数名中有queue的为任务失败队列所用函数，使用list
    函数名中有task的为任务队列所用函数，使用list
    函数名中有history的为任务记录所用函数，使用set
    """
    def __init__(self):
        """
        初始化Redis
        """
        self.conn = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

    @logger.catch
    def add_proxy_to_redis(self, proxy, score=PROXY_SCORE_INIT):
        """
        向Redis数据库添加数据
        :param proxy: 代理信息
        :param score: 代理分数
        :return: None
        """
        if not is_valid_proxy(proxy):
            """
            判断是否符合代理格式
            """
            logger.debug('无效的代理格式,ip地址:{},端口号:{},不存入', proxy.get('ip'), proxy.get('port'))
            return
        if not self.exists_proxy(proxy):
            """
            判断是否存在于数据库中
            """
            data = proxy.get('ip') + ':' + proxy.get('port')
            logger.debug('有效的代理,端口号:{},ip地址:{}', proxy.get('port'), proxy.get('ip'))
            return self.conn.zadd(REDIS_PROXY_POOL_KEY, score, data)

    @logger.catch
    def get_random_proxy(self):
        """
        从redis中获取一个代理
        如果有满分，则获取满分
        如果没有代理，抛出异常
        :return: proxy_dic, 例如：{'ip':0.0.0.0, 'port':1111}
        """
        # 尝试获取满分代理
        proxies = self.conn.zrangebyscore(REDIS_PROXY_POOL_KEY, PROXY_SCORE_MAX, PROXY_SCORE_MAX)
        if len(proxies):
            proxies_dict = convert_proxy_or_proxies_to_dict(proxies)
            return choice_proxy(proxies_dict)
        # 无满分代理，返回异常
        raise PoolEmptyException

    @logger.catch
    def decrease_proxy(self, proxy):
        """
        对代理池中不符合的进行减分操作，如果分数小于最低分，则删除
        :param proxy: proxy_dic, 例如：{'ip':0.0.0.0, 'port':1111}
        :return: 新分数
        """
        data = str(proxy.get('ip')) + ':' + str(proxy.get('port'))
        score = self.conn.zscore(REDIS_PROXY_POOL_KEY, data)
        # 若当前分数大于最高分
        if score and score > PROXY_SCORE_MIN:
            logger.info('当前代理{}分数为{}，减1', data, score)
            return self.conn.zincrby(REDIS_PROXY_POOL_KEY, data, -1)
        else:
            logger.info('当前代理{}分数{}低于最小值，删除', data, score)
            return self.conn.zrem(REDIS_PROXY_POOL_KEY, data)

    @logger.catch
    def exists_proxy(self, proxy):
        """
        判断此代理是否存在
        :param proxy: proxy_dic, 例如：{'ip':0.0.0.0, 'port':1111}
        :return: bool
        """
        data = proxy.get('ip') + ':' + proxy.get('port')
        return not self.conn.zscore(REDIS_PROXY_POOL_KEY, data) is None

    @logger.catch
    def max_proxy(self, proxy):
        """
        将传入代理的分数置为最高
        :param proxy:
        :return:
        """
        data = proxy.get('ip') + ':' + proxy.get('port')
        logger.info('当前代理{}有效，分数置为{}', data, PROXY_SCORE_MAX)
        return self.conn.zadd(REDIS_PROXY_POOL_KEY, PROXY_SCORE_MAX, data)

    @logger.catch
    def get_proxy_count(self):
        """
        获取当前数据库中的总个数
        :return: int, 例如：100
        """
        return self.conn.zcard(REDIS_PROXY_POOL_KEY)

    @logger.catch
    def get_all_proxy(self):
        """
        获取当前数据库中的所有代理
        :return: list
        """
        return convert_proxy_or_proxies_to_dict(self.conn.zrangebyscore(REDIS_PROXY_POOL_KEY,
                                                                        PROXY_SCORE_MIN, PROXY_SCORE_MAX))

    @logger.catch
    def get_batch_proxy(self, start, end):
        """
        获取一批代理
        :param start:开始索引
        :param end: 结束索引
        :return: list
        """
        return convert_proxy_or_proxies_to_dict(self.conn.zrevrange(REDIS_PROXY_POOL_KEY, start, end - 1))

    @logger.catch
    def remove_useless_proxy(self):
        """
        删除无用代理，无用代理为分数为0到99分的代理
        在代理池更新前
        :return:
        """
        return self.conn.zremrangebyscore(REDIS_PROXY_POOL_KEY, PROXY_SCORE_MIN, PROXY_SCORE_DELETE)

    @logger.catch
    def is_proxy_pool_empty(self):
        """
        判断代理池是否为空
        :return:
        """
        if self.conn.zcount(REDIS_PROXY_POOL_KEY, PROXY_SCORE_MAX, PROXY_SCORE_MAX):
            return False
        else:
            return True

    @logger.catch
    def push_url_to_queue(self, url):
        """
        向队列中添加url,尾进
        :param url:
        :return:
        """
        logger.debug('url = {}入队成功', url)
        return self.conn.rpush(REDIS_FAIL_URL_QUEUE_KEY, url)

    @logger.catch
    def pop_url_from_queue(self):
        """
        每次从队列中取出指定数量的url
        如果数量不足，则取出队列中的所有
        :return:list
        """
        count = self.get_url_queue_length()
        if count >= URL_BATCH:
            logger.info('已取出{}个url，等待重新爬取.....', URL_BATCH)
            url_list = self.conn.lrange(REDIS_FAIL_URL_QUEUE_KEY, 0, URL_BATCH)
            self.conn.ltrim(REDIS_FAIL_URL_QUEUE_KEY, URL_BATCH+1, count)
        else:
            logger.info('已取出{}个url，等待重新爬取.....', count)
            url_list = self.conn.lrange(REDIS_FAIL_URL_QUEUE_KEY, 0, count)
            self.conn.ltrim(REDIS_FAIL_URL_QUEUE_KEY, 0, 0)
            self.conn.lpop(REDIS_FAIL_URL_QUEUE_KEY)
        return url_list

    def is_url_queue_empty(self):
        """
        判断队列是否为空
        :return:
        """
        if self.conn.llen(REDIS_FAIL_URL_QUEUE_KEY) == 0:
            return True
        else:
            return False

    def get_url_queue_length(self):
        """
        获取当前队列长度
        :return:
        """
        return self.conn.llen(REDIS_FAIL_URL_QUEUE_KEY)

    def add_task(self, task):
        """
        向任务队列添加任务
        :param task: from_station:to_station
        :return:
        """
        # 将字符串存入数据库中
        return self.conn.sadd(REDIS_TASK_QUEUE_KEY, task)

    def get_task(self):
        """
        从任务队列中取出一个任务
        :return: task:from_station:to_station
        """
        return self.conn.spop(REDIS_TASK_QUEUE_KEY)

    def get_task_count(self):
        """
        获取当前任务队列中剩余的任务数量
        :return: int: 12314
        """
        return self.conn.scard(REDIS_TASK_QUEUE_KEY)

    def move_task_to_history(self, task_list):
        """
        将已完成的任务，从临时任务队列中，转移到历史队列中
        :return:
        """
        for task in task_list:
            self.conn.smove(REDIS_TEMP_TASK_KEY, REDIS_HISTORY_KEY, task)

    def temp_task_add(self, task_list):
        """
        将从任务队列中获得的任务存入临时队列中，防止程序
        中断后，部分任务丢失，每次开始时对临时任务队列和
        历史队列进行取差集工作判断上次中断时是否有任务已取出未完成
        :param task_list: 任务列表
        :return:
        """
        for task in task_list:
            self.conn.sadd(REDIS_TEMP_TASK_KEY, task)

    def get_temp_task_count(self):
        """
        获取临时任务队列的任务数量
        :return:
        """
        return self.conn.scard(REDIS_TEMP_TASK_KEY)

    def is_incomplete_task(self):
        """
        是否是未完成任务，
        :return:
        """
        if self.get_temp_task_count() == 0:
            return False
        else:
            return True

    @logger.catch
    def get_task_from_temp(self):
        """
        从临时任务队列中获取任务
        :return:
        """
        task_list = []
        for i in range(self.get_temp_task_count()):
            task = self.conn.spop(REDIS_TEMP_TASK_KEY)
            eval1 = ast.literal_eval(task)
            task_list.append(eval1)
        return task_list


if __name__ == '__main__':
    r = Redis()
    # temp = r.get_task_from_temp()
    # for i in temp:
    #     print(i)
    #     print(type(i))
    #     eval1 = ast.literal_eval(i)
    #     print(eval1)
    #     print(type(eval1))
