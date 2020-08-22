import multiprocessing
import time
from ProxyPool.ProxyGetter import api
from ProxyPool.ProxySave.save import Save
from ProxyPool.ProxyTest.tester import Test
from ProxyPool.config import CYCLE_TESTER, ENABLE_TESTER, CYCLE_GETTER, ENABLE_GETTER, ENABLE_SERVER, API_HOST, \
    API_PORT, API_THREADED
from ProxyPool.utils import generate_tomorrow_date
from loguru import logger

class StartProxyPool(object):
    """
        代理池启动类
    """
    @logger.catch
    def run_tester(self, cycle=CYCLE_TESTER):
        """
        开始测试
        :param cycle: 间隔时间
        :return:
        """
        if not ENABLE_TESTER:
            logger.info('禁止代理测试，退出')
            return

        test_url = 'https://kyfw.12306.cn/otn/leftTicket/init?linktypeid=dc&fs=%E6%88%90%E9%83%BD' \
                   ',CDW&ts=%E5%8C%97%E4%BA%AC,BJP&date={}&flag=N,N,Y'.format(generate_tomorrow_date())
        test = Test(test_url)
        # 记录测试次数
        loop = 0
        while True:
            logger.info("第{}次代理检测开始.....".format(loop))
            test.run()
            loop += 1
            time.sleep(cycle)

    @logger.catch
    def run_getter(self, cycle=CYCLE_GETTER):
        """
        运行代理获取
        :param cycle:
        :return:
        """
        if not ENABLE_GETTER:
            logger.info("禁止获取代理，退出")
            return
        save = Save()
        loop = 0
        # 获取代理前清除代理池中的无效代理
        while True:
            save.clear_useless_proxies()
            logger.info("已清除代理池中的无效代理，目前剩余代理为{}", save.redis.get_proxy_count())
            logger.info('第{}次获取代理开始.....'.format(loop))
            save.run()
            loop += 1
            time.sleep(cycle)

    @logger.catch
    def run_server(self):
        """
        开启web服务
        :return:
        """
        if not ENABLE_SERVER:
            logger.info('web服务已禁用')
            return
        api.app.run(host=API_HOST, port=API_PORT, threaded=API_THREADED)
        api.app.debug = True

    @logger.catch
    def run(self):
        """
        调度整个程序开始运行
        :return:
        """
        test_process = multiprocessing.Process(target=self.run_tester)
        test_process.name = '测试进程'
        logger.info('开始测试，进程名为{}'.format(test_process.name))
        test_process.start()

        getter_process = multiprocessing.Process(target=self.run_getter)
        getter_process.name = '爬取代理进程'
        logger.info('开始爬取代理，进程名为{}'.format(getter_process.name))
        getter_process.start()

        server_process = multiprocessing.Process(target=self.run_server)
        server_process.name = 'web服务进程'
        logger.info('开始web服务，进程名为{}'.format(server_process.name))
        server_process.start()

        test_process.join()
        getter_process.join()
        server_process.join()


if __name__ == '__main__':
    start_proxy_pool = StartProxyPool()
    start_proxy_pool.run()
