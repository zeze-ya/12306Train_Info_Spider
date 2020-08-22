from DataBaseHandler.redis_handle import Redis

proxy_success = {'ip': '110.243.31.77', 'port': '9999'}
proxy_fail = {'ip': '110.243.31', 'port': '0000'}
proxy_max_sore1 = {'ip': '110.242.31.66', 'port': '9999'}
proxy_max_sore2 = {'ip': '110.242.33.66', 'port': '9999'}

def test():
    """插入测试"""
    redis = Redis()
    # result_success = redis.add_proxy_to_redis(proxy_success)
    # result_fail = redis.add_proxy_to_redis(proxy_fail)
    # print(result_success)
    # print(result_fail)
    # redis.add_proxy_to_redis(proxy_max_sore1, 100)
    # redis.add_proxy_to_redis(proxy_max_sore2, 100)

    """获取代理"""
    # proxy = redis.get_random_proxy()
    # print(proxy)

    # 减分
    # redis.decrease({'ip': '110.242.31.66', 'port': 9999})
    # redis.decrease({'ip': '8.88.8.8', 'port': 90})

    # 置满分
    # redis.max({'ip': '110.243.31.77', 'port': '9999'})

    # 获取总数
    # print(redis.get_count())

    # 获取所有代理
    # print(redis.get_all())

    # 获取指定区间代理
    # print(redis.get_batch(1, 5))
    # print(redis.get_batch(1, 1))
    # print(redis.get_batch(5, 5))
    # print(redis.get_batch(0, 1))
    # print(redis.get_batch(1, 6))
    # print(redis.get_batch(0, 0))

    # 随机获取，无满分分支
    # print(redis.get_random_proxy())

    # 随机获取，无代理情况
    print(redis.get_random_proxy())

    redis.remove_useless_proxy()


if __name__ == '__main__':
    test()
