import json

from flask import Flask, g
from loguru import logger
from DataBaseHandler.redis_handle import Redis
from ProxyPool.config import API_HOST, API_PORT, API_THREADED

app = Flask(__name__)

def get_conn():
    """
    获取redis链接
    :return:
    """
    g.redis = Redis()
    return g.redis

@logger.catch
@app.route('/')
def index():
    """
    索引页
    :return:
    """
    return '<h2>欢迎使用代理池系统</h2><br>' \
           '<h3>请在在地址栏输入以下url，使用代理池</h3><br>' \
           '<h4>1.:/random 随机获取一个代理</h4><br>' \
           '<h4>2.:/count 获取代理池中的代理数量</h4><br>' \

@logger.catch
@app.route('/random')
def get_proxy():
    """
    随机获取一个代理
    :return: ip地址以及端口
    """
    conn = get_conn()
    proxy = conn.get_random_proxy()

    if proxy:
        proxy_json = json.dumps(proxy)
        return proxy_json
    else:
        return '<h5>无有效代理</h5><br>'

@logger.catch
@app.route('/count')
def get_count():
    """
    获取当前代理池中的数量
    :return: count, int
    """
    conn = get_conn()
    return str(conn.get_proxy_count())


if __name__ == '__main__':
    app.debug = True
    app.run(host=API_HOST, port=API_PORT, threaded=API_THREADED)
