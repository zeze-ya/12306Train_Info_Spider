import json
import re

from flask import Flask
from loguru import logger

from Spider.TrainSpider.train_spider import TrainSpider
from Spider.config import API_HOST, API_PORT, API_THREADED, re_date, STATION_DICT
from Spider.utils import get_station_code, get_value_by_key

app = Flask(__name__)

@logger.catch
@app.route('/')
def index():
    """
    索引页
    :return:
    """
    return '<h2>欢迎使用车次实时信息查询接口</h2><br>' \
           '<h3>请在在地址栏输入以下url，=后皆为变量，使用该接口</h3><br>' \
           '<h4>1.:/train/from_station=天津/to_station=北京/date=2020-2-19</h4><br>' \
           '<h5>获取date日期时从from_station到to_station的详细车次信息,请尽量输入精准信息，否则查询时间会延长，并且可能出现查询失败的情况<h5><br>'\
           '<h4>2.:/train_name/train_no=040000G76602/from_station=天津/to_station=北京/date=2020-2-19</h4><br>' \
           '<h5>获取列车040000G76602在2020-1-14日从北京到天津的座位情况及票价信息<h5><br>' \

@app.route('/train/from_station=<from_station>/to_station=<to_station>/date=<date>')
def get_train_info_by_station(from_station, to_station, date):
    # 由起始站，终点站，日期获取车次详细信息
    spider = TrainSpider()
    from_station_code_list = get_station_code(from_station)
    to_station_code_list = get_station_code(to_station)
    date_pat = re.compile(re_date)
    match = re.match(date_pat, date)
    if match and to_station_code_list and from_station_code_list:
        urls = []
        # 生成url
        for from_station_code in from_station_code_list:
            for to_station_code in to_station_code_list:
                url = spider.get_train_url(from_station_code, to_station_code, date)
                urls.append(url)

        # 获取url响应
        train_info_list =[]
        for url in urls:
            response = spider.get_url_response(url)
            for info in spider.parse_train_info_for_api(response):
                # 获取列车详细信息
                info_url = spider.generate_station_info_url(info.get('列车号'), info.get('始发站'), info.get('终点站'), date)
                info_response = spider.get_url_response(info_url)
                train_seq_list = spider.parse_station_info_response_for_api(info_response, info.get('出发站编号'), info.get('目标站编号'))
                # 获取票价信息
                price_url = spider.generate_train_price_url(info.get('列车号'), info.get('出发站编号'), info.get('目标站编号'), date)
                price_response = spider.get_url_response(price_url)
                price_dict = spider.parse_price_response_for_api(price_response)
                train_info = {
                    'state': info.get('状态'),
                    'train_name': info.get('车次名'),
                    'start_station': get_value_by_key(info.get('始发站')),
                    'end_station': get_value_by_key(info.get('终点站')),
                    'from_station': get_value_by_key(info.get('乘车站')),
                    'to_station': get_value_by_key(info.get('目标站')),
                    'start_time': info.get('发车时间'),
                    'end_time': info.get('到站时间'),
                    'total_time': info.get('历时'),
                    'second-class seats': info.get('二等座'),
                    'first-class seats': info.get('一等座'),
                    'Business seat': info.get('商务座'),
                    'train_route': train_seq_list,
                    'price': price_dict
                }
                train_info_list.append(train_info)

        json_dict = {
            'state': '成功',
            'data': train_info_list
        }

        return json.dumps(json_dict)

    else:
        return '请检查起始站名，终点站名或日期格式是否有误，日期格式为yyyy-mm-dd'

@app.route('/train_name/train_no=<train_no>/from_station=<from_station>/to_station=<to_station>/date=<date>')
def get_train_info_by_train_no(train_no, from_station, to_station, date):
    # 由火车id获取详细信息
    spider = TrainSpider()
    from_station_code = STATION_DICT.get(from_station)
    to_station_code = STATION_DICT.get(to_station)
    date_pat = re.compile(re_date)
    match = re.match(date_pat, date)
    if match and to_station_code and from_station_code:
        # 生成url
        url = spider.get_train_url(from_station_code, to_station_code, date)
        # 获取url响应
        response = spider.get_url_response(url)
        for info in spider.parse_train_info_for_api(response):
            if info.get('列车号') == train_no:
                # 获取列车详细信息
                info_url = spider.generate_station_info_url(info.get('列车号'), info.get('始发站'), info.get('终点站'), date)
                info_response = spider.get_url_response(info_url)
                train_seq_list = spider.parse_station_info_response_for_api(info_response, info.get('出发站编号'), info.get('目标站编号'))
                # 获取票价信息
                price_url = spider.generate_train_price_url(info.get('列车号'), info.get('出发站编号'), info.get('目标站编号'), date)
                price_response = spider.get_url_response(price_url)
                price_dict = spider.parse_price_response_for_api(price_response)
                train_info = {
                    'state': info.get('状态'),
                    'train_name': info.get('车次名'),
                    'start_station': get_value_by_key(info.get('始发站')),
                    'end_station': get_value_by_key(info.get('终点站')),
                    'from_station': get_value_by_key(info.get('乘车站')),
                    'to_station': get_value_by_key(info.get('目标站')),
                    'start_time': info.get('发车时间'),
                    'end_time': info.get('到站时间'),
                    'total_time': info.get('历时'),
                    'second-class seats': info.get('二等座'),
                    'first-class seats': info.get('一等座'),
                    'Business seat': info.get('商务'),
                    'train_route': train_seq_list,
                    'price': price_dict
                }

        json_dict = {
            'state': 'success',
            'data': train_info
        }

        return json.dumps(json_dict)

    else:
        return '请检查起始站名，终点站名或日期格式是否有误，日期格式为yyyy-mm-dd'


if __name__ == '__main__':
    app.debug = True
    app.run(host=API_HOST, port=API_PORT, threaded=API_THREADED)
