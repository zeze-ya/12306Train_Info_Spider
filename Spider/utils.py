from Spider.config import STATION_DICT
from datetime import date, timedelta

def get_value_by_key(key, _dict=STATION_DICT):
    """通过字典值获取字典名"""
    for k, v in _dict.items():
        if v == key:
            return k
def get_station_code(key):
    """
    模糊匹配车站名，返回车站编码
    :param key:
    :return:列车编码表
    """
    code_list = []
    for k, v in STATION_DICT.items():
        if key in k:
            code_list.append(v)
    return code_list

def generate_url_value(_dict, from_station, to_station, data=''):
    """生成12306接口所需的查询参数"""
    to_station_code = _dict.get(to_station)
    from_station_code = _dict.get(from_station)
    tomorrow = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    infos = {
        'from_station': from_station_code,
        'to_station': to_station_code,
        'data': tomorrow
    }
    return infos

def generate_tomorrow_date():
    tomorrow = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    return tomorrow

def generate_all_url_value():
    """
    生成查询from_stations和to_stations之间所有的车站间的车次
    返回值为列表嵌套字典
    [{'data':出发日期，'from_station':出发车站，'to_station'到达车站}]
    示例：[{'data': '2020-01-20','from_station': 'CUW','to_station': 'CQW'},
    {'data': '2020-01-20','from_station: 'CUW',to_station: 'CXW'}]
    """
    tomorrow = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    infos_list = []

    for from_station in FROM_STATION.values():
        for to_station in TO_STATION.values():
            if from_station != to_station:
                info = {
                    'from_station': from_station,
                    'to_station': to_station,
                    'date': tomorrow
                }
                infos_list.append(info)

    return infos_list


FROM_STATION = {'上海': 'SHH', '上海南': 'SNH', '上海虹桥': 'AOH', '上海西': 'SXH'}
TO_STATION = STATION_DICT

if __name__ == '__main__':
    print(get_station_code('上海'))
