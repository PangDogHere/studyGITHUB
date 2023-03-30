import asyncio
import math
import pandas as pd
import requests
from datetime import datetime
from tqdm.asyncio import tqdm_asyncio
import pyproj
from aiohttp import ClientSession

## 转换函数：
x_pi = 3.14159265358979324 * 3000.0 / 180.0
pi = 3.1415926535897932384626  # π
a = 6378245.0  # 长半轴
ee = 0.00669342162296594323  # 扁率
 # 定义多个key
keys = [
        # 'b4ac68387fd27b0a15a737a109bec4d2',
        # '019f43faf2dac37bcdc95c7d50765637',
        # '7a6b156af01f5806f78a586cba195682',
        'be674ee61c9340f431737521493cd1f0',
        'c239a7342fd76b49c2044e4890a54927',
        '9606891cea6cc58ccd76e02aa998f2fb'
        # '93d56449e802f3888256cc3d1210adff'
        ]
key_index = 0  # 当前使用的key索引
def gcj02towgs84(lng, lat):
    """
    GCJ02(火星坐标系)转GPS84
    :param lng:火星坐标系的经度
    :param lat:火星坐标系纬度
    :return:
    """
    if out_of_china(lng, lat):
        return lng, lat
    dlat = transformlat(lng - 105.0, lat - 35.0)
    dlng = transformlng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * pi
    magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
    mglat = lat + dlat
    mglng = lng + dlng
    return [lng * 2 - mglng, lat * 2 - mglat]
def transformlat(lng, lat):
    ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + \
        0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
            math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * pi) + 40.0 *
            math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * pi) + 320 *
            math.sin(lat * pi / 30.0)) * 2.0 / 3.0
    return ret
def transformlng(lng, lat):
    ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
        0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
            math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * pi) + 40.0 *
            math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * pi) + 300.0 *
            math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
    return ret
def out_of_china(lng, lat):
    """
    判断是否在国内，不在国内不做偏移
    :param lng:
    :param lat:
    :return:
    """
    if lng < 72.004 or lng > 137.8347:
        return True
    if lat < 0.8293 or lat > 55.8271:
        return True
    return False

# 读取Excel文件中的数据
data_df = pd.read_excel('data.xlsx')
# 定义异步函数，通过小区名称和地址获取经纬度信息
async def get_location(name, address, session, sem):
    global key_index
    async with sem:  # 获得信号量
        # 从keys中获取当前使用的key
        key = keys[key_index]
        # url = 'https://restapi.amap.com/v3/place/text?key=' + key + '&keywords=' + '{}|{}'.format(name,address) + '&city=511300'
        url = 'https://restapi.amap.com/v3/geocode/geo?key=' + key + '&address={}{}'.format(address,name)  + '&city=511300'
        async with session.get(url) as response:
            data_dict = await response.json()
            # 解析返回的json数据，获取经纬度信息
            if data_dict['status'] == '1':
                geocodes = data_dict['geocodes']
                if len(geocodes) > 0:
                    location = geocodes[0]['location']
                    formatted_address=data_dict['geocodes'][0]['formatted_address']
                    level=data_dict['geocodes'][0]['level']
                    lng, lat = location.split(',')
                    # 将GCJ-02坐标系转换为WGS-84坐标系
                    wgs84_lng, wgs84_lat = gcj02towgs84(float(lng), float(lat))
                    # return {'name': name, 'address': address,'gcj02_lng':lng,'gcj02_lat':lat, 'wgs84_lng': wgs84_lng, 'wgs84_lat': wgs84_lat,'amap_name':amap_name,'amap_adname':amap_adname,'amap_address':amap_address}
                    return {'name': name, 'address': address,'gcj02_lng':lng,'gcj02_lat':lat, 'wgs84_lng': wgs84_lng, 'wgs84_lat': wgs84_lat,'formatted_address':formatted_address,'level':level}
            else:
                # 判断是否访问次数超限，如果超限，切换到下一个key执行
                info = data_dict['info']
                if info == 'ACCESS_TOO_FREQUENT' :
                    print(info + "什么超限")
                if info == 'CUQPS_HAS_EXCEEDED_THE_LIMIT':
                    print('并发超限：{}{}'.format(name,address))
                    return await get_location(name, address, session, sem)  # 递归获取经纬度信息
                if info == 'DAILY_QUERY_OVER_LIMIT' or info == 'USER_DAILY_QUERY_OVER_LIMIT':
                    key_index += 1
                    if key_index >= len(keys):
                        # 所有key都用完了，打印当前执行的行号，并提前结束循环
                        print('所有key都用完了，打印当前执行的行号，并提前结束循环：', key_index)
                        return None
                    else:
                        return await get_location(name, address, session, sem)  # 递归获取经纬度信息
                print("错误信息：{} 错误ID：{} name:{} address:{}\n".format(data_dict['info'],data_dict['infocode'],name,address))
            return None
# 定义异步函数，用于并发获取所有小区的经纬度信息
async def get_all_locations(data_df):
    tasks = []  # 存放所有异步任务
    sem = asyncio.Semaphore(5)  # 设置并发请求数的上限为30
    async with ClientSession() as session:
        for index, row in data_df.iterrows():
            name = row['name']
            address = row['address']
            task = asyncio.ensure_future(get_location(name, address, session, sem))  # 将异步任务加入列表
            tasks.append(task)
        return await asyncio.gather(*tasks)  # 并发运行所有异步任务，并返回结果
# 运行异步函数，获取所有小区的经纬度信息
loop = asyncio.get_event_loop()
locations = loop.run_until_complete(get_all_locations(data_df))
loop.close()
# 将获取到的经纬度信息保存为新的Excel文件
new_data_df = pd.DataFrame([location for location in locations if location])
date_str = datetime.today().strftime('%Y%m%d%H%M%S')
filename = 'POI_' + date_str + '.xlsx'
new_data_df.to_excel(filename, index=False)
