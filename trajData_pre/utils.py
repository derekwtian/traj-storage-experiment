#!/usr/bin/python3
# -*- coding: utf-8 -*-

import math
from time import time
from datetime import datetime, timezone
import csv
import os


def HaversineDistance(lon1, lat1, lon2, lat2):
    # returns the distance in km
    REarth = 6371
    lat = abs(lat1 - lat2) * math.pi / 180
    lon = abs(lon1 - lon2) * math.pi / 180
    lat1 = lat1 * math.pi / 180
    lat2 = lat2 * math.pi / 180
    a = math.sin(lat / 2) * math.sin(lat / 2) + math.cos(lat1) * math.cos(lat2) * math.sin(lon / 2) * math.sin(lon / 2)
    d = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = REarth * d

    return d


def calcLen(coors):
    total_len = 0
    for index in range(len(coors) - 1):
        total_len += HaversineDistance(coors[index][0], coors[index][1], coors[index+1][0], coors[index+1][1])

    return total_len


def getMBR(coors):
    minlon = coors[0][0]
    minlat = coors[0][1]
    maxlon = coors[0][0]
    maxlat = coors[0][1]
    for item in coors:
        if item[0] < minlon:
            minlon = item[0]
        if item[1] < minlat:
            minlat = item[1]
        if item[0] > maxlon:
            maxlon = item[0]
        if item[1] > maxlat:
            maxlat = item[1]

    return [[minlon, minlat], [maxlon, maxlat]]


def format2stamp(dt="2016-05-05 20:28:54"):
    # 转换成时间数组
    timeArray = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    # 转换成时间戳
    # timestamp = time.mktime(timeArray)

    return int(timeArray.timestamp())


def csvWriter(file_name, data, headers=[]):
    fields_output_file = csv.writer(
        open(
            file_name,
            'w'),
        delimiter=',')
    if len(headers) > 0:
        fields_output_file.writerow(headers)
    fields_output_file.writerows(data)


def getAllFiles(dir):
    path = os.listdir(dir)
    print(len(path))
    result = []
    for p in path:
        if os.path.isdir(p):
            result.append(p)
    return result


if __name__ == '__main__':
    coors = [[-8.618643,41.141412],[-8.618499,41.141376],[-8.620326,41.14251],[-8.622153,41.143815],[-8.623953,41.144373],[-8.62668,41.144778],[-8.627373,41.144697],[-8.630226,41.14521],[-8.632746,41.14692],[-8.631738,41.148225],[-8.629938,41.150385],[-8.62911,41.151213],[-8.629128,41.15124],[-8.628786,41.152203],[-8.628687,41.152374],[-8.628759,41.152518],[-8.630838,41.15268],[-8.632323,41.153022],[-8.631144,41.154489],[-8.630829,41.154507],[-8.630829,41.154516],[-8.630829,41.154498],[-8.630838,41.154489]]
    start_time = time()
    print(calcLen(coors))
    print(HaversineDistance(-76.32936, 36.94985, -76.32922, 36.94979) / ((941-793) / 3600))
    print(HaversineDistance(-76.32958, 36.94978, -76.33, 36.94964) / ((373-261) / 3600))
    print(getMBR(coors))
    print(format2stamp('2008-02-08 00:04:51'))
    print(getAllFiles("/Users/tianwei/Projects/data/ais_v25"))
    print('{:.2f}s'.format(time() - start_time))
