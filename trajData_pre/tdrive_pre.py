#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import csv
import json
from time import time
from ast import literal_eval
from collections import Counter
import utils
import math


statistics_rows = []


def readData(file_name):
    csvFile = open(file_name, "r")
    reader = csv.reader(csvFile)

    curr = 0
    data_rows = []
    coors = []
    tid = ''
    oid = ''
    for item in reader:
        if reader.line_num == 1:
            oid = item[0]
            curr = math.floor(utils.format2stamp(item[1]) / 86400)
            tid = item[0] + '_' + str(utils.format2stamp(item[1]))

        binNum = math.floor(utils.format2stamp(item[1]) / 86400)
        if binNum != curr:
            utils.csvWriter('./data/tdrive/' + tid + '.csv', data_rows)

            statistics_rows.append([
                item[0],
                tid,
                len(coors),
                utils.calcLen(coors),
                coors[-1][2] - coors[0][2],
                json.dumps(utils.getMBR(coors))
            ])

            data_rows.clear()
            coors.clear()
            curr = binNum
            tid = item[0] + '_' + str(utils.format2stamp(item[1]))

        coors.append([float(item[2]), float(item[3]), utils.format2stamp(item[1])])
        data_rows.append([
            item[0],
            tid,
            utils.format2stamp(item[1]),
            item[2],
            item[3]
        ])

    if len(data_rows) > 0:
        statistics_rows.append([
            oid,
            tid,
            len(coors),
            utils.calcLen(coors),
            coors[-1][2] - coors[0][2],
            json.dumps(utils.getMBR(coors))
        ])
        utils.csvWriter('./data/tdrive/' + tid + '.csv', data_rows)


def statistics_writer(dir):
    for item in range(10357):
        file_name = str(item + 1) + '.txt'
        # print(dir + file_name)
        readData(dir + file_name)
    utils.csvWriter('./data/tdrive_statistics.csv', statistics_rows)


def statistics_printer(file_name):
    csvFile = open(file_name, "r")
    reader = csv.reader(csvFile)

    traj_num = 0
    obj = []
    point = []
    point_num = 0
    length = []
    total_len = 0
    for item in reader:
        if int(item[4]) >= 60 and 5 <= float(item[3]) / (int(item[4]) / 3600) <= 120:
            if item[0] not in obj:
                obj.append(item[0])

            point.append(int(item[2]))
            point_num += int(item[2])

            traj_num += 1
            length.append(float(item[3]))
            total_len += float(item[3])

    length.sort()
    point.sort()
    print(len(obj), traj_num, point_num, length[0], length[-1], total_len / len(length))
    print(length[-2], length[-3], point[0], point[-1])


if __name__ == '__main__':
    # 369970966
    # print(pgsql.query("SELECT extract(epoch FROM '2017-02-07 11:18:23.098'::timestamp without time zone)"))
    start_time = time()
    # readData('./data/1.txt')
    # statistics_writer('/Users/tianwei/Projects/data/t-drive/')
    statistics_printer('./data/tdrive_statistics.csv')
    print('{:.2f}s'.format(time() - start_time))
