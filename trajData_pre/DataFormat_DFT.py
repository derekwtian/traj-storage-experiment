#!/usr/bin/python3
# -*- coding: utf-8 -*-

import csv
import utils
from time import time
import os
import sys


def data_tranform(file_name):
    csvFile = open(file_name, "r")
    reader = csv.reader(csvFile)

    data_rows = []
    for item in reader:
        data_rows.append([
            item[0],
            int(item[1]),
            float(item[2]),
            float(item[3])
        ])

    result = []
    threshold = 1.852 / 2
    zeroref = 1e-16
    tid = ''
    flag = False
    times = 0
    for index in range(len(data_rows) - 1):
        duration = data_rows[index + 1][1] - data_rows[index][1]
        if duration != 0:
            curr_tid = data_rows[index][0] + '>_<' + str(data_rows[index][1])
            speed = utils.HaversineDistance(data_rows[index][3], data_rows[index][2], data_rows[index+1][3], data_rows[index+1][2]) / (duration / 3600)
            if flag is False and speed >= threshold:
                flag = True
                tid = curr_tid
            elif flag is True and speed < zeroref:
                flag = False
                times = 0
            elif flag is True and zeroref <= speed < threshold:
                times += 1
            if 3 < times:
                flag = False
                times = 0

            if flag is True:
                result.append([
                    data_rows[index][0],
                    tid,
                    data_rows[index][3],
                    data_rows[index][2],
                    data_rows[index][1],
                    data_rows[index + 1][3],
                    data_rows[index + 1][2],
                    data_rows[index + 1][1]
                ])

    # print(len(data_rows), len(result))
    # utils.csvWriter('./data/dft.csv', result)
    return result


def data_processing(dir, target):
    files = os.listdir(dir)
    print(len(files))
    total = 0
    for item in files:
        data = data_tranform(dir + item)
        utils.csvWriter(target + item, data)
        total += 1
    print(total)


def mapTID(file_name, target_file):
    csvFile = open(file_name, "r")
    reader = csv.reader(csvFile)

    fields_output_file = csv.writer(
        open(
            target_file,
            'a'),
        delimiter=',')

    tid = ""
    oid = ""
    tnum = 0
    onum = 0
    sid = 0
    osid = 0
    obj = set()
    traj = set()
    for item in reader:
        obj.add(item[0])
        traj.add(item[1])
        if item[0] != oid:
            onum += 1
            oid = item[0]
            osid = 0
        if item[1] != tid:
            tnum += 1
            tid = item[1]
            sid = 0
        sid += 1
        osid += 1
        fields_output_file.writerow([item[1], onum, osid, tnum, sid, item[2], item[3], item[4], item[5], item[6], item[7]])

    print(len(obj), len(traj))
    print(onum, tnum)


def transform_beijing(file_name):
    reader = csv.reader(open(file_name, "r"), delimiter=';')

    serial = file_name.split(".")[0].split("_")[2]

    dita_output_file = csv.writer(
        open("./dita_chengdu_" + serial + ".txt", 'a'),
        delimiter=';'
    )

    dft_output_file = csv.writer(
        open("./dft_chengdu_" + serial + ".csv", 'a'),
        delimiter=','
    )

    row_num = 0
    for traj in reader:
        if len(traj) > 0:
            dita_row = []
            for point in traj:
                tmp = point.split(",")
                dita_row.append(tmp[3] + ',' + tmp[2])
            dita_output_file.writerow(dita_row)

            tmp0 = traj[0].split(",")
            tid = tmp0[0] + ">_<" + tmp0[1]
            sid = 0
            for index in range(len(traj) - 1):
                point1 = traj[index].split(",")
                point2 = traj[index+1].split(",")
                dft_output_file.writerow([tid, row_num, sid, point1[2], point1[3], point1[1], point2[2], point2[3], point2[1]])
                sid += 1
            print(row_num, len(traj))
        else:
            print(row_num, "null")
        row_num += 1


def query_generation(file_name):
    reader = csv.reader(open(file_name, "r"))
    tids = [
        "20000288>_<1396346501",
        "20000189>_<1397490604",
        "20000684>_<1397865183",
        "20000108>_<1398702809",
        "20000681>_<1393499660",
        "20000398>_<1393725653",
        "20000370>_<1392838950",
        "20000229>_<1392995308",
        "20000610>_<1392410130",
        "20000502>_<1393405206"
    ]
    queries = []
    count = set()
    for item in reader:
        if item[0] in tids:
            count.add(item[0])
            queries.append(item)

    print(len(count))
    print(count)
    print(set(tids) - count)
    utils.csvWriter("./queries_dft_porto.csv", queries)


if __name__ == '__main__':
    start_time = time()
    # data_tranform('/Users/tianwei/Projects/data/ais_v25/v25_270995.csv')
    # data_processing('/Users/tianwei/Projects/data/ais_v100/', '/Users/tianwei/Projects/data/DFT_format/dft100/')
    # mapTID('/Users/tianwei/Projects/data/DFT_format/query2/query.csv', '/Users/tianwei/Projects/data/DFT_format/query2/queries.csv')
    # transform_beijing("/Users/tianwei/Projects/data/DFT_format/beijing.csv")
    # transform_beijing(sys.argv[1])
    query_generation("/Users/tianwei/Projects/data/DFT_format/dft_porto.csv")
    print('{:.2f}s'.format(time() - start_time))
