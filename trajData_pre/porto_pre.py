#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import csv
import json
from time import time
from ast import literal_eval
import utils


def readData(data_file_name, target_file):
    headers = ['TRIP_ID', 'CALL_TYPE', 'ORIGIN_CALL', 'ORIGIN_STAND', 'TAXI_ID', 'TIMESTAMP', 'DAY_TYPE', 'MISSING_DATA', 'POLYLINE']
    chunks = pd.read_csv(
        data_file_name,
        sep=',',
        header=0,
        names=headers,
        error_bad_lines=False,
        chunksize=10000,
    )

    statistics_rows = []

    total_num = 0
    for chunk in chunks:
        for i, x in chunk.iterrows():
            coors = literal_eval(x.POLYLINE)
            if x.MISSING_DATA is False and 6 <= len(coors) <= 130:
                statistics_rows.append([
                    x.TAXI_ID,
                    x.TRIP_ID,
                    len(coors),
                    utils.calcLen(coors),
                    15 * (len(coors) - 1),
                    json.dumps(utils.getMBR(coors))
                ])

                data_reconstruct(x, total_num, target_file)
                total_num += 1

    print(total_num)
    utils.csvWriter('./data/porto_statistics.csv', statistics_rows)


def data_reconstruct(x, num, target_file):
    data_rows = []

    time = x.TIMESTAMP
    tid = str(x.TAXI_ID) + '>_<' + str(x.TIMESTAMP)
    coors = literal_eval(x.POLYLINE)
    for item in coors:
        data_rows.append([
            x.TAXI_ID,
            x.TRIP_ID,
            tid,
            item[0],
            item[1],
            time
        ])
        time += 15

    result = []
    for index in range(len(data_rows) - 1):
        result.append([
            tid,
            num,
            index,
            data_rows[index][3],
            data_rows[index][4],
            data_rows[index][5],
            data_rows[index + 1][3],
            data_rows[index + 1][4],
            data_rows[index + 1][5],
        ])

    fields_output_file = csv.writer(
        open(
            target_file,
            'a'),
        delimiter=',')

    fields_output_file.writerows(result)


def query_generated(file_name, target_file):
    csvFile = open(file_name, "r")
    reader = csv.reader(csvFile)

    tids_old = [
        '20000596>_<1372637303',
        '20000515>_<1372839442',
        '20000263>_<1374443268',
        '20000100>_<1391504564',
        '20000600>_<1391693338'
    ]

    tids = [
        "20000080>_<1373998180",
        "20000467>_<1378748046",
        "20000249>_<1401018049",
        "20000514>_<1385876002",
        "20000156>_<1377338541",
        "20000364>_<1393456152",
        "20000195>_<1396762391",
        "20000067>_<1386081912",
        "20000410>_<1386760305",
        "20000605>_<1395855445",
        "20000476>_<1374636839",
        "20000547>_<1399655694",
        "20000687>_<1400620396",
        "20000443>_<1399277297",
        "20000058>_<1383945552",
        "20000632>_<1391532145",
        "20000527>_<1399538884",
        "20000658>_<1381320536",
        "20000424>_<1395456347",
        "20000398>_<1396997266"
    ]
    queries = []
    for item in reader:
        if item[0] in tids:
            queries.append(item)

    utils.csvWriter(target_file, queries)


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
        # if int(item[2]) >= 5 and (int(item[2]) - 1) * 0.02 <= float(item[3]) <= (int(item[2]) - 1) * 0.5:
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


'''
df_final_rows.sort(reverse=True)
print(df_final_rows)
result = []
dic = dict(Counter(df_final_rows))
for k in dic:
    result.append([k, dic[k]])
print(dict(Counter(df_final_rows)))
'''


if __name__ == '__main__':
    # 369970966
    # print(pgsql.query("SELECT extract(epoch FROM '2017-02-07 11:18:23.098'::timestamp without time zone)"))
    start_time = time()
    # readData('/Users/tianwei/Projects/data/train.csv', '/Users/tianwei/Projects/data/dft_porto.csv')
    query_generated('/Users/tianwei/Projects/data/DFT_format/dft_porto.csv', '/Users/tianwei/Projects/data/DFT_format/queries_dft_porto.csv')
    # statistics_printer('./data/porto_statistics.csv')
    print('{:.2f}s'.format(time() - start_time))
