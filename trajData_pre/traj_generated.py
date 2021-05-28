import pgsql
import csv
import os
import math
from time import time
import utils
import json


def get_mmsi(tablename):
    sql = "select mmsi,count(*) as num from " + tablename + " group by mmsi having count(*)>=20 order by num desc"
    print(sql)
    mmsi_arr = []
    rows = pgsql.query(sql).get('rows')
    print(rows[885])
    print(rows[884])
    print(rows[886])
    for item in rows:
        mmsi_arr.append(item[0])
    print(len(mmsi_arr))
    return mmsi_arr


def get_traj(tablename, mmsi):
    sql = "select mmsi,extract(epoch FROM basedatetime::timestamp without time zone),lat,lon,sog,cog,heading,vesselname,imo,callsign,vesseltype,status,length,width,draft,cargo from " + tablename + " where mmsi='" + str(mmsi) + "' order by basedatetime"
    # print(sql)
    result = pgsql.query(sql).get('rows')
    return result


def file_generated(file_name, data):
    headers = ['mmsi', 'basedatetime', 'lat', 'lon', 'sog', 'cog', 'heading', 'vesselname', 'imo', 'callsign', 'vesseltype', 'status', 'length', 'width', 'draft', 'cargo']
    # traj_fields_output_file.writerow(headers)
    flag = 0
    total_num = 0
    traj_fields_rows = []
    for item in data:
        traj_fields_rows.append([
            item[0],
            math.floor(item[1]),
            item[2],
            item[3],
            item[4],
            item[5],
            item[6],
            item[7],
            item[8],
            item[9],
            item[10],
            item[11],
            item[12],
            item[13],
            item[14],
            item[15]
        ])
        total_num += 1
        if float(item[4]) < 0.5:
            flag += 1
    if (flag / total_num) <= 0.9:
        traj_fields_output_file = csv.writer(
            open(
                file_name,
                'w'),
            delimiter=',')

        traj_fields_output_file.writerows(traj_fields_rows)
        return total_num
    else:
        return 0


def final_files(tablename, path):
    mmsi_arr = get_mmsi(tablename)
    total_num = 0
    zerospeed = 0
    for chunk_num, oid in enumerate(mmsi_arr):
        file_name = path + tablename + '_' + str(oid) + '.csv'
        # print(file_name)
        tmp = file_generated(file_name, get_traj(tablename, oid))
        if tmp == 0:
            zerospeed += 1
        else:
            total_num += tmp
    print(zerospeed)
    return total_num


statistics_rows = []


def readData(file_name):
    csvFile = open(file_name, "r")
    reader = csv.reader(csvFile)

    curr = 0
    coors = []
    tid = ''
    oid = ''
    for item in reader:
        if reader.line_num == 1:
            oid = item[0]
            curr = math.floor(int(item[1]) / 86400)
            tid = item[0] + '_' + item[1]

        binNum = math.floor(int(item[1]) / 86400)
        if binNum != curr:
            statistics_rows.append([
                item[0],
                tid,
                len(coors),
                utils.calcLen(coors),
                coors[-1][2] - coors[0][2],
                json.dumps(utils.getMBR(coors))
            ])

            coors.clear()
            curr = binNum
            tid = item[0] + '_' + item[1]

        coors.append([float(item[3]), float(item[2]), int(item[1])])

    if len(coors) > 0:
        statistics_rows.append([
            oid,
            tid,
            len(coors),
            utils.calcLen(coors),
            coors[-1][2] - coors[0][2],
            json.dumps(utils.getMBR(coors))
        ])


def statistics_writer(dir, target):
    files = os.listdir(dir)
    print(len(files))
    for item in files:
        # print(dir + item)
        readData(dir + item)
    utils.csvWriter(target, statistics_rows)


def statistics_printer(file_name):
    csvFile = open(file_name, "r")
    reader = csv.reader(csvFile)

    distance = [0, 0, 0, 0, 0]
    time = [0, 0, 0, 0, 0]

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

            if float(item[3]) < 20:
                distance[0] += 1
            elif 20 <= float(item[3]) < 50:
                distance[1] += 1
            elif 50 <= float(item[3]) < 100:
                distance[2] += 1
            elif 100 <= float(item[3]) < 200:
                distance[3] += 1
            elif 200 <= float(item[3]):
                distance[4] += 1

            if int(item[4]) < 3600:
                time[0] += 1
            elif 3600 <= int(item[4]) < 3600 * 6:
                time[1] += 1
            elif 3600 * 6 <= int(item[4]) < 3600 * 12:
                time[2] += 1
            elif 3600 * 12 <= int(item[4]) < 3600 * 24:
                time[3] += 1
            elif 3600 * 24 <= int(item[4]):
                time[4] += 1
        else:
            print(item)

    length.sort()
    point.sort()
    print(distance)
    print(time)
    print(len(obj), traj_num, point_num, length[0], length[-1], total_len / len(length))
    print(length[-2], length[-3], point[0], point[-1])
    print(distance[0] / traj_num, distance[1] / traj_num, distance[2] / traj_num, distance[3] / traj_num, distance[4] / traj_num)
    print(time[0] / traj_num, time[1] / traj_num, time[2] / traj_num, time[3] / traj_num, time[4] / traj_num)


if __name__ == '__main__':
    # 369970966
    # print(pgsql.query("SELECT extract(epoch FROM '2017-02-07 11:18:23.098'::timestamp without time zone)"))
    start_time = time()
    # get_mmsi("v50")
    # num = final_files("v25", "/Users/tianwei/Projects/data/ais_v25/")
    # print(num)
    # statistics_writer('/Users/tianwei/Projects/data/ais_v25/', './data/AISv25_statistics.csv')
    statistics_printer('./data/AISv25_statistics.csv')
    print('{:.2f}s'.format(time() - start_time))
