# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import csv


# 读取csv至字典
csvFile = open("./data/result.csv", "r")
reader = csv.reader(csvFile)

# 建立空字典
x = []
y = []
for item in reader:
    # 忽略第一行
    # if reader.line_num == 1:
    #     continue
    x.append(item[0])
    y.append(item[1])
csvFile.close()

plt.bar(x, y)
plt.show()
