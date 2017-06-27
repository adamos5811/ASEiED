from __future__ import print_function
import sys
import numpy as np
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession

def parseVector(line):
    return np.array([int(x) for x in line.split(',')])
def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = np.sum((p - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex


if __name__ == "__main__":
    #deklaracja zmiennych
    F = "data.txt" #plik z danymi
    K = 2  #ilosc klastrow

    #program
    print("hardly working...")

    spark = SparkSession\
        .builder\
        .appName("PythonKMeans")\
        .getOrCreate()
    lines = spark.read.text(F).rdd.map(lambda r: r[0])
    data = lines.map(parseVector).cache()
    kPoints = data.takeSample(False, K, 1)
    tempDist = 1.0
    while tempDist > 0.001:
        closest = data.map(
            lambda p: (closestPoint(p, kPoints), (p, 1)))
        pointStats = closest.reduceByKey(
            lambda p1_c1, p2_c2: (p1_c1[0] + p2_c2[0], p1_c1[1] + p2_c2[1]))
        newPoints = pointStats.map(
            lambda st: (st[0], st[1][0] / st[1][1])).collect()
        tempDist = sum(np.sum((kPoints[iK] - p) ** 2) for (iK, p) in newPoints)
        for (iK, p) in newPoints:
            kPoints[iK] = p

    print("Final centers: " + str(kPoints))
    spark.stop()

    dataset = []
    f = open("data.txt")
    lines = f.readlines()
    for line in lines:
        point = line.split(",")
        dataset.append([int(point[0]), int(point[1])])

    x, y = zip(*dataset)
    plt.scatter(x, y)
    x,y = zip(*kPoints)
    plt.plot(x,y,'ro')
    plt.show()