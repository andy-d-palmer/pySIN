from pyspark import SparkContext, SparkConf

import numpy as np
import sys
import bisect
import os
import json
import datetime

from array import array

def txt_to_spectrum(s):
    arr = s.strip().split("|")
    return ( arr[0], np.array([ float(x) for x in arr[2].split(" ") ]), np.array([ float(x) for x in arr[1].split(" ") ]) )

def seq_to_spectrum(x):
    arr = x[1].strip().split("|")
    return ( x[0], np.array([ float(x) for x in arr[1].split(" ") ]), np.array([ float(x) for x in arr[0].split(" ") ]) )

def get_one_group_total(mz_lower, mz_upper, mzs, intensities):
    return np.sum(intensities[ bisect.bisect_left(mzs, mz_lower) : bisect.bisect_right(mzs, mz_upper) ])

def get_many_groups_total(q, sp):
    return [(i, sp[0], get_one_group_total(q[i][0], q[i][1], sp[1], sp[2])) for i in xrange(len(queries))]

def get_many_groups_total_txt(q, sp):
    return ["%s:%.6f" % (sp[0], get_one_group_total(q[0], q[1], sp[1], sp[2])) for q in queries]

def txtquery_to_mzvalues(line):
    arr = line.strip().split(',')
    (mz, tol) = ( float(arr[0]), float(arr[1]) )
    return (mz - tol, mz + tol)

conf = SparkConf() #.setAppName("Extracting m/z images").setMaster("local") #.set("spark.executor.memory", "16g").set("spark.driver.memory", "8g")
sc = SparkContext(conf=conf)

queries = sc.textFile("s3n://sin-test-s3bucket/peaklist.csv").map(txtquery_to_mzvalues).collect()
qBr = sc.broadcast(queries)

ff = sc.textFile("s3n://sin-test-s3bucket/Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.txt")
spectra = ff.map(txt_to_spectrum)
# spectra.cache()

qres = spectra.map(lambda sp : get_many_groups_total_txt(qBr.value, sp)).reduce(lambda x, y: [ x[i] + " " + y[i] for i in xrange(len(x))])

with open("/spark.res.txt", "w") as f:
    for q in qres:
        f.write(q + "\n")

sc.stop()
exit(0)

