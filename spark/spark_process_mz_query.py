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

## this reads a regular text file
ff = sc.textFile("/Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.txt")
## gzipped file
# ff = sc.textFile("/media/data/ims/Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.txt.gz")
## and this is Hadoop HDFS
# ff = sc.textFile("hdfs://localhost:9000/user/snikolenko/Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.txt")
spectra = ff.map(txt_to_spectrum)

def txtfile_to_array(fname, itemtype='d'):
    sp_array = array(itemtype)
    sp_meta = {
        "itemsize" : sp_array.itemsize,
        "spectra" : []
    }
    with open(fname) as infile:
        for line in infile:
            s = txt_to_spectrum(line)
            sp_meta["spectra"].append( { "name" : s[0], "len" : len(s[1]) } )
            sp_array.extend(s[1])
            sp_array.extend(s[2])
    return (sp_meta, sp_array)

def save_array(dirname, sp_meta, sp_array):
    if not os.path.exists(dirname):
        os.mkdir(dirname)
    with open(dirname + os.sep + "meta.json", "w") as outfile:
        json.dump(sp_meta, outfile)
    with open(dirname + os.sep + "data.bin", "w") as outfile:
        sp_array.tofile(outfile)

def read_array(dirname):
    with open(dirname + os.sep + "meta.json") as infile:
        sp_meta = json.load(infile)
    total = np.sum([ x["len"] for x in sp_meta["spectra"] ])
    if sp_meta['itemsize'] == 4:
        sp_array = array('f')
    else:
        sp_array = array('d')
    with open(dirname + os.sep + "data.bin") as infile:
         sp_array.fromfile(infile, total)
    return sp_meta, sp_array
    

# (sp_meta, sp_data) = txtfile_to_array("/media/data/ims/Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.txt", 'd')
# save_array("Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.Spark", sp_meta, sp_data)


# (sp_meta_float, sp_data_float) = txtfile_to_array("/media/data/ims/Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.txt", 'f')
# save_array("Ctrl3s2.float", sp_meta_float, sp_data_float)

# (sp_meta2, sp_data2) = read_array("Ctrl3s2.float")


# dirname = "/media/data/ims/Ctrl3s2.float"
# sp_meta = json.loads( "\n".join(sc.textFile(dirname + os.sep + "meta.json").collect()) )

# sp_data = 

## this is a spark sequence file
# ff = sc.sequenceFile("/media/data/ims/Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.Spark")
# spectra = ff.map(seq_to_spectrum)

spectra.cache()

# queries = sc.textFile("/media/data/ims/peak_list.csv").map(txtquery_to_mzvalues).collect()
queries = sc.textFile("/peaklist.csv").map(txtquery_to_mzvalues).collect()
qBr = sc.broadcast(queries)
qres = spectra.map(lambda sp : get_many_groups_total_txt(qBr.value, sp)).reduce(lambda x, y: [ x[i] + " " + y[i] for i in xrange(len(x))])

# qres.saveAsTextFile("/result.csv")

with open("/root/spark.res.txt", "w") as f:
    for q in qres:
        f.write(q + "\n")

sc.stop()
exit(0)

