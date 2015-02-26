from __future__ import print_function

import numpy as np
import sys
import bisect
import os
import json
import h5py as h5
import datetime
from array import array

# fname = '/media/data/ims/Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.txt'
# out_fname = '/media/data/ims/Ctrl3s2.h5'

if len(sys.argv) < 3:
    print("Usage: python convert_txt_to_bin.py input.txt output.h5")
    exit(0)

def txt_to_spectrum(s):
    arr = s.strip().split("|")
    return ( arr[0], np.array([ float(x) for x in arr[2].split(" ") ]), np.array([ float(x) for x in arr[1].split(" ") ]) )

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
    
def my_print(s):
    print("[" + str(datetime.datetime.now()) + "] " + s, file=sys.stderr)


my_print("Reading %s and writing to %s..." % ( sys.argv[1], sys.argv[2]) )
f = h5.File(sys.argv[2], 'w')
f.create_group('sp')
with open(sys.argv[1]) as infile:
    for line in infile:
        s = txt_to_spectrum(line)
        g = f.create_group(s[0])
        g.create_dataset('mz', dtype='f', data=s[1])
        g.create_dataset('sp', dtype='f', data=s[2])
f.close()
my_print("All done!")
exit(0)

