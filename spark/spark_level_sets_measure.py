from pyspark import SparkContext, SparkConf

import numpy as np
import scipy.ndimage
import scipy.ndimage.morphology
from operator import attrgetter,itemgetter
import sys
import bisect
import os
import json
import datetime
from array import array

datadir = '/media/data/ims'

num_levels = 20

def txt_to_spectrum(s):
    arr = s.strip().split("|")
    return ( arr[0], np.array([ float(x) for x in arr[2].split(" ") ]), np.array([ float(x) for x in arr[1].split(" ") ]) )

def get_one_group_total(mz_lower, mz_upper, mzs, intensities):
    return np.sum(intensities[ bisect.bisect_left(mzs, mz_lower) : bisect.bisect_right(mzs, mz_upper) ])

def get_many_groups_total(q, sp):
    return [(i, sp[0], get_one_group_total(q[i][0], q[i][1], sp[1], sp[2])) for i in xrange(len(queries))]

def get_many_groups_total_forimage(q, sp):
    return (int(sp[0]), [get_one_group_total(q[i][0], q[i][1], sp[1], sp[2]) for i in xrange(len(queries))])

def get_many_groups_total_txt(q, sp):
    return ["%s:%.6f" % (sp[0], get_one_group_total(q[0], q[1], sp[1], sp[2])) for q in queries]

def txtquery_to_mzvalues(line):
    arr = line.strip().split(',')
    (mz, tol) = ( float(arr[0]), float(arr[1]) )
    return (mz - tol, mz + tol)

import scipy.interpolate as interpolate

def clean_image(im):
    # Image properties
    notnull=im>0
    im_size=np.shape(im)
    if np.all(notnull == False):
        return im
    # print im_size
    # hot spot removal (quantile threshold)
    im_q = np.percentile(im[notnull],99)
    im[im>im_q] = im_q

    # # interpolate to replace missing data - not always present
    # X,Y=np.meshgrid(np.arange(0,im_size[1]),np.arange(0,im_size[0]))
    # f=interpolate.interp2d(X[notnull],Y[notnull],im[notnull], kind='linear', copy=False)
    # im=f(np.arange(0,im_size[1]),np.arange(0,im_size[0]))

    return im


## global hardcoded morphology masks
dilate_mask = [[0,1,0],[1,1,1],[0,1,0]]
erode_mask = [[1,1,1],[1,1,1],[1,1,1]]
label_mask = np.ones((3,3))

import scipy.ndimage as ndimage
import scipy.ndimage.morphology as morphology


def measure_of_chaos(img,nlevels): 
    ''' Function for calculating a measure of image noise using level sets
    # Inputs: image - numpy array of pixel intensities
             nlevels - number of levels to calculate over (note that we approximating a continuious distribution with a 'large enough' number of levels)
    # Outputs: measure_value
     
    This function calculates the number of connected regions above a threshold at an evenly spaced number of threshold
    between the min and max values of the image.

    There is some morphological operations to deal with orphaned pixels and heuristic parameters in the image processing.

    # Usage 
    img = misc.imread('/Users/palmer/Copy/ion_image.png').astype(float)
    print measure_of_chaos(img,20)
    '''
    # Image in/preparation
    sum_notnull = np.sum(img > 0)
    if sum_notnull == 0:
        return 0
    im=clean_image(img)

    return float(np.sum([
        ndimage.label(
            morphology.binary_erosion(
                morphology.binary_dilation(im > lev,structure=dilate_mask)
            , structure=erode_mask)
        )[1] for lev in np.linspace(np.amin(im),np.amax(im),nlevels)]))/(sum_notnull*nlevels)


conf = SparkConf() #.setAppName("Extracting m/z images").setMaster("local") #.set("spark.executor.memory", "16g").set("spark.driver.memory", "8g")
sc = SparkContext(conf=conf)

ff = sc.textFile(datadir + "/Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.txt")
spectra = ff.map(txt_to_spectrum)
spectra.cache()

## read queries from file
# queries = sc.textFile("/media/data/ims/peaklist.csv").map(txtquery_to_mzvalues).collect()
queries = sc.textFile(datadir + "/peaklist.csv").map(txtquery_to_mzvalues).collect()
qBr = sc.broadcast(queries)
qres = sorted( spectra.map(lambda sp : get_many_groups_total_forimage(qBr.value, sp)).collect(), key=itemgetter(0) )

## read pixel indices from file
file_pixels = sc.textFile(datadir + '/pixels.txt').collect()
(nColumns, nRows) = (int(file_pixels[0].split(' ')[0]), int(file_pixels[0].split(' ')[1]))
pix_ind = [ int(s.strip()) for s in file_pixels[1:]]

def create_image(qres, i):
    img = np.zeros((nRows*nColumns,1))
    for n in xrange(len(qres)):
        img[pix_ind[n]] = qres[n][1][i]
    return np.reshape(img,(nRows,nColumns))

images = sc.parallelize( [ create_image(qres,i) for i in xrange(len(queries)) ] )
chaos_measures = images.map( lambda img : measure_of_chaos(img, num_levels) ).collect()

with open(datadir + '/res.chaosmeasures.noint.txt', "w") as outfile:
    for res in chaos_measures:
        outfile.write("%.6f\n" % res)



# with open('coordinates.txt', 'w') as outfile:
#     outfile.write("\n".join([" ".join([str(x) for x in coords[i,:]]) for i in xrange(len(index_list)) ]))

# bounding_box = [ [np.amin(coords[:,ii]), np.amax(coords[:,ii])] for ii in xrange(3) ]
# pixel_indices = np.zeros(len(coords))
# _coord = np.asarray(coords) 
# _coord = np.around(_coord,5) # correct for numerical precision   
# _coord = _coord - np.amin(_coord,axis=0)
# # calculate step size in xyz
# step = np.zeros((3,1))
# for ii in range(0,3):
#     step[ii] = np.mean(np.diff(np.unique(_coord[:,ii])))  
# _coord = (_coord.T / step).T
# _coord_max = np.amax(_coord,axis=0)
# nColumns = _coord_max[1]+1
# nRows = _coord_max[0]+1
# for c in range(0,len(_coord)):
#     pixel_indices[c] = _coord[c,0]*(nColumns) + _coord[c,1]
# with open('/media/data/ims/pixels.txt', 'w') as outfile:
#     outfile.write("%d %d\n" % (nRows, nColumns))
#     outfile.write("\n".join([("%d" % x) for x in pixel_indices]))


