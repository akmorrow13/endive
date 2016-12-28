"""

Take in a list of FITS files on S3 and featurize them with different
featurizers.

"""

import matplotlib
matplotlib.use("Agg")
from matplotlib import pylab

import cPickle as pickle
import time
import os
import subprocess

from ruffus import *
import sys
sys.path.append(".")
sys.path.append("../")

import pythonrun
import shutil
from pythonrun import dict_product
import pandas as pd
import featuresolvepipelinerun
import shutil
import boto3

import exputil
import util
import jsocio
import dbconfig
import numpy as np
import glob
from ruffus import * 
import sklearn.metrics
import platform

np.random.seed(0)

RUNNING_ON = 'ec2'

OUTPUT_DATASET_HDFS_ROOT = "s3n://jonas-solar-sdo/"
KEYSTONE_MEM = '50g'
S3_CSV_LOCATION = "timeseries_datasets/"
CSV_LOCAL_DIR = os.path.join(dbconfig.LOCAL_S3_MIRROR_BASE, "timeseries_datasets")
BASE_KEY = S3_CSV_LOCATION
USE_YARN=False
LOCAL_EXP_ROOT = "/data/jonas/suncast/experiments" 

if platform.node() == 'c65':
    RUNNING_ON = 'local'
    #LOCAL_EXP_ROOT = "/home/eecs/jonas/suncast/data" 
    KEYSTONE_MEM = '100g'

elif platform.node() == 'amp-spark-master':
    RUNNING_ON = 'cluster'
    OUTPUT_DATASET_HDFS_ROOT = "/user/jonas/suncast/"
    CSV_LOCAL_DIR="/home/eecs/jonas/suncast/data.s3/timeseries_datasets"
    LOCAL_EXP_ROOT = "/home/eecs/jonas/suncast/experiments" 
    USE_YARN = True

FITS_CONFIG = {

    'data.test' :  
    { 
        'numParts' : 4096,
        's3bucket' : "jonas-solar-sdo",
        'max_keys' : 10, 
        'sourceKeyBase' : "hyperspectral.fits/timeseries.hyperspectral.alldata.hmi_512_256.fits", 
        'outKeyPrefix' : "output/timeseries.hyperspectral.alldata.hmi_512_256.fits/"
        
    }
}



def get_keylist_params():
    for config_name, config in FITS_CONFIG.iteritems():
        out_pickle = os.path.join(LOCAL_EXP_ROOT, "{}.pickle".format(config_name))
        yield None, out_pickle, config_name, config

@files(get_keylist_params)
def create_keylist(infile, out_pickle, config_name, config):
    s3 = boto3.resource('s3')
        
    b = s3.Bucket('jonas-solar-sdo')
    keylist = []
    max_keys = config.get('max_keys', 0)
    for k in b.objects.filter(Prefix=config['sourceKeyBase']):
        # get filename

        l = len(config['sourceKeyBase'])
        filename = k.key[l+1:]
        outkey = os.path.join(config['outKeyPrefix'], filename + ".csv")
        keylist.append((k.key, outkey))

        if max_keys > 0 and len(keylist) == max_keys:
            break
    pickle.dump({'keylist' : keylist, 
                 'config_name' : config_name, 
                 'config' : config}, 
                open(out_pickle, 'w'))
    
@transform(create_keylist, suffix(".pickle"), ".results.pickle")
def featurize(infile, outfile):

    indata = pickle.load(open(infile, 'r'))

    keylist = indata['keylist']
    config_name = indata['config_name']
    config = indata['config']

    pipelineJar = "target/scala-2.10/solarflares-assembly-0.1.jar"
    pipelineClass = "pipelines.FITSTarballFeaturize"

    keyfile_name = infile + ".keylist"
    fid = open(keyfile_name, 'w')
    for ki, ko in keylist:
        fid.write("{} {}\n".format(ki, ko))
    fid.close()
    
    conf = {'s3bucket' : config['s3bucket'], 
            'numParts' : config['numParts'], 
            'keyFilename' : keyfile_name, 
            'runtag' : "fits_tarball_featurize.{}".format(config_name)}
    logpath = infile[:-7]
    print logpath
    pythonrun.run(conf, logpath, pipelineClass, pipelineJar, 
                  SPARK_EXECUTOR_CORES=16, 
                  KEYSTONE_MEM = KEYSTONE_MEM, use_yarn=USE_YARN)
    
    open(outfile, 'w')
if __name__ == "__main__":

    pipeline_run([create_keylist, featurize])
