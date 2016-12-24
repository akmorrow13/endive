import numpy as np
from pylab import *
import pythonrun
import os

BASE_KERNEL_PIPELINE_CONFIG = \
{
    "reference": "/home/eecs/akmorrow/ADAM/endive/workfiles/hg19.2bit",
    "dnase": "/user/vaishaal/endive-data/dnase_bams/coverage",
}

EXECUTOR_MEM = '100g'
NUM_EXECUTORS = 12
CORES_PER_EXECUTOR = 24

dataset_creation_pipeline_class = "net.akmorrow13.endive.pipelines.SingleTFDatasetCreationPipeline"

featurization_pipeline_class = "net.akmorrow13.endive.pipelines.KitchenSinkFeaturizePipeline"

solver_pipeline_class = "net.akmorrow13.endive.pipelines.SolverPipeline"
test_pipeline_class = "net.akmorrow13.endive.pipelines.TestPipeline"

pipeline_jar = os.path.relpath("../target/scala-2.10/endive-assembly-0.1.jar")


def make_gaussian_filter_gen(gamma, alphabet_size=4, kmer_size=8, seed=0):
    np.random.seed(seed)
    def gaussian_filter_gen(num_filters):
        out = np.random.randn(num_filters,kmer_size*alphabet_size).astype('float32') * gamma
        print out.shape
        return out
    return gaussian_filter_gen

def run_kitchensink_featurize_pipeline(windowPath,
               filterPath,
               logpath,
               filter_gen_gen=make_gaussian_filter_gen,
               gamma = 1.0,
               sample = 0.01,
               alphabet_size=4,
               kmer_size=8,
               num_filters=256,
               featuresOutput="/user/vaishaal/tmp/features",
               seed=0,
               cores_per_executor=CORES_PER_EXECUTOR,
               num_executors=NUM_EXECUTORS,
               executor_mem=EXECUTOR_MEM,
               use_yarn=True,
               base_config=BASE_KERNEL_PIPELINE_CONFIG):

    kernel_pipeline_config = base_config.copy()
    filter_gen = make_gaussian_filter_gen(gamma=gamma, alphabet_size=alphabet_size, kmer_size=kmer_size)

    w = filter_gen(num_filters)
    np.savetxt(filterPath, w, delimiter=",")
    kernel_pipeline_config["filtersPath"] = filterPath
    kernel_pipeline_config["aggregatedSequenceOutput"] = windowPath
    kernel_pipeline_config["featurizeSample"] = sample
    kernel_pipeline_config["kmerLength"] = kmer_size
    kernel_pipeline_config["approxDim"] = num_filters
    kernel_pipeline_config["readFiltersFromDisk"] = True
    kernel_pipeline_config["featuresOutput"] = featuresOutput
    kernel_pipeline_config["seed"] = seed


    pythonrun.run(kernel_pipeline_config,
              logpath,
              featurization_pipeline_class,
              pipeline_jar,
              executor_mem,
              cores_per_executor,
              num_executors,
              use_yarn=True)

    return True


def run_solver_pipeline(featuresPath,
               logpath,
               hdfsclient,
               cores_per_executor=CORES_PER_EXECUTOR,
               num_executors=NUM_EXECUTORS,
               executor_mem=EXECUTOR_MEM,
               use_yarn=True,
               valChromosomes=[],
               valCellTypes=[],
               reg=0.1,
               predictionsPath="/user/vaishaal/tmp",
               valDuringSolve=False,
               modelPath="/home/eecs/vaishaal/endive-models",
               base_config=BASE_KERNEL_PIPELINE_CONFIG):

    kernel_pipeline_config = base_config.copy()
    kernel_pipeline_config["featuresOutput"] = featuresPath
    kernel_pipeline_config["predictionsOutput"] = predictionsPath
    kernel_pipeline_config["valDuringSolve"] = valDuringSolve
    kernel_pipeline_config["valCellTypes"] = valCellTypes
    kernel_pipeline_config["valChromosomes"] = valChromosomes
    kernel_pipeline_config["modelOutput"] = modelPath
    kernel_pipeline_config["lambda"] = reg
    pythonrun.run(kernel_pipeline_config,
              logpath,
              solver_pipeline_class,
              pipeline_jar,
              executor_mem,
              cores_per_executor,
              num_executors,
              use_yarn=True)

    if valDuringSolve:
        train_preds = load_hdfs_vector(predictionsPath + "/trainPreds", hdfsclient=hdfsclient, shape=(-1, 2))
        valCellTypesStr = map(str, valCellTypes)
        val_pred_name = predictionsPath + "/valPreds_{0}_{1}".format(','.join(valChromosomes), ','.join(valCellTypesStr))
        print val_pred_name
        val_preds = load_hdfs_vector(val_pred_name, hdfsclient=hdfsclient, shape=(-1, 2))
        return (train_preds, val_preds)
    return ([], [])

def run_test_pipeline(featuresPath,
               logpath,
               hdfsclient,
               cores_per_executor=CORES_PER_EXECUTOR,
               num_executors=NUM_EXECUTORS,
               executor_mem=EXECUTOR_MEM,
               use_yarn=True,
               reg=0.1,
               predictionsPath="/user/vaishaal/tmp",
               modelPath="/home/eecs/vaishaal/endive-models",
               delete_predictions_from_hdfs=False,
               base_config=BASE_KERNEL_PIPELINE_CONFIG):

    kernel_pipeline_config = base_config.copy()
    kernel_pipeline_config["featuresOutput"] = featuresPath
    kernel_pipeline_config["predictionsOutput"] = predictionsPath
    pythonrun.run(kernel_pipeline_config,
              logpath,
              test_pipeline_class,
              pipeline_jar,
              executor_mem,
              cores_per_executor,
              num_executors,
              use_yarn=True)

    test_pred_name = predictionsPath + "/testPreds"
    test_preds = load_hdfs_vector(test_pred_name, hdfsclient=hdfsclient, shape=(-1, 2))
    test_meta_name = predictionsPath + "/testMetaData"
    test_meta  = load_test_metadata(test_meta_name, hdfsclient=hdfsclient)

    if (delete_predictions_from_hdfs):
        os.system("hadoop fs -rmr ${0}".format(test_pred_name))
        os.system("hadoop fs -rmr ${0}".format(test_meta_name))

    return test_preds, test_meta


def load_test_metadata(metadataPath, hdfsclient, tmpPath="/tmp/"):
    status = list(hdfsclient.copyToLocal([metadataPath], tmpPath))[0]
    fname = os.path.basename(os.path.normpath(metadataPath))
    print status['error']
    vectors = []
    if status['error'] != '':
        return
    for part in os.listdir(tmpPath + fname):
        partName = tmpPath + fname + "/" + part
        if (os.stat(partName).st_size == 0):
            continue
        part_vector = np.ravel(genfromtxt(partName, delimiter=",", dtype='str'))
        vectors.append(part_vector)

    ov = np.concatenate(vectors)
    ov = ov.reshape((-1,4))

    os.system("rm -rf " + tmpPath + fname)
    return ov




def load_hdfs_vector(hdfsPath, hdfsclient, tmpPath="/tmp/", shape=None):
    status = list(hdfsclient.copyToLocal([hdfsPath], tmpPath))[0]
    fname = os.path.basename(os.path.normpath(hdfsPath))
    print status['error']
    vectors = []
    if status['error'] != '':
        return

    for part in os.listdir(tmpPath + fname):
        partName = tmpPath + fname + "/" + part
        if (os.stat(partName).st_size == 0):
            continue
        part_vector = np.ravel(genfromtxt(partName, delimiter=","))
        vectors.append(part_vector)

    ov = np.concatenate(vectors)
    if (shape != None):
        ov = ov.reshape(shape)

    os.system("rm -rf " + tmpPath + fname)
    return ov

















