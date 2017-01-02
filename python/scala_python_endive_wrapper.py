import numpy as np
from pylab import *
import pythonrun
import os
import pandas as pd
from sklearn import metrics
from joblib import Parallel, delayed
import multiprocessing

predictionsPath = "hdfs://amp-spark-master.amp:8020/user/akmorrow/predictions"
modelPath = "/home/eecs/akmorrow/endive-models"
partitions = 2000

BASE_KERNEL_PIPELINE_CONFIG = \
{
    "reference": "/home/eecs/akmorrow/ADAM/endive/workfiles/hg19.2bit",
    "dnaseNarrow": "/data/anv/DREAMDATA/DNASE/peaks/relaxed/",
    "dnaseBams": "/data/anv/DREAMDATA/dnase_bams/merged_coverage/"
}

EXECUTOR_MEM = '100g'
NUM_EXECUTORS = 30
CORES_PER_EXECUTOR = 8

dataset_creation_pipeline_class = "net.akmorrow13.endive.pipelines.SingleTFDatasetCreationPipeline"

featurization_pipeline_class = "net.akmorrow13.endive.pipelines.DnaseKernelPipeline"

# solver_pipeline_class = "net.akmorrow13.endive.pipelines.SolverHardNegativeThreshPipeline"
solver_pipeline_class = "net.akmorrow13.endive.pipelines.SolverPipeline"
test_pipeline_class = "net.akmorrow13.endive.pipelines.TestPipeline"

pipeline_jar = os.path.relpath("../target/scala-2.10/endive-assembly-0.1.jar")


def recall_at_fdr(y_true, y_score, fdr_cutoff=0.05):
    precision, recall, thresholds = metrics.precision_recall_curve(y_true, y_score)
    fdr = 1- precision
    cutoff_index = next(i for i, x in enumerate(fdr) if x <= fdr_cutoff)
    return recall[cutoff_index]


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
               featuresOutput, # ="/user/vaishaal/tmp/features",
               filter_gen_gen=make_gaussian_filter_gen,
               gamma = 1.0,
               sample = 0.01,
               alphabet_size=4,
               num_partitions=337,
               kmer_size=8,
               num_filters=256,
               seed=0,
               cores_per_executor=CORES_PER_EXECUTOR,
               num_executors=NUM_EXECUTORS,
               executor_mem=EXECUTOR_MEM,
               use_yarn=True,
               base_config=BASE_KERNEL_PIPELINE_CONFIG):

    kernel_pipeline_config = base_config.copy()
    # filter_gen = make_gaussian_filter_gen(gamma=gamma, alphabet_size=alphabet_size, kmer_size=kmer_size)

    # w = filter_gen(num_filters)
    # np.savetxt(filterPath, w, delimiter=",")
    # kernel_pipeline_config["filtersPath"] = filterPath
    kernel_pipeline_config["numPartitions"] = num_partitions
    kernel_pipeline_config["aggregatedSequenceOutput"] = windowPath
    kernel_pipeline_config["featurizeSample"] = sample
    kernel_pipeline_config["kmerLength"] = kmer_size
    kernel_pipeline_config["approxDim"] = num_filters
    kernel_pipeline_config["readFiltersFromDisk"] = True
    kernel_pipeline_config["featuresOutput"] = featuresOutput
    kernel_pipeline_config["seed"] = seed
    kernel_pipeline_config["alphabetSize"] = alphabet_size
    kernel_pipeline_config["numPartitions"] = partitions
    print kernel_pipeline_config


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
               predictionsPath, #="/user/vaishaal/tmp",
               modelPath, #="/home/eecs/vaishaal/endive-models",
               cores_per_executor=CORES_PER_EXECUTOR,
               num_executors=NUM_EXECUTORS,
               executor_mem=EXECUTOR_MEM,
               use_yarn=True,
               valChromosomes=[],
               valCellTypes=[],
               reg=0.1,
               negativeSamplingFreq=1.0,
               mixtureWeight=-1.0,         
               predictedOutput=None,         
               valDuringSolve=False,
               base_config=BASE_KERNEL_PIPELINE_CONFIG):

    kernel_pipeline_config = base_config.copy()
    kernel_pipeline_config["featuresOutput"] = featuresPath
    kernel_pipeline_config["predictionsOutput"] = predictionsPath
    kernel_pipeline_config["valDuringSolve"] = valDuringSolve
    kernel_pipeline_config["valCellTypes"] = valCellTypes
    kernel_pipeline_config["valChromosomes"] = valChromosomes
    kernel_pipeline_config["modelOutput"] = modelPath
    kernel_pipeline_config["lambda"] = reg
    kernel_pipeline_config["negativeSamplingFreq"] = negativeSamplingFreq
    kernel_pipeline_config["mixtureWeight"] = mixtureWeight
    if (predictedOutput != None):
        kernel_pipeline_config["saveTestPredictions"] = predictedOutput
    
    pythonrun.run(kernel_pipeline_config,
              logpath,
              solver_pipeline_class,
              pipeline_jar,
              executor_mem,
              cores_per_executor,
              num_executors,
              use_yarn=True)

    if valDuringSolve:
        train_preds = load_hdfs_vector_parallel(predictionsPath + "/trainPreds", hdfsclient=hdfsclient, shape=(-1, 2))
        valCellTypesStr = map(str, valCellTypes)
        val_pred_name = predictionsPath + "/valPreds_{0}_{1}".format(','.join(valChromosomes), ','.join(valCellTypesStr))
        print val_pred_name
        val_preds = load_hdfs_vector_parallel(val_pred_name, hdfsclient=hdfsclient, shape=(-1, 2))
        return (train_preds, val_preds)
    return ([], [])

def run_test_pipeline(featuresPath,
               logpath,
               hdfsclient,
               predictionsPath, # ="/user/vaishaal/tmp",
               modelPath, # ="/home/eecs/vaishaal/endive-models",
               cores_per_executor=CORES_PER_EXECUTOR,
               num_executors=NUM_EXECUTORS,
               executor_mem=EXECUTOR_MEM,
               use_yarn=True,
               reg=0.1,
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
    test_preds = load_hdfs_vector_parallel(test_pred_name, hdfsclient=hdfsclient, shape=(-1, 2))
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
    ov = ov.reshape((-1,5))

    os.system("rm -rf " + tmpPath + fname)
    return pd.DataFrame(ov, columns=['chr', 'start', 'end', 'cellType'])




def load_hdfs_vector(hdfsPath, hdfsclient, tmpPath="/tmp/", shape=None):
    status = list(hdfsclient.copyToLocal([hdfsPath], tmpPath))[0]
    print status
    fname = os.path.basename(os.path.normpath(hdfsPath))
    print status['error']
    vectors = []
    if status['error'] != '':
        return

    for part in os.listdir(tmpPath + fname):
        print(part)
        partName = tmpPath + fname + "/" + part
        if (os.stat(partName).st_size == 0):
            continue
        part_vector = np.ravel(genfromtxt(partName, delimiter=",", dtype='float16'))
        vectors.append(part_vector)

    ov = np.concatenate(vectors)
    if (shape != None):
        ov = ov.reshape(shape)

    os.system("rm -rf " + tmpPath + fname)
    return ov

def parse_part(part, tmpPath, fname):
    partName = tmpPath + fname + "/" + part
    part_vector = np.ravel(genfromtxt(partName, delimiter=",", dtype='float16'))
    return part_vector


def load_hdfs_vector_parallel(hdfsPath, hdfsclient, tmpPath="/tmp/", shape=None):

    fname = os.path.basename(os.path.normpath(hdfsPath))
    os.system("hadoop fs -copyToLocal  {0} {1}".format(hdfsPath, tmpPath))
    def filter_empty(part):
        partName = tmpPath + fname + "/" + part
        return os.stat(partName).st_size != 0

    parts = filter(filter_empty, list(os.listdir(tmpPath + fname)))

    num_cores = multiprocessing.cpu_count()

    # this part is now in parallel
    vectors = Parallel(n_jobs=num_cores)(delayed(parse_part)(i, tmpPath, fname) for i in parts)
    ov = np.concatenate(vectors)
    if (shape != None):
        ov = ov.reshape(shape)
    os.system("rm -rf " + tmpPath + fname)
    return ov


def make_submission_output(outfile, test_preds, meta_df):
    meta_df = meta_df.copy(True)
    meta_df['chr_int'] = meta_df['chr'].map(lambda x: int(x.replace('chr', '').replace('X', '24').replace('Y', '25')))
    meta_df['start'] = pd.to_numeric(meta_df['start'])
    sorted_test = meta_df.sort_values(['chr_int', 'start'])[['chr', 'start', 'end']]
    prob_pred = (test_preds[:,1] - min(test_preds[:,1]))/max(test_preds[:,1] - min(test_preds[:,1]))
    sorted_test['prob'] = prob_pred
    sorted_test.to_csv(outfile, sep='\t', header=False)

def pr_result(y_test, y_test_pred, y_train=None, y_train_pred=None):
    fpr, tpr, thresh = metrics.precision_recall_curve(y_test, y_test_pred)
    test_auc = metrics.average_precision_score(y_test, y_test_pred)
    plot(fpr, tpr, label="test")
    print("Test PR AUC {0}".format(test_auc))
    if (not (y_train is None) and not (y_train_pred is None)):
        fpr, tpr, thresh = metrics.precision_recall_curve(y_train, y_train_pred)
        train_auc = metrics.average_precision_score(y_train, y_train_pred)
        plot(fpr, tpr, label="train")
        print("Train PR AUC {0}".format(train_auc))
    else:
        train_auc = None
    plt.legend(loc=4)
    plt.figure()

    return test_auc, train_auc


def roc_result(y_test, y_test_pred, y_train=None, y_train_pred=None):
    fpr, tpr, thresh = metrics.roc_curve(y_test, y_test_pred)
    test_auc = metrics.roc_auc_score(y_test, y_test_pred)
    plot(fpr, tpr, label="test")
    print("Test ROC AUC {0}".format(test_auc))
    if (not (y_train is None) and not (y_train_pred is None)):
        fpr, tpr, thresh = metrics.roc_curve(y_train, y_train_pred)
        train_auc = metrics.roc_auc_score(y_train, y_train_pred)
        plot(fpr, tpr, label="train")
        print("Train PR AUC {0}".format(train_auc))
    else:
        train_auc = None
    plt.legend(loc=4)
    plt.figure()
    return test_auc, train_auc



def cross_validate(feature_path, hdfsclient, chromosomes, cellTypes,                                    logPath, 
                   numHoldOutChr=1, 
                   numHoldOutCell=1,
                   num_folds=1,
                   regs=[0.1],
                   negativeSamplingFreqs=[1.0],
                   mixtureWeights=[-1.0],
                   seed=0,
                   executor_mem='100g',
                   predicted_output=None,
                   cores_per_executor=32,
                   num_executors=14,
                   other_meta={}):
    results = []
    np.random.seed(seed=seed)
    for fold in range(num_folds):
        print("FOLD " + str(fold))
        test_cell_types = []
        test_chromosomes = []
        test_chromosomes = map(lambda i: chromosomes[i], np.random.choice(len(chromosomes), numHoldOutChr, replace=False))
        test_cell_types = map(lambda i: cellTypes[i], np.random.choice(len(cellTypes), numHoldOutCell, replace=False))
        print("HOLDING OUT CHROMOSOMES {0}".format(test_chromosomes))
        print("HOLDING OUT CELL TYPES {0}".format(test_cell_types))
        for neg in negativeSamplingFreqs:
          for reg in regs:
            for mixtureWeight in mixtureWeights:
                print("RUNING SOLVER WITH REG={0}".format(reg))
                print("AND MIXTUREWEIGHT={0}".format(mixtureWeight))
                
                train_res, val_res = run_solver_pipeline(feature_path,
                               logPath,
                               hdfsclient,
                               predictionsPath,
                               modelPath,                          
                               executor_mem=executor_mem,
                               num_executors=num_executors,
                               cores_per_executor=cores_per_executor,
                               reg=reg,
                               negativeSamplingFreq=neg,
                               mixtureWeight=mixtureWeight,                           
                               valCellTypes=[8],
                               valChromosomes=test_chromosomes,
                               predictedOutput=None,
                               valDuringSolve=True)
                
                train_metrics = compute_metrics(train_res[:, 1], train_res[:, 0], tag='train')
                val_metrics = compute_metrics(val_res[:, 1], val_res[:, 0], tag='val')
                result = dict(train_metrics.items() + val_metrics.items() + other_meta.items())
                result['reg'] = reg
                print(mixtureWeight)
                result['negativeSamplingFreq'] = neg
                result['test_chromosomes'] = test_chromosomes
                result['test_celltypes'] = test_cell_types
                results.append(result)
                print(result)
    return pd.DataFrame(results)



def compute_metrics(predictions, labels, tag=''):
    predictions = predictions[np.where(labels >= 0)]
    labels = labels[np.where(labels >= 0)]
    print("COMPUTING {0} metrics".format(tag))
    result = {}
    result[tag + '_auPRC'] = metrics.average_precision_score(labels, predictions)
    result[tag + '_auROC'] = metrics.roc_auc_score(labels, predictions)
    result[tag + '_recall_at_50_fdr'] = recall_at_fdr(labels, predictions, fdr_cutoff=0.50)
    result[tag + '_recall_at_25_fdr'] = recall_at_fdr(labels, predictions, fdr_cutoff=0.25)
    result[tag + '_recall_at_10_fdr'] = recall_at_fdr(labels, predictions, fdr_cutoff=0.10)
    result[tag + '_recall_at_05_fdr'] = recall_at_fdr(labels, predictions, fdr_cutoff=0.05)
    return result

def string_to_enum_celltypes(string_cell_types):
    ALL_CELLTYPES = ['A549','GM12878', 'H1hESC', 'HCT116', 'HeLaS3', 'HepG2', 'IMR90', 'K562', 'MCF7', 'PC3',
  'Panc1', 'SKNSH', 'inducedpluripotentstemcell', 'liver']
    return map(lambda x: x[0], filter(lambda x: x[1] in string_cell_types, enumerate(ALL_CELLTYPES)))














