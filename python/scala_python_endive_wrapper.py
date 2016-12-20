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
    kernel_pipeline_config["dim"] = num_filters
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








