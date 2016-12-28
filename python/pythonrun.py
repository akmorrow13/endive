"""
Experimental code to try running the keystone pipeline from python
with the YAML config 
"""

"""
runpipeline will either run spark locally if SPARK_HOME is not set
or run spark on the cluster if SPARK_HOME is set

note KEYSTONE_HOME must be set

./run-pipeline.sh classname jarfile 
"""
import os
import subprocess
import pyaml
import yaml

import itertools
import time 

def dict_product(dicts):
    return (dict(itertools.izip(dicts, x)) for x in itertools.product(*dicts.itervalues()))


def run(config, logpath,
        pipelineClass, pipelineJar, 
        KEYSTONE_MEM = "100g", 
        SPARK_EXECUTOR_CORES =32, 
        SPARK_NUM_EXECUTORS = 14,
        OMP_NUM_THREADS=1, 
        output_sanity_check=True, 
        use_yarn = False):

    t1 = time.time()
    config_yaml = logpath +  ".config.yaml"
    time_log = logpath +  ".time"
    env = os.environ.copy()
    myenv = {}
    myenv['KEYSTONE_MEM'] = KEYSTONE_MEM
    myenv['SPARK_EXECUTOR_CORES'] = str(SPARK_EXECUTOR_CORES)
    myenv['SPARK_NUM_EXECUTORS'] = str(SPARK_NUM_EXECUTORS)
    myenv['OMP_NUM_THREADS'] = str(OMP_NUM_THREADS)
    env.update(myenv)
    # sanity check before running the process
    
    # if not os.path.isdir(outdir):
    #     raise ValueError("output dir must exist")

    logfile = logpath + ".spark.log"
    # if os.path.exists(logfile) and output_sanity_check:
    #     raise ValueError("output dir has logfile, should be empty")
    
    pipelineJar = os.path.abspath(pipelineJar)
    if not os.path.exists(pipelineJar):
        raise ValueError("Cannot find pipeline jar")
    
    yaml.dump(config, open(config_yaml, 'w'))

    # basically capturing the popen output and saving it to disk  and 
    # printing it to screen are a multithreaded nightmare, so we let
    # tee do the work for us

    ## pipefail set so that we get the correct process return code
    
    if use_yarn:
        cmd = os.path.abspath("../bin/run-pipeline-yarn.sh")
    else:
        cmd = os.path.abspath("../bin/run-pipeline.sh")
    cmdstr = " ".join(["set -o pipefail;", 
                       cmd, pipelineClass, 
                       pipelineJar, config_yaml, "2>&1", 
                       "|", "tee", logfile])
    # entirely for debugging
    rerun_sh_file = open(logpath + ".spark.run.sh", 'w')
    for k, v in myenv.iteritems():
        rerun_sh_file.write("export %s=%s\n" % (k, v))
    rerun_sh_file.write("\n\n%s\n" % cmdstr)
    rerun_sh_file.flush()
                      
    p = subprocess.Popen(cmdstr, 
                         shell=True, executable='/bin/bash', 
                         env = env)
    p.wait()

    if p.returncode != 0:
        raise Exception("invocation terminated with non-zero exit status")

    time_log_fid = open(time_log, 'w')
    time_log_fid.write("%f\n" % (time.time() - t1))
    time_log_fid.close()

if __name__ == "__main__":
    # capture the log file

    # example yaml config

    config = {'reference': '/home/eecs/akmorrow/ADAM/endive/workfiles/hg19.2bit',
    'dnase': '/user/vaishaal/endive-data/dnase_bams/coverage',
    'aggregatedSequenceOutput': '/user/vaishaal/endive-data/aggregated/ATF2/ATF2'}

    pipelineClass="net.akmorrow13.endive.pipelines.KernelPipeline"
    pipelineJar = os.path.relpath("target/scala-2.10/endive-assembly-0.1.jar")
    logpath = os.path.relpath("/home/eecs/vaishaal/logs")
    run(config, logpath, pipelineClass, pipelineJar, use_yarn=True)
