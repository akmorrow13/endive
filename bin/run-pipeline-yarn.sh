#!/bin/bash

# Figure out where we are.
FWDIR="$(cd `dirname $0`; pwd)"

CLASS=$1
shift
JARFILE=$1
shift

# Figure out where the Scala framework is installed
FWDIR="$(cd `dirname $0`/..; pwd)"

if [ -z "$1" ]; then
  echo "Usage: run-main.sh <class> [<args>]" >&2
  exit 1
fi

if [ -z "$OMP_NUM_THREADS" ]; then
    export OMP_NUM_THREADS=1 added as we were nondeterministically running into an openblas race condition 
fi


echo "automatically setting OMP_NUM_THREADS=$OMP_NUM_THREADS"

ASSEMBLYJAR="$FWDIR"/target/scala-2.10/endive-assembly-0.1-deps.jar


echo "RUNNING ON THE CLUSTER" 
# TODO: Figure out a way to pass in either a conf file / flags to spark-submit
KEYSTONE_MEM=${KEYSTONE_MEM:-1g}
KEYSTONE_MEM=120g
export KEYSTONE_MEM

# Set some commonly used config flags on the cluster
spark-submit \
  --master yarn\
  --class $CLASS \
  --num-executors  12 \
  --executor-cores 48 \
  --driver-class-path $JARFILE:$ASSEMBLYJAR:$HOME/hadoop/conf \
  --driver-library-path /opt/amp/gcc/lib64:/opt/amp/openblas/lib:$FWDIR/lib \
  --conf spark.executor.extraLibraryPath=/opt/amp/openblas/lib:$FWDIR/lib \
  --conf spark.executor.extraClassPath=$JARFILE:$ASSEMBLYJAR:$HOME/hadoop/conf \
  --conf spark.driver.extraClassPath=$JARFILE:$ASSEMBLYJAR:$HOME/hadoop/conf \
  --conf spark.executorEnv.LD_LIBRARY_PATH=/opt/amp/gcc/lib64:/opt/amp/openblas/lib:$LD_LIBRARY_PATH \
  --conf spark.serializer=org.apache.spark.serializer.JavaSerializer \
  --conf spark.yarn.executor.memoryOverhead=15300 \
  --conf spark.mlmatrix.treeBranchingFactor=16 \
  --conf spark.shuffle.reduceLocality.enabled=true \
  --conf spark.mlmatrix.treeExecutorAgg=true \
  --conf spark.yarn.am.waitTime=200 \
  --conf spark.driver.maxResultSize=0 \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.yarn.appMasterEnv.OMP_NUM_THREADS=1 \
  --conf spark.network.timeout=600 \
  --conf spark.executorEnv.OMP_NUM_THREADS=1 \
  --conf spark.storage.memoryFraction=0.6 \
  --conf spark.network.timeout=300s \
  --driver-memory 60g \
  --executor-memory 100g \
  --jars $ASSEMBLYJAR \
  $JARFILE \
  "$@"
