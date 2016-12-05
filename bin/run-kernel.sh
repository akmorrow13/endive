#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"

DATE=`date +"%Y_%m_%d_%H_%M_%S"`
PIPELINE=net.akmorrow13.endive.pipelines.KernelPipeline
$FWDIR/bin/run-pipeline-yarn.sh $PIPELINE $FWDIR/target/scala-2.10/endive-assembly-0.0.1-SNAPSHOT.jar "$@" 2>&1

