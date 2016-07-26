#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"

DATE=`date +"%Y_%m_%d_%H_%M_%S"`
PIPELINE=net.akmorrow13.endive.pipelines.FullMatrixPipeline
$FWDIR/bin/run-pipeline-local.sh $PIPELINE $FWDIR/target/scala-2.10/endive-assembly-0.1.jar "$@" 2>&1
