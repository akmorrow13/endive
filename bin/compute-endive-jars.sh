#!/usr/bin/env bash

set -e

# Figure out where MANGO is installed
ENDIVE_REPO="$(cd `dirname $0`/..; pwd)"

CLASSPATH="$(. "$ENDIVE_REPO"/bin/compute-endive-classpath.sh)"

# list of jars to ship with spark; trim off the first from the CLASSPATH --> this is /etc
ENDIVE_JARS="$(echo "$CLASSPATH" | tr ":" "\n" | tail -n +2 | perl -pe 's/\n/,/ unless eof' )"

echo "$ENDIVE_JARS"

