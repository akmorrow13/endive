#!/usr/bin/env bash

# Figure out where MANGO is installed
SCRIPT_DIR="$(cd `dirname $0`/..; pwd)"

# Setup CLASSPATH like appassembler

# Assume we're running in a binary distro
ENDIVE_CMD="$SCRIPT_DIR/bin/endive"
REPO="$SCRIPT_DIR/repo"

# Fallback to source repo
if [ ! -f $ENDIVE_CMD ]; then
ENDIVE_CMD="$SCRIPT_DIR/target/appassembler/bin/endive"
REPO="$SCRIPT_DIR/target/appassembler/repo"
fi

if [ ! -f "$ENDIVE_CMD" ]; then
  echo "Failed to find appassembler scripts in $BASEDIR/bin"
  echo "You need to build ENDIVE before running this program"
  exit 1
fi
eval $(cat "$ENDIVE_CMD" | grep "^CLASSPATH")

echo "$CLASSPATH"
