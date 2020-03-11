#!/bin/bash
TARGET_PATH=~/benchmark-results/
mkdir -p $TARGET_PATH

pushd SOMns
echo Starting Benchmark of Uniform `date '+%d/%m/%Y_%H:%M:%S'`
rebench -f -S bench-light.conf all
echo Finished Benchmark of Uniform `date '+%d/%m/%Y_%H:%M:%S'`

## Archive results
bzip2 uniform.data
mv uniform.data.bz2 $TARGET_PATH/

echo Data archived to $TARGET_PATH

popd
pushd SOMns_ALT
echo Starting Benchmark of Specialized `date '+%d/%m/%Y_%H:%M:%S'`
rebench -f -S bench-light.conf all
echo Finished Benchmark of Specialized `date '+%d/%m/%Y_%H:%M:%S'`

## Archive results
bzip2 uniform.data
mv specialized.data.bz2 $TARGET_PATH/

echo Data archived to $TARGET_PATH
popd