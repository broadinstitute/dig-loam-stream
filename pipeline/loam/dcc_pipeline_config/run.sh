#!/bin/bash

source /broad/software/scripts/useuse
reuse Java-1.8
reuse Tabix
reuse UGER

mkdir uger
mkdir qc

java -jar -Xmx1G -Dloamstream-log-level="DEBUG" \
/humgen/diabetes2/users/ryank/software/dig-loam-stream/target/scala-2.12/LoamStream-assembly-1.3-SNAPSHOT.jar \
--conf loamstream.conf \
params.loam \
/humgen/diabetes2/users/ryank/software/dig-loam-stream/pipeline/loam/binaries.loam \
/humgen/diabetes2/users/ryank/software/dig-loam-stream/pipeline/loam/cloud_helpers.loam \
/humgen/diabetes2/users/ryank/software/dig-loam-stream/pipeline/loam/dcc_pipeline.loam \
/humgen/diabetes2/users/ryank/software/dig-loam-stream/pipeline/loam/scripts.loam \
/humgen/diabetes2/users/ryank/software/dig-loam-stream/pipeline/loam/store_helpers.loam
