#!/bin/bash

OUTFILE=${1?"Usage: $0 <output filename for flight recording>"}

java -DdataConfig=../pipeline/conf/ci/camp-ci.conf -DpipelineConfig=../pipeline/conf/qc.conf -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:StartFlightRecording=duration=240s,filename=${OUTFILE},settings=loamstream.jfc -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -jar ../target/scala-2.12/LoamStream-assembly-1.3-SNAPSHOT.jar --conf ../pipeline/conf/loamstream.conf `ls ../pipeline/loam/*.loam | grep -v analysis`
