#!/bin/sh
#
#
#
cd `dirname $0`
cd ../..

DIR=./target/

RUN=java -jar target/jmh-benchmarks.jar com.coroptis.index.loadtest.Main
