# HestiaStore JMH Benchmarks

This module isolates JMH dependencies from production modules.

## Why separate module

- JMH dependencies are declared only here.
- The module is configured with:
  - `maven.install.skip=true`
  - `maven.deploy.skip=true`
- Result: benchmark artifacts are not installed to local Maven repo and are not deployed.

## Build benchmark runner

From repository root:

```sh
mvn -pl benchmarks -am package
```

This produces a runnable JMH fat-jar:

`benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar`

## Run benchmarks

```sh
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar ChunkStoreWriteBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar ChunkStoreSteadyWriteBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar DataBlockByteReaderBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SingleChunkEntryIteratorBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SortedDataFileWriterBenchmark
```

Compare both modes in one run (recommended):

```sh
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar "ChunkStore.*Benchmark" -prof gc
```

Read-path only (recommended for byte-slice migration checks):

```sh
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar "DataBlockByteReaderBenchmark" -prof gc
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar "SingleChunkEntryIteratorBenchmark" -prof gc
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar "SortedDataFileWriterBenchmark" -prof gc
```

Quick smoke run:

```sh
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar "ChunkStore.*Benchmark" -wi 1 -i 1 -f 1 -r 1s -w 1s
```
