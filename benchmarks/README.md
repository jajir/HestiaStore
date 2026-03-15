# HestiaStore JMH Benchmarks

This module isolates JMH dependencies from production modules.

See also:

- [Benchmark history and per-change comparison](./benchmark-history.md)

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
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar ByteSequenceCrc32Benchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar StringEncodingBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexGetBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexHotPartitionPutBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexMixedDrainBenchmark
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
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar "StringEncodingBenchmark" -prof gc
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexGetBenchmark -p readPathMode=overlay -prof gc
```

Mixed partitioned-ingest workloads with concurrent reads:

```sh
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexMixedDrainBenchmark -prof gc
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexMixedDrainBenchmark -p workloadMode=splitHeavy -prof gc
```

Quick smoke run:

```sh
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar "ChunkStore.*Benchmark" -wi 1 -i 1 -f 1 -r 1s -w 1s
```

## Canonical compare flow

Canonical profile definitions live in `benchmarks/profiles`.

Validate profile contracts before changing canonical JMH coverage:

```sh
mvn -pl benchmarks -am -DskipTests=false -Dtest=BenchmarkProfileContractTest test
```

Run script-level smoke coverage for compare/history tooling:

```sh
mvn -pl benchmarks -am -DskipTests=false -Dtest=BenchmarkHistoryScriptsSmokeTest test
```

PR benchmark runs now surface in three places:

- Actions job summary
- a sticky PR comment with the latest comparison
- the `perf-artifacts` branch under
  `history/<profile>/pull-requests/pr-<number>/...`

Nightly canonical baselines continue to advance only through
`history/<profile>/latest-main.json`.

Run a profile locally:

```sh
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile segment-index-pr-smoke \
  --output-dir /tmp/hestia-bench/current
```

Compare two profile runs:

```sh
python3 benchmarks/scripts/compare_jmh_profile.py \
  --baseline /tmp/hestia-bench/base/summary.json \
  --candidate /tmp/hestia-bench/current/summary.json \
  --markdown-out /tmp/hestia-bench/comparison.md \
  --json-out /tmp/hestia-bench/comparison.json
```
