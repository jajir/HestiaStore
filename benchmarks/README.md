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
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar UniqueCacheSortedKeyIteratorBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexGetBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexMultiSegmentGetBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexRangeScanBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentMergeSequentialBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexHotRoutePutBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexMixedDrainBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexPersistedMutationBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexLifecycleBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SenkuIndexBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SequentialFileReadingBenchmark
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SequentialFileWritingBenchmark
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
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar "UniqueCacheSortedKeyIteratorBenchmark" -prof gc
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexGetBenchmark -p readPathMode=live -prof gc
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexMultiSegmentGetBenchmark -p workingSetMode=cold -prof gc
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexRangeScanBenchmark -prof gc
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentMergeSequentialBenchmark -prof gc
```

The measured sorted-key snapshot comparison is documented in
[`results/unique-cache-sorted-key-iterator-comparison.md`](results/unique-cache-sorted-key-iterator-comparison.md).

The range-scan benchmark compares the bounded API with full-stream filtering
and also measures a complete sequential read. All measurements use `FAIL_FAST`
and reject an invocation unless it returns the complete expected result. This
prevents maintenance, cache-capacity eviction, or index closing from appearing
as an artificial speedup. The segment-merge benchmark isolates the unbounded
per-entry hot loop so bounded-scan checks cannot regress it unnoticed.

Mixed partitioned-ingest workloads with concurrent reads:

```sh
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexMixedDrainBenchmark -prof gc
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexMixedDrainBenchmark -p workloadMode=splitHeavy -prof gc
```

Persisted mutation and lifecycle paths:

```sh
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexPersistedMutationBenchmark -t 1 -p walMode=sync -prof gc
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexPersistedMutationBenchmark -t 16 -p walMode=sync -prof gc
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexLifecycleBenchmark -p walMode=sync
```

Run both persisted-mutation thread counts with otherwise identical parameters.
The one-writer case protects latency-sensitive behavior; the 16-writer case
exposes WAL queue admission and sync-batching contention.

Senku's first-version baseline uses only `MemDirectory` and covers ingestion,
synchronous flush, flush-to-L0, recursive run merge, complete ready streaming,
and ingest-to-first-sorted-result latency. Its end-to-end cases include 0% and
50% duplicates, balanced and skewed shards, and page/part boundary crossings:

```sh
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile senku-index-baseline \
  --output-dir /tmp/hestia-bench/senku
```

The filesystem-backed parallel-ingestion profile measures eight caller threads
using unique `Long` keys with deliberately biased bit-board-style hashes and the
non-null `NullValue.NULL` singleton. It uses a ten-million-entry rotation
threshold, 32 persistent shards, a fixed 4 GiB heap, and three forks. Each fork
uses one index across warmup and measurement so flush and maintenance work
overlaps sustained ingestion. The runner retains raw JMH JSON and log files
under the supplied output directory:

```sh
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile senku-parallel-ingestion \
  --output-dir /tmp/hestia-bench/senku-parallel
```

Run the same class with JFR when CPU, allocation hot spots, garbage-collection
generations and pauses, or peak live heap are needed. Keep JFR separate from the
GC-profiler run so profiler overhead is comparable between commits:

```sh
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar \
  SenkuParallelIngestionBenchmark -prof jfr:dir=/tmp/hestia-bench/senku-jfr
```

Run the profile from both the candidate and a detached worktree at the selected
baseline commit on the same quiet machine. The sample-time result contains the
put p50, p95, and p99 percentiles; the throughput result and `gc` secondary
metrics contain operations per second and allocation per operation. Capture
process peak resident memory alongside each run with the operating system's
process-accounting tool.

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
- a sticky PR comment with the latest comparison against canonical `main`
  and, once a PR has history, the delta against the previous PR run
- the `perf-artifacts` branch under
  `history/<profile>/pull-requests/pr-<number>/...`

Canonical `main` baselines continue to advance through
`history/<profile>/latest-main.json`. Today, `segment-index-pr-smoke`
publishes there on `push` to `main`, while `segment-index-nightly` and
`diskio-nightly` publish there from the nightly schedule.

Run a profile locally:

```sh
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile segment-index-pr-smoke \
  --output-dir /tmp/hestia-bench/current
```

Measure Logback-backed MDC overhead on otherwise identical live-get and hot-put
workloads:

```sh
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile segment-index-context-logging \
  --output-dir /tmp/hestia-bench/context-logging
```

Run the nightly disk I/O profile locally:

```sh
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile diskio-nightly \
  --output-dir /tmp/hestia-bench/diskio
```

Compare two profile runs:

```sh
python3 benchmarks/scripts/compare_jmh_profile.py \
  --baseline /tmp/hestia-bench/base/summary.json \
  --candidate /tmp/hestia-bench/current/summary.json \
  --markdown-out /tmp/hestia-bench/comparison.md \
  --json-out /tmp/hestia-bench/comparison.json
```
