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

`benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar`

## Run benchmarks

The `concatenated-page-read` profile measures first materialization of fresh
slice trees, complete single-block and multiblock Zstd chunk reads, and ranked
Senku maintenance merges. It includes flat-array, cached-child, and steady-write
controls. Each case uses three JVM forks, a fixed 1 GiB heap, four active
processors, and GC profiling. Compare every parameter combination separately;
lower time and allocated bytes per operation are better, while higher write
throughput is better. The read fixtures use in-memory storage and include block
reads, CRC validation, and decompression. Run both versions on a quiet host
before drawing latency conclusions.

```sh
python3 benchmarks/scripts/run_jmh_profile.py --repo-root . \
  --profile concatenated-page-read --output-dir /tmp/hestia-bench/page-read
```

The `senku-pipeline` profile checks encoded-rank maintenance, bounded parallel
flush preparation, and generic versus explicitly selected primitive long-set
ingestion. It includes repeat/fork settings and GC profiling. Compare matching
parameter sets; checkpoint jars without an `api` parameter represent generic
ingestion. Short sustained-ingestion results can vary with maintenance phases.
Ready-summary metadata has measurable costs on small numeric-delta merges, so
report those alongside ranked-key improvements rather than averaging the two.

The `senku-maintenance-hotpaths` profile isolates repeated discovery of 8,192
immutable run manifests (with zero or 256 summary buckets) and four/64-way
primitive ranked set merges of one million input records (zero or 50 percent
duplicates). Merge fixtures retain Zstd 3, 8 KiB blocks and one-million-key
pages. Metadata fixtures contain manifests only and deliberately admit no merge
jobs. Compare the old strict periodic path with cached discovery after priming
the catalog; keep setup outside measurement. Use a quiet host for latency
claims and retain allocation and exact persisted-output checks separately.

```bash
python3 benchmarks/scripts/run_jmh_profile.py --repo-root . \
  --profile senku-maintenance-hotpaths --output-dir /tmp/hestia-bench/senku-maintenance
```

The `senku-batched-flush` profile adds four-million-key primitive ranked flushes
with uniform routing and a 75-percent hot shard, while retaining one-million-key
pages and Zstd 3. Its batch-ingestion comparison submits the same 4,096 generated
keys individually or through `putLongs`; JMH scores are normalized per key, not
per batch. Input generation is included identically in both ingestion modes.
Run on a quiet host before making speedup claims; an active solver invalidates
clean CPU comparisons. Compare final bytes/state and exact output separately.

```sh
python3 benchmarks/scripts/run_jmh_profile.py --repo-root . \
  --profile senku-batched-flush --output-dir /tmp/hestia-bench/senku-batched-flush
```

```sh
python3 benchmarks/scripts/run_jmh_profile.py --repo-root . \
  --profile senku-pipeline --output-dir /tmp/hestia-bench/senku-pipeline
```

```sh
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar ChunkStoreWriteBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar ChunkStoreSteadyWriteBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar DataBlockByteReaderBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SingleChunkEntryIteratorBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SortedDataFileWriterBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar ByteSequenceCrc32Benchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar StringEncodingBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar UniqueCacheSortedKeyIteratorBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexGetBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexMultiSegmentGetBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexRangeScanBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentMergeSequentialBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexHotRoutePutBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexMixedDrainBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexPersistedMutationBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexLifecycleBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SenkuIndexBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SequentialFileReadingBenchmark
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SequentialFileWritingBenchmark
```

Compare both modes in one run (recommended):

```sh
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar "ChunkStore.*Benchmark" -prof gc
```

Read-path only (recommended for byte-slice migration checks):

```sh
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar "DataBlockByteReaderBenchmark" -prof gc
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar "SingleChunkEntryIteratorBenchmark" -prof gc
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar "SortedDataFileWriterBenchmark" -prof gc
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar "StringEncodingBenchmark" -prof gc
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar "UniqueCacheSortedKeyIteratorBenchmark" -prof gc
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexGetBenchmark -p readPathMode=live -prof gc
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexMultiSegmentGetBenchmark -p workingSetMode=cold -prof gc
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexRangeScanBenchmark -prof gc
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentMergeSequentialBenchmark -prof gc
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
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexMixedDrainBenchmark -prof gc
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexMixedDrainBenchmark -p workloadMode=splitHeavy -prof gc
```

Persisted mutation and lifecycle paths:

```sh
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexPersistedMutationBenchmark -t 1 -p walMode=sync -prof gc
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexPersistedMutationBenchmark -t 16 -p walMode=sync -prof gc
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar SegmentIndexLifecycleBenchmark -p walMode=sync
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
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar \
  SenkuParallelIngestionBenchmark -prof jfr:dir=/tmp/hestia-bench/senku-jfr
```

Run the profile from both the candidate and a detached worktree at the selected
baseline commit on the same quiet machine. The sample-time result contains the
put p50, p95, and p99 percentiles; the throughput result and `gc` secondary
metrics contain operations per second and allocation per operation. Capture
process peak resident memory alongside each run with the operating system's
process-accounting tool.

The focused flush-preparation profile excludes page encoding and filesystem I/O
to compare the current range-partitioned reference array with separate
per-shard arrays, maps already partitioned by persistent shard, and concurrent
sorting of independent shard ranges. It also compares the construction cost of
the current mutation-stripe maps with persistent-shard maps so moving work into
ingestion is not evaluated as a free optimization:

```sh
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile senku-flush-preparation \
  --output-dir /tmp/hestia-bench/senku-flush-preparation
```

The complete flush-writer profile starts with populated detached maps and times
the production partitioning, sorting, page encoding, and `MemDirectory` write:

```sh
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile senku-flush-writer \
  --output-dir /tmp/hestia-bench/senku-flush-writer
```

The bounded ingestion-map baseline isolates construction of unique entries and
eight-thread updates of an existing fixed key set. It compares a synthetic
unique-hash distribution that systematically collapses JDK bucket bits, the
real bit-board-style distribution, and randomized keys without including
flush, maintenance, filesystem, or final-drain work:

```sh
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile senku-ingestion-map-baseline \
  --output-dir /tmp/hestia-bench/senku-ingestion-map-baseline
```

Four focused profiles cover the current ingestion, flush-ordering, and
maintenance-encoding hot paths. Run each profile from the candidate and the
selected baseline worktree on the same quiet host:

```sh
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile senku-ingestion-contention \
  --output-dir /tmp/hestia-bench/senku-ingestion-contention

python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile senku-flush-ordering \
  --output-dir /tmp/hestia-bench/senku-flush-ordering

python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile senku-merge-encoding \
  --output-dir /tmp/hestia-bench/senku-merge-encoding

python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile senku-compression \
  --output-dir /tmp/hestia-bench/senku-compression
```

The ingestion profile compares `HashMap` with the mixed open-addressed table at
32, 64, and 128 stripes for build and duplicate-update throughput. Its lock
benchmark exposes accumulated wait and hold nanoseconds as JMH auxiliary
counters; divide each counter by its `operations` counter for nanoseconds per
update. Its rotation benchmark reports the latency and allocation cost of
locking every stripe and replacing the lazy batch. The flush profile isolates
partitioning and ordering, comparing the former full `Map.Entry[]` plus TimSort
with serial and four-worker compact ordering. The merge profile measures the
complete decode, reduction, page encoding, compression, and in-memory write
phase for generic and exact primitive-long paths at 8, 32, and 128 KiB block
sizes. The compression profile fixes the primitive path and 8 KiB blocks, and
compares `prefix-zstd`, `delta-zstd`, and `delta-none`. Its synthetic interleaved
long keys isolate pipeline costs; use representative board pages separately to
measure compression ratio. When comparing against the previous Snappy-only
revision, omit `storage` on the baseline and keep the JDK, entry count, block
size, warmups, measurements, forks, and profiler identical.

Local validation on macOS with JDK 25.0.2 (2026-09-05), using the profile's
100,000 interleaved keys, 8 KiB blocks, two 500 ms warmups, four 500 ms
measurements, two forks, and the GC profiler:

| Primitive merge pipeline | Mean time (ms/op) | Java allocation (MB/op) |
| --- | ---: | ---: |
| Previous prefix/Snappy | 1.898 | 2.203 |
| Prefix/Zstd-3 | 2.056 | 2.040 |
| Delta-varint/Zstd-3 | 1.460 | 0.784 |
| Delta-varint/raw | 1.544 | 1.128 |

The earlier baseline run was 1.936 ms/op, so the selected delta/Zstd pipeline
reduced measured time by 23–25% and allocation by 64%. Prefix/Zstd alone traded
some CPU time for space. These synthetic timings are not full solver throughput.

A separate production-codec round-trip check used 12,395,378 saved 49-bit board
keys, regrouped with the application's 25-bit prefix routing into 128 pages.
Payloads occupied 28,803,721 bytes with prefix/Snappy, 18,822,895 bytes with
prefix/Zstd-3, and 13,333,436 bytes with delta-varint/Zstd-3 (54% smaller than
prefix/Snappy). All keys round-tripped exactly. These are payload bytes, excluding
chunk headers, block padding, and metadata; the saved sample is not a claim
about every solver round's compression ratio.

The `senku-fixed-weight-compression` profile compares numeric deltas with
fixed-weight parity-class rank deltas on exactly the same synthetic logical
keys. Both variants use the primitive merge path, Zstd-3, and 8 KiB blocks.
Run it when changing rank conversion or its merge/reader integration:

```sh
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile senku-fixed-weight-compression \
  --output-dir /tmp/hestia-bench/senku-fixed-weight-compression
```

The synthetic keys have 27 set bits within 49 bits and even parity under mask
`3`; they are not claimed to represent a solver frontier. This profile isolates
the rank conversion CPU and allocation tradeoff. Use real keys separately for
compression-density measurements, and verify reconstructed logical keys.

Local validation on Apple M4/macOS with JDK 25.0.2 (2026-09-05), using the
profile's 100,000 logical keys, two 500 ms warmups, four 500 ms measurements,
two forks, and the GC profiler:

| Primitive fixed-weight merge pipeline | Mean time (ms/op) | Java allocation (B/op) |
| --- | ---: | ---: |
| Numeric delta / Zstd-3 | 1.688 ± 0.064 | 1,169,523 |
| Fixed-weight parity rank delta / Zstd-3 | 11.923 ± 0.289 | 787,270 |

The rank pipeline was 7.06 times slower in this CPU-focused merge benchmark,
while allocating 32.7% fewer Java bytes per operation. These measurements are
not full solver throughput or a compression-ratio measurement: rank encoding
trades conversion CPU for more compact pages, and must be evaluated together
with real-data storage savings and the rest of the application pipeline.

The `gc` profiler records allocation per operation, allocation rate, GC count,
and GC time beside the primary throughput or latency result. The merge profile
also has a short JFR pass for allocation-site and CPU phase attribution. For a
separate peak-resident-memory measurement on macOS, build the runner and wrap a
single-fork invocation with the operating-system accounting tool:

```sh
mvn -pl benchmarks -am package

/usr/bin/time -l java -jar \
  benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar \
  SenkuMergeEncodingBenchmark.merge \
  -p path=primitive -p dataBlockBytes=8192 \
  -wi 1 -i 3 -f 1 -prof jfr:dir=/tmp/hestia-bench/senku-merge-jfr
```

Record `maximum resident set size` from `time`, and use the generated recording
to inspect peak live heap, GC pauses, and samples attributed to source decoding,
duplicate reduction, page encoding, compression, and writing. Keep this pass
separate from canonical GC-profiler comparisons because JFR changes the
measurement overhead.

Quick smoke run:

```sh
java -jar benchmarks/target/benchmarks-1.1.1-SNAPSHOT.jar "ChunkStore.*Benchmark" -wi 1 -i 1 -f 1 -r 1s -w 1s
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
