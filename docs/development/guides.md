# Developer Guides

This page links repeatable maintainer workflows that do not fit naturally into
user documentation.

## Run JMH benchmarks

The current JMH runner lives in the `benchmarks` module.

Build the runner:

```bash
mvn -pl benchmarks -am package
```

Run one benchmark:

```bash
java -jar benchmarks/target/benchmarks-*.jar SegmentIndexGetBenchmark
```

Run a smoke profile:

```bash
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile segment-index-pr-smoke \
  --output-dir /tmp/hestia-bench/current
```

Compare two profile runs:

```bash
python3 benchmarks/scripts/compare_jmh_profile.py \
  --baseline /tmp/hestia-bench/base/summary.json \
  --candidate /tmp/hestia-bench/current/summary.json \
  --markdown-out /tmp/hestia-bench/comparison.md \
  --json-out /tmp/hestia-bench/comparison.json
```

Use [benchmarks/README.md](https://github.com/jajir/HestiaStore/blob/main/benchmarks/README.md)
for the full benchmark matrix and supported profiles.

## Profile a long-running workload

For profiling storage behavior, prefer a workload that runs long enough to make
allocation, locking, and I/O visible in a profiler.

- attach a profiler to the benchmark runner or to a representative integration
  test
- keep the workload stable across comparisons
- capture both CPU and allocation views when evaluating regressions

See [JVM Profiling](profiler-stacktrace.md) for profiler interpretation notes.

## Mockito and modules

Mockito may require reflective access to JDK internals in modular test setups:

```text
--add-opens=java.base/java.lang=ALL-UNNAMED
```

Apply the flag only where the test environment actually needs it.
