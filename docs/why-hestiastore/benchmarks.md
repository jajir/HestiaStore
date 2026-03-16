# Benchmarks

This page summarizes the benchmark material that helps evaluate HestiaStore.
Use it to understand workload shape and relative trade-offs before going deeper
into architecture or tuning.

## What the benchmark pages are for

- Compare HestiaStore against alternative embedded engines under a consistent
  local benchmark harness.
- Show workload direction, not absolute guarantees.
- Provide enough methodology to judge whether a result is relevant for your
  production workload.

## Write throughput

![Write benchmark comparison](../images/out-write.svg)

What this view is useful for:

- estimating ingestion headroom for append-heavy local workloads
- comparing compression and filtering overhead against raw write throughput
- spotting whether your expected workload is closer to log-structured or
  low-latency point-write engines

Detailed page:
[Write Throughput Results](out-write.md)

## Random read throughput

![Random read benchmark comparison](../images/out-read.svg)

What this view is useful for:

- estimating point-lookup behavior on pre-populated datasets
- comparing negative-lookup friendly designs with disk-backed structures
- understanding where HestiaStore fits relative to memory-mapped or native
  storage engines

Detailed page:
[Random Read Results](out-read.md)

## Sequential read throughput

![Sequential read benchmark comparison](../images/out-sequential.svg)

What this view is useful for:

- evaluating scan-heavy or export-style workloads
- comparing sparse-index and iterator performance across engines
- checking whether your main pressure is scan throughput instead of random
  access latency

Detailed page:
[Sequential Read Results](out-sequential.md)

## Methodology summary

- All benchmark suites run with the same JVM, hardware, and dataset shape for a
  given comparison.
- Benchmarks use warm-up plus sustained measurement windows; treat the results
  as relative comparisons, not vendor-style peak numbers.
- Storage size, CPU use, and confidence intervals matter as much as raw
  throughput.
- If your workload mixes scans, hot reads, WAL durability, and background
  maintenance, validate with your own representative benchmark before drawing
  conclusions.

## How to read the results

- Prefer the chart direction and rank ordering over small score deltas.
- Use the [Alternatives](alternatives.md) page to turn benchmark results into a
  fit decision.
- Use [Performance Model & Sizing](../architecture/segmentindex/performance.md)
  if HestiaStore is already selected and you need tuning guidance.

## Detailed results and raw artifacts

Detailed benchmark pages stay inside the `Evaluate` section so they can grow as
new benchmark suites are added:

- [Write Throughput Results](out-write.md)
- [Random Read Results](out-read.md)
- [Sequential Read Results](out-sequential.md)

Raw benchmark artifacts and compare tooling remain in the repository:

- [Benchmarks module README](https://github.com/jajir/HestiaStore/blob/main/benchmarks/README.md)
- [Benchmark history and comparisons](https://github.com/jajir/HestiaStore/blob/main/benchmarks/benchmark-history.md)
