# Performance Tuning

Tune HestiaStore with workload data, not by turning every knob at once. Start
from a correct baseline, measure, then change one group of settings at a time.

## Tune in this order

1. Verify the basic configuration and directory choice are correct.
2. Measure read latency, write latency, cache hit behavior, and WAL overhead.
3. Adjust one configuration family at a time.
4. Re-measure before keeping a change.

## High-impact tuning areas

### Segment sizing

`withMaxNumberOfKeysInSegment()` controls when segments split. Larger segments
reduce split frequency but can increase maintenance cost and read work within a
segment.

Use larger values when:

- workloads are write-heavy
- segment churn is too high
- you can tolerate larger compaction work units

Use smaller values when:

- read locality is suffering
- split and maintenance work is too bursty
- recovery or rebuild windows need smaller units

### Cache sizing

- `withMaxNumberOfSegmentsInCache()` controls index-level segment residency
- `withMaxNumberOfKeysInSegmentCache()` controls per-segment cached data

Increase cache budgets when cache misses or repeated disk reads dominate.
Reduce them when memory pressure hurts the rest of the application more than the
saved I/O helps.

### Sparse index granularity

`withMaxNumberOfKeysInSegmentIndexPage()` changes how coarse or fine the
segment-level sparse index is.

- Smaller pages improve seek precision but increase index overhead.
- Larger pages reduce index overhead but can increase scan work per lookup.

### Bloom filters

Bloom filters mainly help negative lookups.

- Enable and size them when misses are common.
- Disable them when misses are rare or memory is tight.
- Measure false-positive behavior before assuming the defaults are wrong.

### WAL overhead

If WAL is enabled:

- monitor `getWalSyncAvgNanos()`, `getWalPendingSyncBytes()`, and
  `getWalRetainedBytes()`
- compare `ASYNC`, `GROUP_SYNC`, and `SYNC` only against your durability target
- use the canary rollout before enabling WAL broadly

See [WAL](wal.md) and [WAL Canary Runbook](wal-canary-runbook.md).

## Operating signals to watch while tuning

- read and write latency percentiles
- registry cache hit and miss counts
- partition buffer growth and throttle counts
- WAL sync failures, pending bytes, and checkpoint lag
- compaction frequency and recovery time after restart

## Avoid these mistakes

- tuning from synthetic intuition instead of measured workload data
- changing segment sizing and cache settings together without an intermediate
  baseline
- enabling expensive logging everywhere while measuring storage performance
- assuming benchmark results replace production profiling

## Deep-dive references

- [Configuration](../configuration/index.md)
- [Monitoring](monitoring.md)
- [Performance Model & Sizing](../architecture/segmentindex/performance.md)
