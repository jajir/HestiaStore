# Why HestiaStore

Use this section to decide whether HestiaStore matches your workload before you
commit to integration or tuning work.

## Choose HestiaStore when you need

- an embeddable Java library instead of a separate database service
- large local datasets with bounded-memory point lookups
- ordered iteration or range-like scans over persisted keys
- operationally simple storage in a single directory
- optional WAL-based crash recovery without adding a separate coordinator

## Reconsider if you need

- distributed replication and failover
- SQL, joins, and relational query planning
- multi-key transactional semantics
- a storage engine optimized primarily for ultra-low-latency in-memory access
- a drop-in replacement for high-write native LSM engines without validating
  your workload first

## Decision checklist

- Workload shape: mostly point reads, scans, ingestion bursts, or mixed traffic
- Durability: flush/close boundaries only, or WAL-backed recovery as well
- Operations: backup windows, recovery expectations, and monitoring maturity
- Footprint: local disk budget, cache budget, and acceptable compaction work
- Platform: Java version, container or host filesystem constraints, and whether
  native dependencies are acceptable

## Read next

- [Alternatives](alternatives.md) for fit trade-offs against other engines
- [Benchmarks](benchmarks.md) for relative workload comparisons
- [Quality & Testing](quality.md) for CI, coverage, and validation signals
- [Security](../SECURITY.md) for reporting and threat posture
- [Getting Started](../how-to-use/index.md) if the fit already looks right
