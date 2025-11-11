# ❓ Why HestiaStore

Choosing a storage engine is about fit. HestiaStore shines when you need a fast, embeddable key–value store with predictable file I/O, simple operations, and very large datasets. Understanding where it fits (and where it does not) helps you make confident decisions about performance, durability, and operational effort.

What to consider when assessing fit:

- Workload shape: write/read ratios, point lookups vs. scans, negative‑lookup rate
- Data volume: per‑segment size, total keys, growth expectations
- Durability model: flush/close boundaries, transactional file commits
- Concurrency: single‑writer vs. multi‑threaded access, iteration under mutation
- Operations: compaction windows, monitoring, backups, and recovery steps
- Platform constraints: pure‑Java, filesystem characteristics, containerization

Key resources to decide quickly:

- [Alternatives](alternatives.md) — quick comparison and when to prefer other engines.
- [Benchmarks (write)](out-write.md) — throughput while writing simple key–value pairs after warm‑up.
- [Benchmarks (read)](out-read.md) — random lookup throughput on a pre‑populated dataset.
- [Benchmarks (sequential read)](out-sequential.md) — forward scan throughput across keys in order.
- [Security](../SECURITY.md) — security policy, reporting, and handling guidance.
- [Quality & Testing](quality.md) — CI status, coverage, static analysis, and dependency checks.
