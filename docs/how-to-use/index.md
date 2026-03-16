# Getting Started

HestiaStore is a library, not a standalone server. The goal of this section is
to get you from dependency installation to a working embedded index with the
fewest decisions possible.

## Recommended path

1. [Install](install.md) the dependency from Maven Central.
2. Run the [Quick Start](quick-start.md) example with an in-memory directory.
3. Switch to a filesystem-backed directory when you need persistence.
4. Use [Configuration](../configuration/index.md) only after the basic path
   works.
5. Use [Operations](../operations/index.md) once WAL, monitoring, backup, or
   tuning become relevant.

## Typical use cases

- large local key-value datasets inside one Java service
- point lookups with predictable disk layout
- ordered scans over persisted keys
- test-friendly storage with `MemDirectory`

## When to choose something simpler

- all data fits comfortably in memory and persistence is not required
- your main need is SQL or transactional relational behavior
- you need distributed replication more than embedded storage
