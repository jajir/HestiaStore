# HestiaStore WAL Tools Distribution

This module packages command-line WAL tooling as a standalone zip distribution.

## Included commands

- `bin/wal_verify` - verify WAL metadata/records and report corruption
- `bin/wal_dump` - dump WAL record metadata and segment summaries

Both commands are wrappers around:

- `org.hestiastore.index.segmentindex.wal.WalTool`

## Build

```bash
mvn -pl wal-tools -am package
```

Distribution artifact:

- `wal-tools/target/wal-tools-<version>.zip`
