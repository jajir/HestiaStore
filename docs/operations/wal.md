# WAL Operations

HestiaStore supports opt-in Write-Ahead Logging (WAL) per index.

## Enabling WAL

```java
Wal wal = Wal.builder()
    .withEnabled(true)
    .withDurabilityMode(WalDurabilityMode.GROUP_SYNC)
    .build();

IndexConfiguration<String, String> conf = IndexConfiguration
    .<String, String>builder()
    .withKeyClass(String.class)
    .withValueClass(String.class)
    .withName("orders")
    .withWal(wal)
    .build();
```

Default is disabled (`Wal.EMPTY`).

## Durability Modes

- `ASYNC`: append returns without waiting for fsync.
- `GROUP_SYNC`: append waits for group fsync (batched by delay/size).
- `SYNC`: append waits for immediate fsync per write.

## Corruption Policy

- `TRUNCATE_INVALID_TAIL`: startup truncates broken tail and continues.
- `FAIL_FAST`: startup aborts when corruption is detected.

## WAL Files

Inside index directory:

- `wal/format.meta` - WAL format marker and checksum
- `wal/checkpoint.meta` - monotonic checkpoint LSN
- `wal/*.wal` - WAL segments

## Tooling

`WalTool` supports:

- `verify` - checksum/structure validation and first-error location
- `dump` - per-segment summaries

Usage:

```bash
java -cp engine/target/classes org.hestiastore.index.segmentindex.wal.WalTool verify /path/to/index/wal
java -cp engine/target/classes org.hestiastore.index.segmentindex.wal.WalTool dump /path/to/index/wal
```

## Operational Signals

When retention pressure exceeds `maxBytesBeforeForcedCheckpoint`, write path triggers forced checkpoint and backpressure until retained WAL drops under threshold.
