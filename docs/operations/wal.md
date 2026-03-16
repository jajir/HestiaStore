# WAL

HestiaStore supports opt-in write-ahead logging per index. WAL is the main
feature to enable when you need local crash recovery stronger than flush/close
boundaries alone.

For staged rollout, use [WAL Canary Runbook](wal-canary-runbook.md). For
longer-term replication design work, see
[WAL Replication and Fencing Design](../development/wal-replication-fencing-design.md).

## Enable WAL

```java
Wal wal = Wal.builder()
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

WAL is disabled by default through `Wal.EMPTY`.

## Choose a durability mode

- `ASYNC`: lowest write latency, weakest durability guarantee
- `GROUP_SYNC`: batched fsync behavior with a balanced latency/durability trade-off
- `SYNC`: fsync on each write, strongest durability and highest write overhead

Choose the mode from your durability target first, then tune performance around
that choice.

## Corruption and recovery policy

- `TRUNCATE_INVALID_TAIL`: startup truncates a broken tail and continues
- `FAIL_FAST`: startup stops when corruption is detected
- Recovery validates global LSN monotonicity across WAL segments
- Invalid `wal/checkpoint.meta` content aborts recovery

## WAL directory layout

Inside the index directory:

- `wal/format.meta`
- `wal/checkpoint.meta`
- `wal/*.wal`

WAL segment files are named as `<20-digit-base-lsn>.wal`.

## Tooling

`WalTool` supports:

- `verify` for integrity checks
- `dump` for record-level diagnostics

Run it from compiled classes:

```bash
java -cp engine/target/classes org.hestiastore.index.segmentindex.wal.WalTool verify /path/to/index/wal
java -cp engine/target/classes org.hestiastore.index.segmentindex.wal.WalTool dump /path/to/index/wal
```

Or use the packaged CLI:

```bash
mvn -pl wal-tools -am package
unzip wal-tools/target/wal-tools-<version>.zip -d /tmp
/tmp/wal-tools-<version>/bin/wal_verify /path/to/index/wal
/tmp/wal-tools-<version>/bin/wal_dump /path/to/index/wal
```

JSON output is available through `--json` for both commands.

Exit codes:

- `0`: success
- `1`: usage or runtime failure
- `2`: `verify` found WAL issues

## Operating signals

Monitor these first:

- `getWalSyncFailureCount()`
- `getWalCorruptionCount()`
- `getWalTruncationCount()`
- `getWalRetainedBytes()`
- `getWalCheckpointLagLsn()`
- `getWalPendingSyncBytes()`
- `getWalSyncAvgNanos()`

When retained WAL exceeds `maxBytesBeforeForcedCheckpoint`, the write path
applies forced checkpoint behavior and backpressure until retained WAL drops.

Structured log events include:

- `event=wal_recovery_start`
- `event=wal_recovery_invalid_tail`
- `event=wal_recovery_tail_repair`
- `event=wal_recovery_drop_newer_segments`
- `event=wal_recovery_checkpoint_clamp`
- `event=wal_recovery_complete`
- `event=wal_checkpoint_cleanup`
- `event=wal_retention_pressure_start`
- `event=wal_retention_pressure_cleared`
- `event=wal_sync_failure`
- `event=wal_sync_failure_transition`

## Metrics exposed by `metricsSnapshot()`

- throughput: `getWalAppendCount()`, `getWalAppendBytes()`
- durability: `getWalSyncCount()`, `getWalSyncFailureCount()`, `getWalDurableLsn()`
- corruption and recovery: `getWalCorruptionCount()`, `getWalTruncationCount()`
- retention and checkpointing:
  `getWalRetainedBytes()`, `getWalSegmentCount()`, `getWalCheckpointLsn()`,
  `getWalCheckpointLagLsn()`
- pending work: `getWalPendingSyncBytes()`, `getWalAppliedLsn()`
- sync latency and batch sizing:
  `getWalSyncTotalNanos()`, `getWalSyncMaxNanos()`, `getWalSyncAvgNanos()`,
  `getWalSyncBatchBytesTotal()`, `getWalSyncBatchBytesMax()`,
  `getWalSyncAvgBatchBytes()`
