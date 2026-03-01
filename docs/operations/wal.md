# WAL Operations

HestiaStore supports opt-in Write-Ahead Logging (WAL) per index.

For staged production rollout, use the dedicated
[WAL Canary Runbook](wal-canary-runbook.md).

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
- Recovery validates strict LSN monotonicity across the whole WAL stream (including segment boundaries); regressions are treated as corruption.
- Invalid `wal/checkpoint.meta` content (non-numeric or negative LSN) aborts recovery.

## WAL Files

Inside index directory:

- `wal/format.meta` - WAL format marker and checksum
- `wal/checkpoint.meta` - monotonic checkpoint LSN
- `wal/*.wal` - WAL segments

## Tooling

`WalTool` supports:

- `verify` - validates format/checkpoint metadata, strict segment naming, checksum/record structure, global LSN monotonicity, and checkpoint <= max WAL LSN
- `dump` - prints per-record metadata, invalid-tail markers, and per-segment summary

Usage:

```bash
java -cp engine/target/classes org.hestiastore.index.segmentindex.wal.WalTool verify /path/to/index/wal
java -cp engine/target/classes org.hestiastore.index.segmentindex.wal.WalTool dump /path/to/index/wal
java -cp engine/target/classes org.hestiastore.index.segmentindex.wal.WalTool verify /path/to/index/wal --json
java -cp engine/target/classes org.hestiastore.index.segmentindex.wal.WalTool dump /path/to/index/wal --json

# packaged CLI distribution (wal-tools module)
mvn -pl wal-tools -am package
unzip wal-tools/target/wal-tools-<version>.zip -d /tmp
/tmp/wal-tools-<version>/bin/wal_verify /path/to/index/wal
/tmp/wal-tools-<version>/bin/wal_dump /path/to/index/wal
/tmp/wal-tools-<version>/bin/wal_verify /path/to/index/wal --json
/tmp/wal-tools-<version>/bin/wal_dump /path/to/index/wal --json
```

`verify` output fields:

- `verify.ok`, `verify.files`, `verify.records`, `verify.maxLsn`
- On failure: `verify.errorFile`, `verify.errorOffset`, `verify.errorMessage`

`--json` output:

- `verify` prints one JSON object with stable fields:
  - `type`, `ok`, `files`, `records`, `maxLsn`, `errorFile`, `errorOffset`, `errorMessage`
- `dump` prints JSON lines with:
  - `type=record`: `file`, `offset`, `lsn`, `op`, `keyLen`, `valueLen`, `bodyLen`
  - `type=invalid_tail`: `file`, `offset`, `reason`
  - `type=summary`: `file`, `size`, `records`, `firstLsn`, `lastLsn`

`WalTool` exit codes:

- `0` - command succeeded (`verify.ok=true` for verify)
- `1` - usage/runtime failure
- `2` - verify completed but detected WAL issues (`verify.ok=false`)

## Operational Signals

When retention pressure exceeds `maxBytesBeforeForcedCheckpoint`, write path triggers forced checkpoint and backpressure until retained WAL drops under threshold.

Notes:

- Backpressure applies only when WAL contains more than one segment (active + at least one sealed segment).
- A single active segment is never deleted by checkpoint; this avoids an unsatisfiable backpressure loop when thresholds are configured below active-segment footprint.
- Recovery/cleanup emits stable structured log events for parsing:
  - `event=wal_recovery_start`
  - `event=wal_recovery_invalid_tail`
  - `event=wal_recovery_tail_repair`
  - `event=wal_recovery_drop_newer_segments`
  - `event=wal_recovery_checkpoint_clamp`
  - `event=wal_recovery_complete`
  - `event=wal_checkpoint_cleanup`

## Metrics

`SegmentIndex.metricsSnapshot()` exposes WAL runtime counters and gauges:

- Throughput: `getWalAppendCount()`, `getWalAppendBytes()`
- Durability: `getWalSyncCount()`, `getWalSyncFailureCount()`, `getWalDurableLsn()`
- Recovery/corruption: `getWalCorruptionCount()`, `getWalTruncationCount()`
- Retention/checkpoint: `getWalRetainedBytes()`, `getWalSegmentCount()`, `getWalCheckpointLsn()`, `getWalCheckpointLagLsn()`
- Pending work: `getWalPendingSyncBytes()`, `getWalAppliedLsn()`
- Sync latency and batch sizing:
  - `getWalSyncTotalNanos()`, `getWalSyncMaxNanos()`, `getWalSyncAvgNanos()`
  - `getWalSyncBatchBytesTotal()`, `getWalSyncBatchBytesMax()`, `getWalSyncAvgBatchBytes()`
