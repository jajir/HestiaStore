# WAL Canary Runbook

This runbook defines a safe rollout process for enabling WAL on production indexes.

Scope:

- WAL is opt-in per index (`withWal(...)`).
- Default remains disabled (`Wal.EMPTY`).
- One WAL lives inside each index directory (`<index>/wal`).

## Goals

- Enable WAL on a small subset of indexes first.
- Detect durability/performance regressions early.
- Roll back quickly to `Wal.EMPTY` when risk signals appear.

## Preconditions

1. Current backup/restore flow is verified for the target index set.
2. `wal-tools` distribution is available for verification:

```bash
mvn -pl wal-tools -am -DskipTests package
unzip wal-tools/target/wal-tools-<version>.zip -d /tmp
```

3. Monitoring is collecting `SegmentIndex.metricsSnapshot()` WAL fields.
4. Target indexes for canary are chosen (low business criticality first).

## Canary Plan

### Phase 0 - Baseline (WAL disabled)

Duration: 24h minimum on target indexes.

Collect baseline:

- write latency
- `getWalPendingSyncBytes()` (should be 0 when disabled)
- `getWalSyncAvgNanos()` (should be 0 when disabled)
- index throughput

### Phase 1 - Enable WAL on canary indexes only

Use explicit WAL config:

```java
Wal wal = Wal.builder()
    .withEnabled(true)
    .withDurabilityMode(WalDurabilityMode.GROUP_SYNC)
    .withSegmentSizeBytes(64L * 1024L * 1024L)
    .withGroupSyncDelayMillis(5L)
    .withGroupSyncMaxBatchBytes(1L * 1024L * 1024L)
    .withMaxBytesBeforeForcedCheckpoint(512L * 1024L * 1024L)
    .withCorruptionPolicy(WalCorruptionPolicy.TRUNCATE_INVALID_TAIL)
    .build();

IndexConfiguration<String, String> conf = IndexConfiguration
    .<String, String>builder()
    .withKeyClass(String.class)
    .withValueClass(String.class)
    .withName("orders-canary")
    .withWal(wal)
    .build();
```

Open/create the canary index with this config.

### Phase 2 - Verify WAL health during rollout

Run WAL verification during rollout windows:

```bash
/tmp/wal-tools-<version>/bin/wal_verify /path/to/index/wal
/tmp/wal-tools-<version>/bin/wal_verify /path/to/index/wal --json
```

If verification fails (`exit code 2`), stop rollout and execute rollback.

Use dump for diagnostics:

```bash
/tmp/wal-tools-<version>/bin/wal_dump /path/to/index/wal --json
```

### Phase 3 - Expand rollout

Expand only if canary passes acceptance criteria for at least 24h.

Recommended expansion:

1. 5% of indexes
2. 25% of indexes
3. 100% of eligible indexes

Pause one full observation window between stages.

## Alert Thresholds

Use these as initial operational thresholds (tune by workload).

| Signal | Warning | Rollback Trigger |
|---|---|---|
| `getWalSyncFailureCount()` | any increase | immediate rollback |
| `getWalCorruptionCount()` | any increase | immediate rollback |
| `getWalTruncationCount()` | any increase outside planned restart | immediate rollback |
| `getWalRetainedBytes()` | > 80% of `maxBytesBeforeForcedCheckpoint` for 10m | > 100% for 10m |
| `getWalCheckpointLagLsn()` | continuously increasing for 10m | increasing for 30m with no stabilization |
| `getWalPendingSyncBytes()` | sustained growth for 10m | sustained growth for 30m |
| `getWalSyncAvgNanos()` | > 2x baseline for 15m | > 4x baseline for 15m |

Critical signals (`sync failure`, `corruption`, `unexpected truncation`) are fail-fast.

## Rollback Procedure (to `Wal.EMPTY`)

1. Stop traffic to affected canary indexes (or switch to read-only).
2. Take a filesystem backup/snapshot of affected index directories.
3. Reopen indexes with WAL disabled override:

```java
IndexConfiguration<String, String> rollbackConf = IndexConfiguration
    .<String, String>builder()
    .withKeyClass(String.class)
    .withValueClass(String.class)
    .withName("orders-canary")
    .withWal(Wal.EMPTY)
    .build();

try (SegmentIndex<String, String> index = SegmentIndex.open(directory, rollbackConf)) {
    index.flushAndWait();
}
```

4. Run integrity checks:
   - `index.checkAndRepairConsistency()`
   - point-read spot checks on business keys
5. Keep `wal/` files for incident forensics until postmortem is complete.
6. Resume traffic only after checks pass.

## Canary Acceptance Criteria

Promote to next stage only when all are true:

1. No increase in `getWalSyncFailureCount()`.
2. No increase in `getWalCorruptionCount()`.
3. No unexpected `getWalTruncationCount()` increments.
4. `getWalRetainedBytes()` remains below forced-checkpoint threshold with headroom.
5. Write latency SLO remains within agreed variance from baseline.

## Incident Data to Capture

For any rollback-triggering event, capture:

1. `wal_verify --json` output
2. `wal_dump --json` output around the failing segment
3. index runtime metrics snapshot around the event window
4. precise software version and commit hash
