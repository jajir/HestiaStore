# WAL Canary Runbook

This runbook defines a safe rollout process for enabling WAL on production indexes.

Scope:

- WAL is opt-in per index through the grouped `wal(...)` builder section.
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
sha256sum -c wal-tools-<version>.zip.sha256
unzip wal-tools-<version>.zip -d /opt/hestia/wal-tools
```

Concrete example:

```bash
VERSION=1.2.3
RELEASE_DIR=/srv/releases/hestiastore
WAL_TOOLS_DIR=/opt/hestia/wal-tools

cd "$RELEASE_DIR"
sha256sum -c "wal-tools-${VERSION}.zip.sha256"
unzip -o "wal-tools-${VERSION}.zip" -d "$WAL_TOOLS_DIR"
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
IndexConfiguration<String, String> conf = IndexConfiguration
    .<String, String>builder()
    .identity(identity -> identity
        .name("orders-canary")
        .keyClass(String.class)
        .valueClass(String.class))
    .wal(wal -> wal
        .durability(WalDurabilityMode.GROUP_SYNC)
        .segmentSizeBytes(64L * 1024L * 1024L)
        .groupSyncDelayMillis(5)
        .groupSyncMaxBatchBytes(1024 * 1024)
        .maxBytesBeforeForcedCheckpoint(512L * 1024L * 1024L)
        .corruptionPolicy(WalCorruptionPolicy.TRUNCATE_INVALID_TAIL))
    .build();
```

Open/create the canary index with this config.

### Phase 2 - Verify WAL health during rollout

Run WAL verification during rollout windows:

```bash
/opt/hestia/wal-tools/bin/wal_verify /path/to/index/wal
/opt/hestia/wal-tools/bin/wal_verify /path/to/index/wal --json
```

Concrete example:

```bash
WAL_VERIFY=/opt/hestia/wal-tools/bin/wal_verify
CANARY_WAL=/srv/hestia/indexes/orders-canary/wal

"$WAL_VERIFY" "$CANARY_WAL"
"$WAL_VERIFY" "$CANARY_WAL" --json
```

If verification fails (`exit code 2`), stop rollout and execute rollback.

Use dump for diagnostics:

```bash
/opt/hestia/wal-tools/bin/wal_dump /path/to/index/wal --json
```

Concrete example:

```bash
/opt/hestia/wal-tools/bin/wal_dump /srv/hestia/indexes/orders-canary/wal --json
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
    .identity(identity -> identity
        .name("orders-canary")
        .keyClass(String.class)
        .valueClass(String.class))
    .wal(wal -> wal.disabled())
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
