# WAL Replication and Fencing Design (Phase 6 Prep)

This document defines the design contract for Phase 6:

- WAL log shipping (leader -> replicas)
- epoch/term-based fencing
- replicated durability acknowledgement modes

It is a preparation artifact: implementation can proceed without reopening core design decisions.

## Status

- Current WAL v1 scope is local durability only.
- This document defines the extension path to distributed durability.

## Goals

1. Replicate WAL records by LSN range to follower nodes.
2. Prevent stale leaders/writers from appending after leadership changes.
3. Provide explicit durability levels: local-only vs replicated acknowledgements.
4. Keep backward compatibility for existing local-WAL deployments.

## Non-Goals

1. Building a new consensus algorithm inside HestiaStore.
2. Global multi-index distributed transactions.
3. Cross-region conflict resolution in v1 replication.

Leader election is assumed to be provided by external cluster coordination.

## Locked Assumptions

1. One WAL per index directory (`<index>/wal`).
2. WAL remains opt-in (`withWal(...)`), default `Wal.EMPTY`.
3. Local durability behavior must remain unchanged when replication is disabled.
4. WAL v1 record framing/checksum rules stay unchanged; replication metadata uses extension fields.

## Replication Model

For one index:

1. Exactly one leader is allowed to append at a time.
2. Followers accept leader WAL records in LSN order.
3. Followers verify record checksum/structure before append.
4. Followers expose `lastReplicatedLsn` and `lastDurableLsn`.

## WAL Header Extension Contract

Use reserved header extension area (already planned via `epochSupport`):

1. `epoch` (or `term`) - monotonically increasing leadership epoch.
2. `sourceNodeId` - stable identifier of leader node.
3. `flags` bit for replication metadata presence.

Rules:

1. `epochSupport=false` keeps extension disabled and local behavior unchanged.
2. When replication is enabled, `epochSupport=true` is mandatory.
3. Records with missing required replication metadata are rejected in replicated mode.

## Fencing Contract

1. Every append request must include current leader epoch.
2. Runtime stores `currentEpoch` durably.
3. Append is rejected when request epoch < stored epoch.
4. On epoch bump:
   - new epoch is persisted before accepting writes
   - previous leader/writer sessions are invalidated

Failure behavior:

1. If epoch persistence fails, index enters error state and rejects new writes.
2. No write acknowledgement is allowed after failed epoch transition.

## Log Shipping Protocol (Minimal v1)

Logical RPC-level contract:

1. `Fetch(fromLsn, maxBytes)` -> stream/list of WAL records.
2. `AppendReplicated(records, epoch, sourceNodeId)` -> follower append result.
3. `Ack(lastDurableLsn, epoch)` -> follower durable progress.

Follower validation on `AppendReplicated`:

1. Epoch must match expected leader epoch.
2. First record LSN must be contiguous with follower WAL tail (or recovery point).
3. Each record checksum and framing must be valid.
4. Invalid tail handling follows configured corruption policy.

## Durability Acknowledgement Modes (Replication-Aware)

Keep existing local modes and add replicated interpretation:

1. `LOCAL_ASYNC`: local append only.
2. `LOCAL_GROUP_SYNC`: local group fsync only.
3. `LOCAL_SYNC`: local sync per write.
4. `REPLICATED_QUORUM`: acknowledge after local durability + replica quorum durable ack.
5. `REPLICATED_ALL`: acknowledge after all in-sync replicas durable ack.

Initial rollout recommendation:

- Enable replication with `LOCAL_GROUP_SYNC` first (observe),
- then promote selected indexes to `REPLICATED_QUORUM`.

## Recovery and Failover Rules

1. New leader must not serve writes until:
   - epoch bump persisted
   - log position selected from acknowledged durable LSN boundary
2. Followers that are ahead of chosen leader point must truncate to leader boundary.
3. Safe-tail truncation rules remain the same as local WAL recovery.

## Compatibility and Migration

1. Existing indexes with `wal.enabled=false` remain unaffected.
2. Existing WAL-enabled local indexes can stay local (`epochSupport=false`).
3. Replication enablement is explicit configuration migration:
   - set replication settings
   - set `epochSupport=true`
4. Downgrade path:
   - disable replication mode
   - keep local WAL enabled

## Observability Requirements

Add replication metrics:

1. `walReplicationSentRecords`, `walReplicationSentBytes`
2. `walReplicationLagLsn` (leader durable LSN - follower durable LSN)
3. `walReplicationAckLatencyNanos` (histogram)
4. `walFencingRejectCount`
5. `walEpoch`
6. `walReplicaInSyncCount`

Add structured events:

1. `event=wal_epoch_bump`
2. `event=wal_fencing_reject`
3. `event=wal_replication_append_reject`
4. `event=wal_replication_lag_threshold_exceeded`

## Security Requirements

1. Replication transport must support TLS.
2. Node identity must be authenticated before accepting replicated append.
3. Epoch transition operations must be auditable.
4. Do not disable checksum validation for replicated traffic.

## Phase 6 Implementation Plan (Execution Order)

### P6.1 Metadata and configuration

1. Finalize config schema for replication and epoch controls.
2. Add validation rules (`epochSupport=true` required when replication enabled).
3. Add manifest read/write compatibility tests.

### P6.2 Fencing core

1. Persist and enforce epoch in WAL runtime.
2. Reject stale epoch writes.
3. Add unit tests for epoch transitions and stale writer rejection.

### P6.3 Log shipping channel

1. Implement WAL record fetch by LSN range.
2. Implement follower append endpoint with strict validation.
3. Add integration tests for contiguous/invalid stream behavior.

### P6.4 Ack and durability policy

1. Implement replica ack tracking.
2. Add `REPLICATED_QUORUM` ack gating.
3. Add tests for ack loss and timeout/fallback behavior.

### P6.5 Failover correctness

1. Leader handoff with epoch bump and boundary selection.
2. Deterministic truncation of divergent tails on followers.
3. Add failover simulation tests (leader crash, partition, rejoin).

### P6.6 Operational hardening

1. Metrics, logs, and alerts for lag/fencing.
2. Runbook for replication incident handling.
3. Canary rollout with guarded enablement.

## Acceptance Criteria for Phase 6

1. No stale leader writes accepted after epoch change.
2. Deterministic failover replay from acknowledged durable boundary.
3. Replicated durability modes match documented acknowledgement semantics.
4. WAL corruption/tail-repair behavior remains safe under replication.
