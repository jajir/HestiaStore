# Operations

Use this section once HestiaStore is already integrated and you need to run it
reliably in production-like environments.

## What belongs here

- WAL enablement and recovery
- monitoring and alerting
- tuning for memory, I/O, and maintenance behavior
- backup, restore, and post-crash validation
- offline export/import for migration and portability
- staged rollout and rollback procedures

## Recommended operating path

1. Start with [WAL](wal.md) if you need local crash recovery.
2. Add [Monitoring](monitoring.md) before broad rollout.
3. Read [Startup Memory Estimate](memory-estimate.md) when checking heap
   pressure during startup.
4. Use [Performance Tuning](tuning.md) only after you have workload evidence.
5. Define [Backup & Restore](backup-restore.md) before calling the system
   production-ready.
6. Add [Export & Import](export-import.md) when you need portable logical
   backups or migration tooling.
7. Use [WAL Canary Runbook](wal-canary-runbook.md) for staged WAL rollouts.

## After an unexpected shutdown

Recommended recovery sequence:

1. Reopen the index.
2. Run `checkAndRepairConsistency()`.
3. If WAL is enabled, inspect the WAL directory with `wal_verify`.
4. Run `compact()` if you need to restore locality and clean up fragmented
   layout.
5. Take a fresh backup after the system is healthy again.

## Related docs

- [Configuration](../configuration/index.md) for builder-level settings
- [Startup Memory Estimate](memory-estimate.md) for heap-pressure report
  assumptions
- [Monitoring Architecture](../architecture/monitoring/index.md) for internal
  monitoring design
- [Performance Model & Sizing](../architecture/segmentindex/performance.md) for
  implementation-level tuning context
- [Export & Import](export-import.md) for offline logical backup and migration
  flows
