# Backup and Restore

Backups for HestiaStore are filesystem-level backups of the index directory plus
the operational procedure around them. This page describes practical strategies
that are easy to reason about.

## What must be backed up

Back up the whole index directory, including:

- segment data files
- index metadata
- lock-free closed-state contents
- the `wal/` directory when WAL is enabled

Do not back up only selected files from an active index and assume the result is
recoverable.

## Recommended backup strategies

### Best: cold backup

1. Stop writes.
2. Close the index cleanly.
3. Copy or snapshot the directory.
4. Reopen the index.

This is the simplest and lowest-risk procedure.

### Acceptable: coordinated snapshot

If you cannot fully stop the host process:

1. Quiesce application writes to the target index.
2. Call `flush()`.
3. Take a filesystem snapshot or directory copy.
4. Resume writes.

Use this only when the surrounding application can reliably coordinate the
pause window.

### After an unclean shutdown

Do not treat the first recovered post-crash state as your new clean backup
without validation.

1. Reopen the index.
2. Run `checkAndRepairConsistency()`.
3. If WAL is enabled, inspect the WAL directory with `wal_verify`.
4. Optionally run `compact()` if you want a cleaner on-disk layout.
5. Take a fresh backup only after the index is healthy again.

## Restore procedure

1. Restore the full directory to the target host or path.
2. Ensure no stale process is holding the `.lock` file.
3. Open the index with the expected configuration.
4. Run `checkAndRepairConsistency()`.
5. Perform spot-check reads on known keys.
6. Resume application traffic only after the checks pass.

## WAL-specific notes

- If WAL is enabled, restore the `wal/` directory together with the index data.
- Use `wal_verify` before reopening when the backup source or transport might
  have corrupted files.
- Prefer the [WAL Canary Runbook](wal-canary-runbook.md) before turning WAL on
  for critical indexes.

## Validation checklist

- The restored index opens successfully.
- `checkAndRepairConsistency()` completes without unrecoverable errors.
- Expected keys can be read.
- Monitoring shows healthy state after restore.
- A new backup window is scheduled after major repair or compaction work.
