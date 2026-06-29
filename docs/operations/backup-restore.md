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

Concrete example:

```bash
APP_SERVICE=hestia-orders
SOURCE_INDEX=/srv/hestia/indexes/orders
BACKUP_ROOT=/srv/hestia/fs-backups
BACKUP_DIR="$BACKUP_ROOT/orders-2026-04-21"

sudo systemctl stop "$APP_SERVICE"
mkdir -p "$BACKUP_DIR"
rsync -a --delete "$SOURCE_INDEX/" "$BACKUP_DIR/"
sudo systemctl start "$APP_SERVICE"
```

If you need a portable logical artifact instead of a raw directory copy, use
[Export & Import](export-import.md).

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

Concrete example with WAL verification:

```bash
RESTORE_INDEX=/srv/hestia/indexes/orders
WAL_VERIFY=/opt/hestiastore/wal-tools/bin/wal_verify

"$WAL_VERIFY" "$RESTORE_INDEX/wal" --json
```

## Restore procedure

1. Restore the full directory to the target host or path.
2. Ensure no stale process is holding the `.lock` file.
3. Open the index with the expected configuration.
4. Run `checkAndRepairConsistency()`.
5. Perform spot-check reads on known keys.
6. Resume application traffic only after the checks pass.

Concrete filesystem restore example:

```bash
BACKUP_DIR=/srv/hestia/fs-backups/orders-2026-04-21
RESTORE_INDEX=/srv/hestia-restored/indexes/orders

mkdir -p "$RESTORE_INDEX"
rsync -a --delete "$BACKUP_DIR/" "$RESTORE_INDEX/"
```

If you need to restore into a newly created index with edited configuration or
move data into another system, prefer [Export & Import](export-import.md)
instead of raw directory restore.

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

Simple operator flow:

```bash
APP_SERVICE=hestia-orders-restore
BACKUP_DIR=/srv/hestia/fs-backups/orders-2026-04-21
RESTORE_INDEX=/srv/hestia-restored/indexes/orders
WAL_VERIFY=/opt/hestiastore/wal-tools/bin/wal_verify

mkdir -p "$RESTORE_INDEX"
rsync -a --delete "$BACKUP_DIR/" "$RESTORE_INDEX/"
"$WAL_VERIFY" "$RESTORE_INDEX/wal" --json
sudo systemctl start "$APP_SERVICE"
```
