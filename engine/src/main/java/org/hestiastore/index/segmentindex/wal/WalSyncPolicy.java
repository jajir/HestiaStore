package org.hestiastore.index.segmentindex.wal;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentindex.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.WalDurabilityMode;
import org.slf4j.Logger;

final class WalSyncPolicy {

    private final IndexWalConfiguration wal;
    private final WalStorage storage;
    private final WalRuntimeMetrics metrics;
    private final Logger logger;
    private final Object monitor;
    private final Supplier<List<WalSegmentDescriptor>> segmentsSupplier;
    private final BooleanSupplier closedSupplier;
    private final AtomicLong durableLsn = new AtomicLong(0L);
    private final Set<String> pendingSyncSegmentNames = new LinkedHashSet<>();

    private long pendingSyncHighLsn = 0L;
    private long pendingSyncBytes = 0L;
    private RuntimeException syncFailure;

    WalSyncPolicy(final IndexWalConfiguration wal, final WalStorage storage,
            final WalRuntimeMetrics metrics, final Logger logger,
            final Object monitor,
            final Supplier<List<WalSegmentDescriptor>> segmentsSupplier,
            final BooleanSupplier closedSupplier) {
        this.wal = wal;
        this.storage = storage;
        this.metrics = metrics;
        this.logger = logger;
        this.monitor = monitor;
        this.segmentsSupplier = segmentsSupplier;
        this.closedSupplier = closedSupplier;
    }

    void resetAfterRecovery(final long maxLsn) {
        durableLsn.set(maxLsn);
        pendingSyncHighLsn = maxLsn;
        pendingSyncBytes = 0L;
        pendingSyncSegmentNames.clear();
        syncFailure = null;
    }

    void afterAppend(final long lsn, final int recordBytes,
            final String segmentName) {
        if (wal.getDurabilityMode() == WalDurabilityMode.ASYNC) {
            return;
        }
        pendingSyncHighLsn = Math.max(pendingSyncHighLsn, lsn);
        pendingSyncBytes += recordBytes;
        pendingSyncSegmentNames.add(segmentName);
        if (wal.getDurabilityMode() == WalDurabilityMode.SYNC
                || wal.getGroupSyncDelayMillis() <= 0
                || pendingSyncBytes >= wal.getGroupSyncMaxBatchBytes()) {
            syncGroupPendingLocked();
        }
        if (wal.getDurabilityMode() == WalDurabilityMode.GROUP_SYNC) {
            waitUntilDurableLocked(lsn);
        }
    }

    void syncGroupPendingSafely() {
        synchronized (monitor) {
            if (closedSupplier.getAsBoolean()) {
                return;
            }
            try {
                syncGroupPendingLocked();
            } catch (RuntimeException ex) {
                markSyncFailure(ex);
            }
        }
    }

    void syncGroupPendingLocked() {
        if (wal.getDurabilityMode() == WalDurabilityMode.ASYNC) {
            return;
        }
        checkSyncFailure();
        if (pendingSyncHighLsn <= durableLsn.get()) {
            pendingSyncBytes = 0L;
            pendingSyncSegmentNames.clear();
            return;
        }
        if (pendingSyncSegmentNames.isEmpty()) {
            return;
        }
        final long batchBytes = pendingSyncBytes;
        final long startedNanos = System.nanoTime();
        try {
            final Set<String> remaining = new HashSet<>(pendingSyncSegmentNames);
            for (final WalSegmentDescriptor segment : segmentsSupplier.get()) {
                if (remaining.remove(segment.name())) {
                    storage.sync(segment.name());
                }
            }
            for (final String segmentName : remaining) {
                storage.sync(segmentName);
            }
            storage.syncMetadata();
            durableLsn.set(pendingSyncHighLsn);
            pendingSyncBytes = 0L;
            pendingSyncSegmentNames.clear();
            metrics.recordSyncSuccess(System.nanoTime() - startedNanos,
                    batchBytes);
            synchronized (monitor) {
                monitor.notifyAll();
            }
        } catch (RuntimeException ex) {
            markSyncFailure(ex);
            checkSyncFailure();
        }
    }

    void closeAndFlushPending() {
        if (wal.getDurabilityMode() == WalDurabilityMode.ASYNC
                || syncFailure != null) {
            return;
        }
        try {
            syncGroupPendingLocked();
        } catch (RuntimeException ex) {
            markSyncFailure(ex);
        }
    }

    void checkSyncFailure() {
        if (syncFailure != null) {
            throw new IndexException("WAL sync failure", syncFailure);
        }
    }

    boolean hasSyncFailure() {
        return syncFailure != null;
    }

    long durableLsn() {
        return durableLsn.get();
    }

    long pendingSyncBytes() {
        return pendingSyncBytes;
    }

    private void waitUntilDurableLocked(final long lsn) {
        while (durableLsn.get() < lsn) {
            checkSyncFailure();
            if (closedSupplier.getAsBoolean()) {
                throw new IndexException(
                        "WAL runtime closed while waiting for durability.");
            }
            try {
                synchronized (monitor) {
                    monitor.wait(Math.max(1L, wal.getGroupSyncDelayMillis()));
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IndexException(
                        "Interrupted while waiting for group WAL sync.", ex);
            }
        }
    }

    private void markSyncFailure(final RuntimeException ex) {
        if (syncFailure == null) {
            syncFailure = ex;
        }
        metrics.recordSyncFailure();
        logger.error(
                "event=wal_sync_failure durableLsn={} pendingHighLsn={} pendingSyncBytes={} segmentCount={} syncFailureCount={}",
                durableLsn.get(), pendingSyncHighLsn, pendingSyncBytes,
                segmentsSupplier.get().size(), metrics.syncFailureCount(), ex);
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }
}
