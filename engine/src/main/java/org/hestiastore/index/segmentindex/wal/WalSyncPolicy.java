package org.hestiastore.index.segmentindex.wal;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies configured WAL durability boundaries and records sync failures.
 */
final class WalSyncPolicy {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(WalSyncPolicy.class);

    private final IndexWalConfiguration wal;
    private final WalStorage storage;
    private final WalRuntimeMetrics metrics;
    private final Object monitor;
    private final WalSegmentCatalog segmentCatalog;
    private final AtomicLong durableLsn = new AtomicLong(0L);
    private final Set<String> pendingSyncSegmentNames = new LinkedHashSet<>();

    private long pendingSyncHighLsn = 0L;
    private long pendingSyncBytes = 0L;
    private long pendingGroupSyncDeadlineNanos = 0L;
    private volatile RuntimeException syncFailure;

    WalSyncPolicy(final IndexWalConfiguration wal,
            final WalStorage storage, final WalRuntimeMetrics metrics,
            final Object monitor,
            final WalSegmentCatalog segmentCatalog) {
        this.wal = wal;
        this.storage = storage;
        this.metrics = metrics;
        this.monitor = monitor;
        this.segmentCatalog = segmentCatalog;
    }

    void resetAfterRecovery(final long maxLsn) {
        durableLsn.set(maxLsn);
        pendingSyncHighLsn = maxLsn;
        pendingSyncBytes = 0L;
        pendingSyncSegmentNames.clear();
        pendingGroupSyncDeadlineNanos = 0L;
        syncFailure = null;
    }

    void afterAppend(final long lsn, final int recordBytes,
            final String segmentName) {
        if (wal.isAsyncDurabilityMode()) {
            return;
        }
        if (pendingSyncSegmentNames.isEmpty()
                && wal.isGroupSyncDurabilityMode()
                && wal.getGroupSyncDelayMillis() > 0L) {
            pendingGroupSyncDeadlineNanos = System.nanoTime()
                    + TimeUnit.MILLISECONDS
                            .toNanos(wal.getGroupSyncDelayMillis());
        }
        pendingSyncHighLsn = Math.max(pendingSyncHighLsn, lsn);
        pendingSyncBytes += recordBytes;
        pendingSyncSegmentNames.add(segmentName);
        if (wal.isGroupSyncDurabilityMode()
                && (wal.getGroupSyncDelayMillis() <= 0
                        || pendingSyncBytes >= wal
                                .getGroupSyncMaxBatchBytes())) {
            syncGroupPendingLocked();
        }
    }

    /**
     * Applies the physical sync boundary after the append worker has written one
     * queue batch. Synchronous mode syncs every batch. Delayed group-sync mode
     * syncs when its pending deadline expires, including while the append queue
     * remains continuously busy.
     */
    void afterAppendBatch() {
        if (wal.isSyncDurabilityMode()
                || pendingGroupSyncDeadlineReached()) {
            syncGroupPendingLocked();
        }
    }

    /**
     * Returns how long the append worker may block before pending group-sync
     * records must be synchronized.
     *
     * @return positive wait in nanoseconds, or zero when no timed sync is pending
     */
    long pendingGroupSyncWaitNanosLocked() {
        if (syncFailure != null || pendingGroupSyncDeadlineNanos == 0L) {
            return 0L;
        }
        return Math.max(1L,
                pendingGroupSyncDeadlineNanos - System.nanoTime());
    }

    /**
     * Returns whether written-future completion must wait for the append-worker
     * batch sync boundary.
     *
     * @return true only for synchronous durability
     */
    boolean completesAtAppendBatchBoundary() {
        return wal.isSyncDurabilityMode();
    }

    /**
     * Synchronizes pending WAL data while the runtime monitor is held.
     */
    void syncGroupPendingLocked() {
        if (wal.isAsyncDurabilityMode()) {
            return;
        }
        checkSyncFailure();
        if (pendingSyncHighLsn <= durableLsn.get()) {
            pendingSyncBytes = 0L;
            pendingSyncSegmentNames.clear();
            pendingGroupSyncDeadlineNanos = 0L;
            return;
        }
        if (pendingSyncSegmentNames.isEmpty()) {
            pendingGroupSyncDeadlineNanos = 0L;
            return;
        }
        final long batchBytes = pendingSyncBytes;
        final long startedNanos = System.nanoTime();
        try {
            for (final String segmentName : pendingSyncSegmentNames) {
                storage.sync(segmentName);
            }
            if (segmentCatalog.hasPendingMetadataSync()) {
                storage.syncMetadata();
                segmentCatalog.markMetadataSynced();
            }
            durableLsn.set(pendingSyncHighLsn);
            pendingSyncBytes = 0L;
            pendingSyncSegmentNames.clear();
            pendingGroupSyncDeadlineNanos = 0L;
            metrics.recordSyncSuccess(System.nanoTime() - startedNanos,
                    batchBytes);
            monitor.notifyAll();
        } catch (RuntimeException ex) {
            markSyncFailure(ex);
            checkSyncFailure();
        }
    }

    /**
     * Flushes remaining synchronous work during runtime close.
     */
    void closeAndFlushPending() {
        if (wal.isAsyncDurabilityMode() || syncFailure != null) {
            return;
        }
        try {
            syncGroupPendingLocked();
        } catch (RuntimeException ignored) {
            // Failure was recorded by syncGroupPendingLocked().
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

    void waitUntilDurable(final long lsn) {
        if (wal.isAsyncDurabilityMode()) {
            return;
        }
        synchronized (monitor) {
            while (durableLsn.get() < lsn) {
                checkSyncFailure();
                try {
                    monitor.wait(Math.max(1L, wal.getGroupSyncDelayMillis()));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IndexException(
                            "Interrupted while waiting for WAL durability.", ex);
                }
            }
            checkSyncFailure();
        }
    }

    private void markSyncFailure(final RuntimeException ex) {
        if (syncFailure != null) {
            return;
        }
        syncFailure = ex;
        metrics.recordSyncFailure();
        LOGGER.error(
                "event=wal_sync_failure durableLsn={} pendingHighLsn={} pendingSyncBytes={} segmentCount={} syncFailureCount={}",
                durableLsn.get(), pendingSyncHighLsn, pendingSyncBytes,
                segmentCatalog.segments().size(), metrics.syncFailureCount(),
                ex);
        monitor.notifyAll();
    }

    private boolean pendingGroupSyncDeadlineReached() {
        return pendingGroupSyncDeadlineNanos != 0L
                && pendingGroupSyncDeadlineNanos - System.nanoTime() <= 0L;
    }
}
