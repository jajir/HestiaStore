package org.hestiastore.index.segmentindex.wal;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class WalSyncPolicy {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(WalSyncPolicy.class);

    private final IndexWalConfiguration wal;
    private final WalStorage storage;
    private final WalRuntimeMetrics metrics;
    private final Object monitor;
    private final WalSegmentCatalog segmentCatalog;
    private final AtomicBoolean closed;
    private final AtomicLong durableLsn = new AtomicLong(0L);
    private final Set<String> pendingSyncSegmentNames = new LinkedHashSet<>();

    private long pendingSyncHighLsn = 0L;
    private long pendingSyncBytes = 0L;
    private RuntimeException syncFailure;

    WalSyncPolicy(final IndexWalConfiguration wal,
            final WalStorage storage, final WalRuntimeMetrics metrics,
            final Object monitor,
            final WalSegmentCatalog segmentCatalog,
            final AtomicBoolean closed) {
        this.wal = wal;
        this.storage = storage;
        this.metrics = metrics;
        this.monitor = monitor;
        this.segmentCatalog = segmentCatalog;
        this.closed = closed;
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
        if (wal.isAsyncDurabilityMode()) {
            return;
        }
        pendingSyncHighLsn = Math.max(pendingSyncHighLsn, lsn);
        pendingSyncBytes += recordBytes;
        pendingSyncSegmentNames.add(segmentName);
        if (wal.isSyncDurabilityMode()
                || wal.getGroupSyncDelayMillis() <= 0
                || pendingSyncBytes >= wal.getGroupSyncMaxBatchBytes()) {
            syncGroupPendingLocked();
        }
    }

    void syncGroupPendingSafely() {
        synchronized (monitor) {
            if (closed.get()) {
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
        if (wal.isAsyncDurabilityMode()) {
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
            for (final WalSegmentDescriptor segment : segmentCatalog.segments()) {
                if (remaining.remove(segment.name())) {
                    storage.sync(segment.name());
                }
            }
            for (final String segmentName : remaining) {
                storage.sync(segmentName);
            }
            if (segmentCatalog.hasPendingMetadataSync()) {
                storage.syncMetadata();
                segmentCatalog.markMetadataSynced();
            }
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
        if (wal.isAsyncDurabilityMode() || syncFailure != null) {
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
        if (syncFailure == null) {
            syncFailure = ex;
        }
        metrics.recordSyncFailure();
        LOGGER.error(
                "event=wal_sync_failure durableLsn={} pendingHighLsn={} pendingSyncBytes={} segmentCount={} syncFailureCount={}",
                durableLsn.get(), pendingSyncHighLsn, pendingSyncBytes,
                segmentCatalog.segments().size(), metrics.syncFailureCount(),
                ex);
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }
}
