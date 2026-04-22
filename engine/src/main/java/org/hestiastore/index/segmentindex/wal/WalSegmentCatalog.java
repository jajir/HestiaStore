package org.hestiastore.index.segmentindex.wal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.segmentindex.Wal;
import org.slf4j.Logger;

final class WalSegmentCatalog {

    private static final long CHECKPOINT_CLEANUP_LOG_INTERVAL_NANOS = TimeUnit.SECONDS
            .toNanos(5L);

    private final Wal wal;
    private final WalStorage storage;
    private final WalMetadataCatalog metadataCatalog;
    private final Logger logger;
    private final List<WalSegmentDescriptor> segments = new ArrayList<>();

    private long retainedBytes = 0L;
    private long checkpointCleanupLastLogNanos = 0L;
    private long checkpointCleanupSuppressedEvents = 0L;
    private long checkpointCleanupSuppressedDeletedSegments = 0L;
    private long checkpointCleanupSuppressedDeletedBytes = 0L;

    WalSegmentCatalog(final Wal wal, final WalStorage storage,
            final WalMetadataCatalog metadataCatalog, final Logger logger) {
        this.wal = wal;
        this.storage = storage;
        this.metadataCatalog = metadataCatalog;
        this.logger = logger;
    }

    void resetRecoveredSegments() {
        retainedBytes = 0L;
        segments.clear();
    }

    void addRecoveredSegment(final String name, final long baseLsn,
            final long sizeBytes, final long maxLsn) {
        segments.add(new WalSegmentDescriptor(name, baseLsn, sizeBytes, maxLsn));
        retainedBytes += sizeBytes;
    }

    WalSegmentDescriptor ensureActiveSegmentFor(final long nextLsn,
            final int nextRecordBytes) {
        if (segments.isEmpty()) {
            return createSegment(nextLsn);
        }
        final WalSegmentDescriptor active = segments.get(segments.size() - 1);
        if (active.sizeBytes() + nextRecordBytes <= wal.getSegmentSizeBytes()) {
            return active;
        }
        return createSegment(nextLsn);
    }

    void recordAppend(final WalSegmentDescriptor activeSegment, final int bytes,
            final long lsn) {
        activeSegment.grow(bytes, lsn);
        retainedBytes += bytes;
    }

    int deleteSegmentsAfter(final List<WalSegmentDescriptor> discoveredSegments,
            final int startIndex) {
        int deletedCount = 0;
        for (int i = startIndex; i < discoveredSegments.size(); i++) {
            storage.delete(discoveredSegments.get(i).name());
            deletedCount++;
        }
        if (deletedCount > 0) {
            storage.syncMetadata();
        }
        return deletedCount;
    }

    void cleanupEligibleSegments(final long checkpointLsn) {
        if (segments.size() <= 1) {
            return;
        }
        final List<WalSegmentDescriptor> retained = new ArrayList<>(
                segments.size());
        int deletedCount = 0;
        long deletedBytes = 0L;
        for (int i = 0; i < segments.size(); i++) {
            final WalSegmentDescriptor segment = segments.get(i);
            final boolean active = i == segments.size() - 1;
            if (!active && segment.maxLsn() <= checkpointLsn) {
                storage.delete(segment.name());
                retainedBytes -= segment.sizeBytes();
                deletedBytes += segment.sizeBytes();
                deletedCount++;
            } else {
                retained.add(segment);
            }
        }
        segments.clear();
        segments.addAll(retained);
        if (deletedCount > 0) {
            storage.syncMetadata();
        }
        if (retainedBytes < 0L) {
            retainedBytes = 0L;
        }
        logCheckpointCleanupIfNeeded(checkpointLsn, deletedCount, deletedBytes);
    }

    boolean isRetentionPressure() {
        if (retainedBytes <= wal.getMaxBytesBeforeForcedCheckpoint()) {
            return false;
        }
        return segments.size() > 1;
    }

    long retainedBytes() {
        return retainedBytes;
    }

    int segmentCount() {
        return segments.size();
    }

    List<WalSegmentDescriptor> segments() {
        return segments;
    }

    private WalSegmentDescriptor createSegment(final long baseLsn) {
        final String name = metadataCatalog.toSegmentFileName(baseLsn);
        storage.touch(name);
        final WalSegmentDescriptor segment = new WalSegmentDescriptor(name,
                baseLsn, 0L, baseLsn);
        segments.add(segment);
        return segment;
    }

    private void logCheckpointCleanupIfNeeded(final long checkpointLsn,
            final int deletedCount, final long deletedBytes) {
        if (deletedCount <= 0) {
            return;
        }
        final long nowNanos = System.nanoTime();
        if (checkpointCleanupLastLogNanos != 0L
                && nowNanos
                        - checkpointCleanupLastLogNanos < CHECKPOINT_CLEANUP_LOG_INTERVAL_NANOS) {
            checkpointCleanupSuppressedEvents++;
            checkpointCleanupSuppressedDeletedSegments += deletedCount;
            checkpointCleanupSuppressedDeletedBytes += deletedBytes;
            return;
        }
        final long suppressedEvents = checkpointCleanupSuppressedEvents;
        final long suppressedDeletedSegments = checkpointCleanupSuppressedDeletedSegments;
        final long suppressedDeletedBytes = checkpointCleanupSuppressedDeletedBytes;
        checkpointCleanupSuppressedEvents = 0L;
        checkpointCleanupSuppressedDeletedSegments = 0L;
        checkpointCleanupSuppressedDeletedBytes = 0L;
        checkpointCleanupLastLogNanos = nowNanos;
        logger.info(
                "event=wal_checkpoint_cleanup checkpointLsn={} deletedSegments={} deletedBytes={} retainedSegments={} retainedBytes={} suppressedEvents={} suppressedDeletedSegments={} suppressedDeletedBytes={}",
                checkpointLsn, deletedCount, deletedBytes, segments.size(),
                retainedBytes, suppressedEvents, suppressedDeletedSegments,
                suppressedDeletedBytes);
    }
}
