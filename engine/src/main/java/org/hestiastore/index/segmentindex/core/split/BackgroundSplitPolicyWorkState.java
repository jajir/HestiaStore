package org.hestiastore.index.segmentindex.core.split;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.segment.SegmentId;

/**
 * Mutable coordination state shared by the split-policy scheduler and worker
 * loop.
 */
final class BackgroundSplitPolicyWorkState {

    private final AtomicBoolean scanScheduled = new AtomicBoolean(false);
    private final AtomicBoolean scanRequested = new AtomicBoolean(false);
    private final AtomicBoolean tickScheduled = new AtomicBoolean(false);
    private final ConcurrentHashMap<SegmentId, Boolean> hintedSegments =
            new ConcurrentHashMap<>();

    void markScanRequested() {
        scanRequested.set(true);
    }

    boolean consumeScanRequested() {
        return scanRequested.getAndSet(false);
    }

    boolean isScanRequested() {
        return scanRequested.get();
    }

    boolean tryMarkScanScheduled() {
        return scanScheduled.compareAndSet(false, true);
    }

    void clearScanScheduled() {
        scanScheduled.set(false);
    }

    boolean isScanScheduled() {
        return scanScheduled.get();
    }

    boolean tryMarkTickScheduled() {
        return tickScheduled.compareAndSet(false, true);
    }

    void clearTickScheduled() {
        tickScheduled.set(false);
    }

    void addHint(final SegmentId segmentId) {
        hintedSegments.put(segmentId, Boolean.TRUE);
    }

    boolean hasPendingHints() {
        return !hintedSegments.isEmpty();
    }

    List<SegmentId> consumeHintedSegmentIds() {
        final List<SegmentId> segmentIds = new ArrayList<>(
                hintedSegments.keySet());
        segmentIds.forEach(hintedSegments::remove);
        return segmentIds;
    }

    void clearHintedSegments() {
        hintedSegments.clear();
    }

    void clearPendingWork() {
        scanRequested.set(false);
        hintedSegments.clear();
    }

    boolean hasPendingWork() {
        return scanRequested.get() || hasPendingHints();
    }
}
