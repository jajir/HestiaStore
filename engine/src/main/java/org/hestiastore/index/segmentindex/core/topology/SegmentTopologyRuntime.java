package org.hestiastore.index.segmentindex.core.topology;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.segmentaccess.SegmentAccessService;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.storage.IndexRecoveryCleanupCoordinator;
import org.hestiastore.index.segmentindex.core.streaming.DirectSegmentAccess;
import org.hestiastore.index.segmentindex.core.streaming.SegmentStreamingService;

/**
 * Owns the segment-topology subsystem that coordinates direct access, stable
 * segment maintenance, full split scans, and recovery cleanup.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentTopologyRuntime<K, V> {

    private final SplitService splitService;
    private final SegmentStreamingService<K, V> streamingService;
    private final SegmentAccessService<K, V> segmentAccessService;
    private final DirectSegmentAccess<K, V> directSegmentAccess;
    private final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator;

    public SegmentTopologyRuntime(final SplitService splitService,
            final SegmentStreamingService<K, V> streamingService,
            final SegmentAccessService<K, V> segmentAccessService,
            final DirectSegmentAccess<K, V> directSegmentAccess,
            final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator) {
        this.splitService = Vldtn.requireNonNull(splitService, "splitService");
        this.streamingService = Vldtn.requireNonNull(streamingService,
                "streamingService");
        this.segmentAccessService = Vldtn.requireNonNull(segmentAccessService,
                "segmentAccessService");
        this.directSegmentAccess = Vldtn.requireNonNull(directSegmentAccess,
                "directSegmentAccess");
        this.recoveryCleanupCoordinator = Vldtn.requireNonNull(
                recoveryCleanupCoordinator, "recoveryCleanupCoordinator");
    }

    public SplitService splitService() {
        return splitService;
    }

    public SegmentAccessService<K, V> segmentAccessService() {
        return segmentAccessService;
    }

    public void cleanupOrphanedSegmentDirectories() {
        recoveryCleanupCoordinator.cleanupOrphanedSegmentDirectories();
    }

    public boolean hasSegmentLockFile(final SegmentId segmentId) {
        return recoveryCleanupCoordinator.hasSegmentLockFile(segmentId);
    }

    public void invalidateSegmentIterators() {
        streamingService.invalidateIterators();
    }

    public void requestFullSplitScan() {
        splitService.requestFullSplitScan();
    }

    public void closeSplitRuntime() {
        splitService.close();
    }

    public EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        return streamingService.openIterator(segmentId, isolation);
    }

    public EntryIterator<K, V> openWindowIterator(
            final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        return directSegmentAccess.openWindowIterator(segmentWindow, isolation);
    }
}
