package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.storage.IndexRecoveryCleanupCoordinator;
import org.hestiastore.index.segmentindex.core.streaming.DirectSegmentAccess;
import org.hestiastore.index.segmentindex.core.streaming.SegmentStreamingService;

/**
 * Narrow runtime view used by the session runtime.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentTopologyRuntimeAccess<K, V> {

    static <K, V> SegmentTopologyRuntimeAccess<K, V> create(
            final SplitService splitService,
            final SegmentStreamingService<K, V> streamingService,
            final DirectSegmentAccess<K, V> directSegmentAccess,
            final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator) {
        return new SegmentTopologyRuntimeAccessImpl<>(
                Vldtn.requireNonNull(splitService, "splitService"),
                Vldtn.requireNonNull(streamingService, "streamingService"),
                Vldtn.requireNonNull(directSegmentAccess, "directSegmentAccess"),
                Vldtn.requireNonNull(recoveryCleanupCoordinator,
                        "recoveryCleanupCoordinator"));
    }

    void cleanupOrphanedSegmentDirectories();

    boolean hasSegmentLockFile(SegmentId segmentId);

    void invalidateSegmentIterators();

    void requestFullSplitScan();

    void closeSplitRuntime();

    EntryIterator<K, V> openSegmentIterator(SegmentId segmentId,
            SegmentIteratorIsolation isolation);

    EntryIterator<K, V> openWindowIterator(SegmentWindow segmentWindow,
            SegmentIteratorIsolation isolation);
}
