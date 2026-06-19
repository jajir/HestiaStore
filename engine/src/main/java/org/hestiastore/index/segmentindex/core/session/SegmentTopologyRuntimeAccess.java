package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.streaming.DirectSegmentCoordinator;
import org.hestiastore.index.segmentindex.core.streaming.SegmentStreamingService;

/**
 * Owns the segment-topology subsystem used by session operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentTopologyRuntimeAccess<K, V> {

    private final SplitService<K, V> splitService;
    private final SegmentStreamingService<K, V> streamingService;
    private final DirectSegmentCoordinator<K, V> directSegmentAccess;

    /**
     * Creates topology runtime access from initialized runtime services.
     *
     * @param <K> key type
     * @param <V> value type
     * @param splitService split runtime service
     * @param streamingService segment streaming service
     * @param directSegmentAccess direct segment-window access
     * @return topology runtime access
     */
    public static <K, V> SegmentTopologyRuntimeAccess<K, V> create(
            final SplitService<K, V> splitService,
            final SegmentStreamingService<K, V> streamingService,
            final DirectSegmentCoordinator<K, V> directSegmentAccess) {
        return new SegmentTopologyRuntimeAccess<>(
                splitService, streamingService, directSegmentAccess);
    }

    private SegmentTopologyRuntimeAccess(final SplitService<K, V> splitService,
            final SegmentStreamingService<K, V> streamingService,
            final DirectSegmentCoordinator<K, V> directSegmentAccess) {
        this.splitService = Vldtn.requireNonNull(splitService, "splitService");
        this.streamingService = Vldtn.requireNonNull(streamingService,
                "streamingService");
        this.directSegmentAccess = Vldtn.requireNonNull(directSegmentAccess,
                "directSegmentAccess");
    }

    /**
     * Invalidates open segment iterators.
     */
    public void invalidateSegmentIterators() {
        streamingService.invalidateIterators();
    }

    /**
     * Requests a full split scan.
     */
    public void requestFullSplitScan() {
        splitService.requestFullSplitScan();
    }

    /**
     * Closes the split runtime.
     */
    public void closeSplitRuntime() {
        splitService.close();
    }

    /**
     * Opens an iterator for one segment.
     *
     * @param segmentId segment id
     * @param isolation iterator isolation mode
     * @return segment iterator
     */
    public EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        return streamingService.openIterator(segmentId, isolation);
    }

    /**
     * Opens an iterator for a segment window.
     *
     * @param segmentWindow segment window
     * @param isolation iterator isolation mode
     * @return window iterator
     */
    public EntryIterator<K, V> openWindowIterator(
            final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        return directSegmentAccess.openWindowIterator(segmentWindow, isolation);
    }
}
