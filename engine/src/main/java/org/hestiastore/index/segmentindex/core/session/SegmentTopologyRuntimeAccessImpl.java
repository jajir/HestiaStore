package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.streaming.DirectSegmentAccess;
import org.hestiastore.index.segmentindex.core.streaming.SegmentStreamingService;

/**
 * Owns the segment-topology subsystem that coordinates direct access, stable
 * segment maintenance, and full split scans.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentTopologyRuntimeAccessImpl<K, V>
        implements SegmentTopologyRuntimeAccess<K, V> {

    private final SplitService splitService;
    private final SegmentStreamingService<K, V> streamingService;
    private final DirectSegmentAccess<K, V> directSegmentAccess;

    SegmentTopologyRuntimeAccessImpl(final SplitService splitService,
            final SegmentStreamingService<K, V> streamingService,
            final DirectSegmentAccess<K, V> directSegmentAccess) {
        this.splitService = Vldtn.requireNonNull(splitService, "splitService");
        this.streamingService = Vldtn.requireNonNull(streamingService,
                "streamingService");
        this.directSegmentAccess = Vldtn.requireNonNull(directSegmentAccess,
                "directSegmentAccess");
    }

    @Override
    public void invalidateSegmentIterators() {
        streamingService.invalidateIterators();
    }

    @Override
    public void requestFullSplitScan() {
        splitService.requestFullSplitScan();
    }

    @Override
    public void closeSplitRuntime() {
        splitService.close();
    }

    @Override
    public EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        return streamingService.openIterator(segmentId, isolation);
    }

    @Override
    public EntryIterator<K, V> openWindowIterator(
            final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        return directSegmentAccess.openWindowIterator(segmentWindow, isolation);
    }
}
