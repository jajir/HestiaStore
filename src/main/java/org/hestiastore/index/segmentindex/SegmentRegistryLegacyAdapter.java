package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;

/**
 * Legacy adapter that exposes {@link SegmentResult} responses for registry
 * callers that have not migrated to {@link SegmentRegistryResult}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentRegistryLegacyAdapter<K, V> {

    private final SegmentRegistry<K, V> delegate;

    public SegmentRegistryLegacyAdapter(final SegmentRegistry<K, V> delegate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    /**
     * Returns the segment for the provided id, loading it if needed.
     *
     * @param segmentId segment id to load
     * @return result containing the segment or a status
     */
    public SegmentResult<Segment<K, V>> getSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        return SegmentRegistryResultAdapters
                .toSegmentResult(delegate.getSegment(segmentId));
    }

    /**
     * Removes a segment from the registry and deletes its files.
     *
     * @param segmentId segment id to remove
     */
    public void removeSegment(final SegmentId segmentId) {
        delegate.removeSegment(segmentId);
    }

    /**
     * Closes the registry, releasing cached segments and executors.
     */
    public void close() {
        delegate.close();
    }
}
