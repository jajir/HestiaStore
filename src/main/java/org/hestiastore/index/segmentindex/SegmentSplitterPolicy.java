package org.hestiastore.index.segmentindex;

/**
 * Encapsulates pre-split statistics for a segment, providing an estimated
 * number of keys that drives split planning.
 *
 * @param <K> key type handled by the segment
 * @param <V> value type handled by the segment
 */
public final class SegmentSplitterPolicy<K, V> {

    private final long estimatedNumberOfKeys;

    /**
     * Creates a policy bound to a specific segment snapshot.
     *
     * @param estimatedNumberOfKeys estimated number of keys in the segment
     */
    SegmentSplitterPolicy(final long estimatedNumberOfKeys) {
        if (estimatedNumberOfKeys < 0) {
            throw new IllegalArgumentException(
                    "Property 'estimatedNumberOfKeys' must be >= 0.");
        }
        this.estimatedNumberOfKeys = estimatedNumberOfKeys;
    }

    /**
     * Provides the combined number of keys currently present in the on-disk
     * index and the delta cache.
     *
     * @return estimated total number of live keys held by the segment
     */
    long estimateNumberOfKeys() {
        return estimatedNumberOfKeys;
    }
}
