package org.hestiastore.index.segmentindex;

/**
 * Encapsulates pre-split evaluations for a segment. It determines whether a
 * compaction should occur before splitting and provides an estimated number of
 * keys combining on-disk and delta-cache data.
 *
 * @param <K> key type handled by the segment
 * @param <V> value type handled by the segment
 */
public final class SegmentSplitterPolicy<K, V> {

    private static final float MINIMAL_PERCENTAGE_DIFFERENCE = 0.9F;

    private final long estimatedNumberOfKeys;
    private final boolean hasTombstones;

    /**
     * Creates a policy bound to a specific segment lifecycle.
     *
     * @param estimatedNumberOfKeys estimated number of keys in the segment
     * @param hasTombstones          whether tombstones were detected
     */
    SegmentSplitterPolicy(
            final long estimatedNumberOfKeys, final boolean hasTombstones) {
        if (estimatedNumberOfKeys < 0) {
            throw new IllegalArgumentException(
                    "Property 'estimatedNumberOfKeys' must be >= 0.");
        }
        this.estimatedNumberOfKeys = estimatedNumberOfKeys;
        this.hasTombstones = hasTombstones;
    }

    /**
     * Evaluates whether a compaction step should precede splitting.
     *
     * @param maxNumberOfKeysInSegment configured split threshold for the
     *                                 segment
     * @return {@code true} when compaction is recommended before attempting a
     *         split
     */
    public boolean shouldBeCompactedBeforeSplitting(
            final long maxNumberOfKeysInSegment) {
        return shouldBeCompactedBeforeSplitting(maxNumberOfKeysInSegment,
                estimateNumberOfKeys());
    }

    public boolean shouldBeCompactedBeforeSplitting(
            final long maxNumberOfKeysInSegment,
            final long estimatedNumberOfKeys) {
        if (estimatedNumberOfKeys <= 3) {
            return true;
        }
        return estimatedNumberOfKeys < maxNumberOfKeysInSegment
                * MINIMAL_PERCENTAGE_DIFFERENCE;
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

    /**
     * @return true when delta cache holds any tombstone entries.
     */
    public boolean hasTombstonesInDeltaCache() {
        return hasTombstones;
    }
}
