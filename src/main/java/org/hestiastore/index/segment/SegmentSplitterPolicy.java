package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

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

    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;

    /**
     * Creates a policy bound to a specific segment lifecycle.
     *
     * @param segmentPropertiesManager statistics provider for the segment
     * @param deltaCacheController     access to delta cache information
     */
    SegmentSplitterPolicy(
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentDeltaCacheController<K, V> deltaCacheController) {
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
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

    boolean shouldBeCompactedBeforeSplitting(
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
        final SegmentStats stats = segmentPropertiesManager.getSegmentStats();
        return stats.getNumberOfKeysInSegment()
                + deltaCacheController.getDeltaCacheSizeWithoutTombstones();
    }
}
