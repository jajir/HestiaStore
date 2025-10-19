package org.hestiastore.index.segment;
import org.hestiastore.index.Pair;
import org.hestiastore.index.Vldtn;

/**
 * Carries pre-computed statistics and mutable counters used during a single
 * segment split execution.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentSplitterPlan<K, V> {

    private K minKey;
    private K maxKey;
    private long lowerCount;
    private long higherCount;
    private final long estimatedNumberOfKeys;
    private final long half;

    private SegmentSplitterPlan(final long estimatedNumberOfKeys) {
        this.estimatedNumberOfKeys = estimatedNumberOfKeys;
        this.half = estimatedNumberOfKeys / 2;
    }

    public static <K, V> SegmentSplitterPlan<K, V> fromPolicy(
            final SegmentSplitterPolicy<K, V> segmentSplitterPolicy) {
        Vldtn.requireNonNull(segmentSplitterPolicy, "segmentSplitterPolicy");
        final long estimatedNumberOfKeys = segmentSplitterPolicy
                .estimateNumberOfKeys();
        return new SegmentSplitterPlan<>(estimatedNumberOfKeys);
    }

    boolean isSplitFeasible() {
        return half > 1;
    }

    void recordLower(final Pair<K, V> pair) {
        Vldtn.requireNonNull(pair, "pair");
        final K key = Vldtn.requireNonNull(pair.getKey(), "key");
        lowerCount++;
        if (minKey == null) {
            minKey = key;
        }
        maxKey = key;
    }

    void recordUpper() {
        higherCount++;
    }

    long getLowerCount() {
        return lowerCount;
    }

    long getHigherCount() {
        return higherCount;
    }

    public long getEstimatedNumberOfKeys() {
        return estimatedNumberOfKeys;
    }

    long getHalf() {
        return half;
    }

    boolean isLowerSegmentEmpty() {
        return lowerCount == 0;
    }

    K getMinKey() {
        return minKey;
    }

    K getMaxKey() {
        return maxKey;
    }
}
