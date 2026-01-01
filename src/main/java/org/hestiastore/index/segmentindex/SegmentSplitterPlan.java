package org.hestiastore.index.segmentindex;
import org.hestiastore.index.Entry;
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

    void recordLower(final Entry<K, V> entry) {
        Vldtn.requireNonNull(entry, "entry");
        final K key = Vldtn.requireNonNull(entry.getKey(), "key");
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
