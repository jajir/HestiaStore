package org.hestiastore.index.segmentindex.split;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;

/**
 * Carries pre-computed statistics and mutable counters used during a single
 * partition split execution.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class PartitionSplitPlan<K, V> {

    private K minKey;
    private K maxKey;
    private long lowerCount;
    private long higherCount;
    private final long estimatedNumberOfKeys;
    private final long half;

    private PartitionSplitPlan(final long estimatedNumberOfKeys) {
        this.estimatedNumberOfKeys = estimatedNumberOfKeys;
        this.half = estimatedNumberOfKeys / 2;
    }

    /**
     * Builds a split plan from the supplied policy.
     *
     * @param <K> key type
     * @param <V> value type
     * @param partitionSplitPolicy policy describing the estimated key count
     * @return new split plan
     */
    static <K, V> PartitionSplitPlan<K, V> fromPolicy(
            final PartitionSplitPolicy partitionSplitPolicy) {
        Vldtn.requireNonNull(partitionSplitPolicy, "partitionSplitPolicy");
        final long estimatedNumberOfKeys = partitionSplitPolicy
                .estimateNumberOfKeys();
        return new PartitionSplitPlan<>(estimatedNumberOfKeys);
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

    /**
     * Returns the estimated number of keys used to build this plan.
     *
     * @return estimated key count
     */
    long getEstimatedNumberOfKeys() {
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
