package org.hestiastore.index.segment;

import org.hestiastore.index.AtomicKey;
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

    private final AtomicKey<K> minKey = new AtomicKey<>();
    private final AtomicKey<K> maxKey = new AtomicKey<>();
    private long lowerCount;
    private long higherCount;
    private final long estimatedNumberOfKeys;
    private final long half;

    private SegmentSplitterPlan(final long estimatedNumberOfKeys) {
        this.estimatedNumberOfKeys = estimatedNumberOfKeys;
        this.half = estimatedNumberOfKeys / 2;
    }

    static <K, V> SegmentSplitterPlan<K, V> fromPolicy(
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
        lowerCount++;
        if (minKey.isEmpty()) {
            minKey.set(pair.getKey());
        }
        maxKey.set(pair.getKey());
    }

    void recordUpper() {
        higherCount++;
    }

    long lowerCount() {
        return lowerCount;
    }

    long higherCount() {
        return higherCount;
    }

    long estimatedNumberOfKeys() {
        return estimatedNumberOfKeys;
    }

    long half() {
        return half;
    }

    boolean hasLowerKeys() {
        return lowerCount > 0;
    }

    K minKey() {
        return minKey.get();
    }

    K maxKey() {
        return maxKey.get();
    }
}
