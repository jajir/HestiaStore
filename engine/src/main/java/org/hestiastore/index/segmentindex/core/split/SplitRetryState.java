package org.hestiastore.index.segmentindex.core.split;

/**
 * Tracks the last unsuccessful split attempt for one segment.
 */
final class SplitRetryState {

    private static final long SPLIT_RETRY_GROWTH_MIN_KEYS = 8L;
    private static final long SPLIT_RETRY_GROWTH_DIVISOR = 10L;
    private static final long SPLIT_RETRY_GROWTH_MAX_KEYS = 1_024L;

    private final long lastAttemptNanos;
    private final long lastObservedKeyCount;
    private final long cooldownNanos;

    SplitRetryState(final long lastAttemptNanos,
            final long lastObservedKeyCount,
            final long cooldownNanos) {
        this.lastAttemptNanos = lastAttemptNanos;
        this.lastObservedKeyCount = lastObservedKeyCount;
        this.cooldownNanos = cooldownNanos;
    }

    /**
     * Returns whether enough time passed or enough new keys arrived to retry.
     *
     * @param nowNanos current monotonic time
     * @param currentKeyCount current observed key count
     * @param splitThreshold configured split threshold
     * @return true when another split attempt should run
     */
    boolean shouldRetry(final long nowNanos, final long currentKeyCount,
            final long splitThreshold) {
        if (currentKeyCount >= lastObservedKeyCount
                + splitRetryGrowthThreshold(splitThreshold)) {
            return true;
        }
        return nowNanos - lastAttemptNanos >= cooldownNanos;
    }

    private static long splitRetryGrowthThreshold(final long splitThreshold) {
        final long byThreshold = Math.max(1L,
                splitThreshold / SPLIT_RETRY_GROWTH_DIVISOR);
        return Math.min(SPLIT_RETRY_GROWTH_MAX_KEYS,
                Math.max(SPLIT_RETRY_GROWTH_MIN_KEYS, byThreshold));
    }
}
