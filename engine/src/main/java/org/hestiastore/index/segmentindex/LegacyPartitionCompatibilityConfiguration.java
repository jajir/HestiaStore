package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Legacy-named compatibility view retained for callers that still consume the
 * removed partition-overlay vocabulary.
 */
public final class LegacyPartitionCompatibilityConfiguration {

    private final IndexWritePathConfiguration writePathConfiguration;
    private final Integer maxNumberOfImmutableRunsPerPartition;

    /**
     * Creates immutable compatibility view.
     *
     * @param writePathConfiguration canonical write-path configuration
     * @param maxNumberOfImmutableRunsPerPartition legacy compatibility limit
     */
    public LegacyPartitionCompatibilityConfiguration(
            final IndexWritePathConfiguration writePathConfiguration,
            final Integer maxNumberOfImmutableRunsPerPartition) {
        this.writePathConfiguration = Vldtn
                .requireNonNull(writePathConfiguration,
                        "writePathConfiguration");
        if (maxNumberOfImmutableRunsPerPartition != null
                && maxNumberOfImmutableRunsPerPartition.intValue() < 1) {
            throw new IllegalArgumentException(
                    "maxNumberOfImmutableRunsPerPartition must be >= 1");
        }
        this.maxNumberOfImmutableRunsPerPartition = maxNumberOfImmutableRunsPerPartition;
    }

    public Integer getMaxNumberOfKeysInActivePartition() {
        return writePathConfiguration.getSegmentWriteCacheKeyLimit();
    }

    public Integer getMaxNumberOfImmutableRunsPerPartition() {
        return maxNumberOfImmutableRunsPerPartition;
    }

    public Integer getMaxNumberOfKeysInPartitionBuffer() {
        return writePathConfiguration
                .getSegmentWriteCacheKeyLimitDuringMaintenance();
    }

    public Integer getMaxNumberOfKeysInIndexBuffer() {
        return writePathConfiguration.getIndexBufferedWriteKeyLimit();
    }

    public Integer getMaxNumberOfKeysInPartitionBeforeSplit() {
        return writePathConfiguration.getSegmentSplitKeyThreshold();
    }
}
