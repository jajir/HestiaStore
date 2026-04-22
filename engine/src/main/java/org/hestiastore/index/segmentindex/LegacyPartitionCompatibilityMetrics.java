package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Legacy-named compatibility metrics retained for older monitoring clients.
 */
public final class LegacyPartitionCompatibilityMetrics {

    private final SegmentIndexWritePathMetrics writePathMetrics;
    private final int maxNumberOfImmutableRunsPerPartition;
    private final int partitionCount;
    private final int activePartitionCount;
    private final int drainingPartitionCount;
    private final int immutableRunCount;
    private final int partitionBufferedKeyCount;
    private final long localThrottleCount;
    private final long globalThrottleCount;
    private final long drainScheduleCount;
    private final int drainInFlightCount;
    private final long drainLatencyP95Micros;
    private final int splitBlockedPartitionCount;
    private final long splitBlockedDrainScheduleCount;
    private final long bufferFullWhileSplitBlockedCount;

    /**
     * Creates immutable compatibility metrics.
     */
    public LegacyPartitionCompatibilityMetrics(
            final SegmentIndexWritePathMetrics writePathMetrics,
            final int maxNumberOfImmutableRunsPerPartition,
            final int partitionCount, final int activePartitionCount,
            final int drainingPartitionCount, final int immutableRunCount,
            final int partitionBufferedKeyCount, final long localThrottleCount,
            final long globalThrottleCount, final long drainScheduleCount,
            final int drainInFlightCount, final long drainLatencyP95Micros,
            final int splitBlockedPartitionCount,
            final long splitBlockedDrainScheduleCount,
            final long bufferFullWhileSplitBlockedCount) {
        this.writePathMetrics = Vldtn.requireNonNull(writePathMetrics,
                "writePathMetrics");
        requireNotNegative(maxNumberOfImmutableRunsPerPartition,
                "maxNumberOfImmutableRunsPerPartition");
        requireNotNegative(partitionCount, "partitionCount");
        requireNotNegative(activePartitionCount, "activePartitionCount");
        requireNotNegative(drainingPartitionCount,
                "drainingPartitionCount");
        requireNotNegative(immutableRunCount, "immutableRunCount");
        requireNotNegative(partitionBufferedKeyCount,
                "partitionBufferedKeyCount");
        requireNotNegative(localThrottleCount, "localThrottleCount");
        requireNotNegative(globalThrottleCount, "globalThrottleCount");
        requireNotNegative(drainScheduleCount, "drainScheduleCount");
        requireNotNegative(drainInFlightCount, "drainInFlightCount");
        requireNotNegative(drainLatencyP95Micros, "drainLatencyP95Micros");
        requireNotNegative(splitBlockedPartitionCount,
                "splitBlockedPartitionCount");
        requireNotNegative(splitBlockedDrainScheduleCount,
                "splitBlockedDrainScheduleCount");
        requireNotNegative(bufferFullWhileSplitBlockedCount,
                "bufferFullWhileSplitBlockedCount");
        this.maxNumberOfImmutableRunsPerPartition = maxNumberOfImmutableRunsPerPartition;
        this.partitionCount = partitionCount;
        this.activePartitionCount = activePartitionCount;
        this.drainingPartitionCount = drainingPartitionCount;
        this.immutableRunCount = immutableRunCount;
        this.partitionBufferedKeyCount = partitionBufferedKeyCount;
        this.localThrottleCount = localThrottleCount;
        this.globalThrottleCount = globalThrottleCount;
        this.drainScheduleCount = drainScheduleCount;
        this.drainInFlightCount = drainInFlightCount;
        this.drainLatencyP95Micros = drainLatencyP95Micros;
        this.splitBlockedPartitionCount = splitBlockedPartitionCount;
        this.splitBlockedDrainScheduleCount = splitBlockedDrainScheduleCount;
        this.bufferFullWhileSplitBlockedCount = bufferFullWhileSplitBlockedCount;
    }

    public int getMaxNumberOfKeysInActivePartition() {
        return writePathMetrics.getSegmentWriteCacheKeyLimit();
    }

    public int getMaxNumberOfImmutableRunsPerPartition() {
        return maxNumberOfImmutableRunsPerPartition;
    }

    public int getMaxNumberOfKeysInPartitionBuffer() {
        return writePathMetrics.getSegmentWriteCacheKeyLimitDuringMaintenance();
    }

    public int getMaxNumberOfKeysInIndexBuffer() {
        return writePathMetrics.getIndexBufferedWriteKeyLimit();
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getActivePartitionCount() {
        return activePartitionCount;
    }

    public int getDrainingPartitionCount() {
        return drainingPartitionCount;
    }

    public int getImmutableRunCount() {
        return immutableRunCount;
    }

    public int getPartitionBufferedKeyCount() {
        return partitionBufferedKeyCount;
    }

    public long getLocalThrottleCount() {
        return localThrottleCount;
    }

    public long getGlobalThrottleCount() {
        return globalThrottleCount;
    }

    public long getDrainScheduleCount() {
        return drainScheduleCount;
    }

    public int getDrainInFlightCount() {
        return drainInFlightCount;
    }

    public long getDrainLatencyP95Micros() {
        return drainLatencyP95Micros;
    }

    public int getSplitBlockedPartitionCount() {
        return splitBlockedPartitionCount;
    }

    public long getSplitBlockedDrainScheduleCount() {
        return splitBlockedDrainScheduleCount;
    }

    public long getBufferFullWhileSplitBlockedCount() {
        return bufferFullWhileSplitBlockedCount;
    }

    private static void requireNotNegative(final long value,
            final String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
