package org.hestiastore.index.segmentindex.partition;

/**
 * Immutable runtime snapshot of the partitioned ingest overlay.
 *
 * @author honza
 */
@SuppressWarnings({ "java:S6206", "java:S107" })
public final class PartitionRuntimeSnapshot {

    private final int partitionCount;
    private final int activePartitionCount;
    private final int drainingPartitionCount;
    private final int immutableRunCount;
    private final int bufferedKeyCount;
    private final long localThrottleCount;
    private final long globalThrottleCount;
    private final long drainScheduleCount;
    private final int drainInFlightCount;
    private final int splitBlockedPartitionCount;
    private final long splitBlockedDrainScheduleCount;
    private final long bufferFullWhileSplitBlockedCount;

    public PartitionRuntimeSnapshot(final int partitionCount,
            final int activePartitionCount, final int drainingPartitionCount,
            final int immutableRunCount, final int bufferedKeyCount,
            final long localThrottleCount, final long globalThrottleCount,
            final long drainScheduleCount, final int drainInFlightCount) {
        this(partitionCount, activePartitionCount, drainingPartitionCount,
                immutableRunCount, bufferedKeyCount, localThrottleCount,
                globalThrottleCount, drainScheduleCount, drainInFlightCount, 0,
                0L, 0L);
    }

    public PartitionRuntimeSnapshot(final int partitionCount,
            final int activePartitionCount, final int drainingPartitionCount,
            final int immutableRunCount, final int bufferedKeyCount,
            final long localThrottleCount, final long globalThrottleCount,
            final long drainScheduleCount, final int drainInFlightCount,
            final int splitBlockedPartitionCount,
            final long splitBlockedDrainScheduleCount,
            final long bufferFullWhileSplitBlockedCount) {
        this.partitionCount = Math.max(0, partitionCount);
        this.activePartitionCount = Math.max(0, activePartitionCount);
        this.drainingPartitionCount = Math.max(0, drainingPartitionCount);
        this.immutableRunCount = Math.max(0, immutableRunCount);
        this.bufferedKeyCount = Math.max(0, bufferedKeyCount);
        this.localThrottleCount = Math.max(0L, localThrottleCount);
        this.globalThrottleCount = Math.max(0L, globalThrottleCount);
        this.drainScheduleCount = Math.max(0L, drainScheduleCount);
        this.drainInFlightCount = Math.max(0, drainInFlightCount);
        this.splitBlockedPartitionCount = Math.max(0,
                splitBlockedPartitionCount);
        this.splitBlockedDrainScheduleCount = Math.max(0L,
                splitBlockedDrainScheduleCount);
        this.bufferFullWhileSplitBlockedCount = Math.max(0L,
                bufferFullWhileSplitBlockedCount);
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

    public int getBufferedKeyCount() {
        return bufferedKeyCount;
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

    public int getSplitBlockedPartitionCount() {
        return splitBlockedPartitionCount;
    }

    public long getSplitBlockedDrainScheduleCount() {
        return splitBlockedDrainScheduleCount;
    }

    public long getBufferFullWhileSplitBlockedCount() {
        return bufferFullWhileSplitBlockedCount;
    }
}
