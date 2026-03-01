package org.hestiastore.index.segmentindex;

import java.util.Objects;

import org.hestiastore.index.Vldtn;

/**
 * Immutable WAL configuration attached to an index configuration.
 */
public final class Wal {

    /**
     * Default segment rotation size in bytes.
     */
    public static final long DEFAULT_SEGMENT_SIZE_BYTES = 64L * 1024L * 1024L;

    /**
     * Default group sync delay in milliseconds.
     */
    public static final int DEFAULT_GROUP_SYNC_DELAY_MILLIS = 5;

    /**
     * Default group sync max batch bytes.
     */
    public static final int DEFAULT_GROUP_SYNC_MAX_BATCH_BYTES = 1024 * 1024;

    /**
     * Default max retained WAL bytes before checkpoint/backpressure.
     */
    public static final long DEFAULT_MAX_BYTES_BEFORE_FORCED_CHECKPOINT = 512L
            * 1024L * 1024L;

    /**
     * Default durability mode.
     */
    public static final WalDurabilityMode DEFAULT_DURABILITY_MODE = WalDurabilityMode.GROUP_SYNC;

    /**
     * Default corruption handling policy.
     */
    public static final WalCorruptionPolicy DEFAULT_CORRUPTION_POLICY = WalCorruptionPolicy.TRUNCATE_INVALID_TAIL;

    /**
     * Default replication mode.
     */
    public static final WalReplicationMode DEFAULT_REPLICATION_MODE = WalReplicationMode.DISABLED;

    /**
     * Default source node id for local (non-replicated) mode.
     */
    public static final String DEFAULT_SOURCE_NODE_ID = "";

    /**
     * Null-object instance meaning WAL is disabled.
     */
    public static final Wal EMPTY = new Wal(false, DEFAULT_DURABILITY_MODE,
            DEFAULT_SEGMENT_SIZE_BYTES, DEFAULT_GROUP_SYNC_DELAY_MILLIS,
            DEFAULT_GROUP_SYNC_MAX_BATCH_BYTES,
            DEFAULT_MAX_BYTES_BEFORE_FORCED_CHECKPOINT,
            DEFAULT_CORRUPTION_POLICY, false, DEFAULT_REPLICATION_MODE,
            DEFAULT_SOURCE_NODE_ID);

    private final boolean enabled;
    private final WalDurabilityMode durabilityMode;
    private final long segmentSizeBytes;
    private final int groupSyncDelayMillis;
    private final int groupSyncMaxBatchBytes;
    private final long maxBytesBeforeForcedCheckpoint;
    private final WalCorruptionPolicy corruptionPolicy;
    private final boolean epochSupport;
    private final WalReplicationMode replicationMode;
    private final String sourceNodeId;

    Wal(final boolean enabled, final WalDurabilityMode durabilityMode,
            final long segmentSizeBytes, final int groupSyncDelayMillis,
            final int groupSyncMaxBatchBytes,
            final long maxBytesBeforeForcedCheckpoint,
            final WalCorruptionPolicy corruptionPolicy,
            final boolean epochSupport,
            final WalReplicationMode replicationMode,
            final String sourceNodeId) {
        this.enabled = enabled;
        this.durabilityMode = Vldtn.requireNonNull(durabilityMode,
                "durabilityMode");
        this.segmentSizeBytes = segmentSizeBytes;
        this.groupSyncDelayMillis = groupSyncDelayMillis;
        this.groupSyncMaxBatchBytes = groupSyncMaxBatchBytes;
        this.maxBytesBeforeForcedCheckpoint = maxBytesBeforeForcedCheckpoint;
        this.corruptionPolicy = Vldtn.requireNonNull(corruptionPolicy,
                "corruptionPolicy");
        this.epochSupport = epochSupport;
        this.replicationMode = Vldtn.requireNonNull(replicationMode,
                "replicationMode");
        this.sourceNodeId = Vldtn.requireNonNull(sourceNodeId, "sourceNodeId")
                .trim();
    }

    /**
     * Creates a new WAL builder.
     *
     * @return WAL builder
     */
    public static WalBuilder builder() {
        return new WalBuilder();
    }

    /**
     * Returns whether WAL is enabled.
     *
     * @return true when enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Returns whether this configuration is the null-object disabled WAL.
     *
     * @return true when disabled
     */
    public boolean isEmpty() {
        return this == EMPTY || !enabled;
    }

    /**
     * Returns durability mode.
     *
     * @return durability mode
     */
    public WalDurabilityMode getDurabilityMode() {
        return durabilityMode;
    }

    /**
     * Returns WAL segment rotation size in bytes.
     *
     * @return segment size in bytes
     */
    public long getSegmentSizeBytes() {
        return segmentSizeBytes;
    }

    /**
     * Returns group sync delay in milliseconds.
     *
     * @return group sync delay
     */
    public int getGroupSyncDelayMillis() {
        return groupSyncDelayMillis;
    }

    /**
     * Returns max group sync batch bytes.
     *
     * @return max group sync batch bytes
     */
    public int getGroupSyncMaxBatchBytes() {
        return groupSyncMaxBatchBytes;
    }

    /**
     * Returns max retained WAL bytes before forced checkpoint/backpressure.
     *
     * @return max retained bytes
     */
    public long getMaxBytesBeforeForcedCheckpoint() {
        return maxBytesBeforeForcedCheckpoint;
    }

    /**
     * Returns corruption handling policy.
     *
     * @return corruption policy
     */
    public WalCorruptionPolicy getCorruptionPolicy() {
        return corruptionPolicy;
    }

    /**
     * Returns reserved epoch support toggle.
     *
     * @return true when epoch support is on
     */
    public boolean isEpochSupport() {
        return epochSupport;
    }

    /**
     * Returns replication mode.
     *
     * @return replication mode
     */
    public WalReplicationMode getReplicationMode() {
        return replicationMode;
    }

    /**
     * Returns source node id used for replicated records.
     *
     * @return source node id
     */
    public String getSourceNodeId() {
        return sourceNodeId;
    }

    /**
     * Returns true when replication mode is enabled.
     *
     * @return true when replication mode is not disabled
     */
    public boolean isReplicationEnabled() {
        return replicationMode != WalReplicationMode.DISABLED;
    }

    /**
     * Returns this instance when non-null, otherwise {@link #EMPTY}.
     *
     * @param wal candidate WAL config
     * @return non-null WAL config
     */
    public static Wal orEmpty(final Wal wal) {
        return wal == null ? EMPTY : wal;
    }

    @Override
    public int hashCode() {
        return Objects.hash(corruptionPolicy, durabilityMode, enabled,
                epochSupport, groupSyncDelayMillis, groupSyncMaxBatchBytes,
                maxBytesBeforeForcedCheckpoint, replicationMode,
                segmentSizeBytes, sourceNodeId);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Wal other)) {
            return false;
        }
        return enabled == other.enabled && segmentSizeBytes == other.segmentSizeBytes
                && groupSyncDelayMillis == other.groupSyncDelayMillis
                && groupSyncMaxBatchBytes == other.groupSyncMaxBatchBytes
                && maxBytesBeforeForcedCheckpoint == other.maxBytesBeforeForcedCheckpoint
                && epochSupport == other.epochSupport
                && replicationMode == other.replicationMode
                && durabilityMode == other.durabilityMode
                && corruptionPolicy == other.corruptionPolicy
                && Objects.equals(sourceNodeId, other.sourceNodeId);
    }

    @Override
    public String toString() {
        return "Wal{enabled=" + enabled + ", durabilityMode=" + durabilityMode
                + ", segmentSizeBytes=" + segmentSizeBytes
                + ", groupSyncDelayMillis=" + groupSyncDelayMillis
                + ", groupSyncMaxBatchBytes=" + groupSyncMaxBatchBytes
                + ", maxBytesBeforeForcedCheckpoint="
                + maxBytesBeforeForcedCheckpoint + ", corruptionPolicy="
                + corruptionPolicy + ", epochSupport=" + epochSupport
                + ", replicationMode=" + replicationMode + ", sourceNodeId='"
                + sourceNodeId + "'}";
    }
}
