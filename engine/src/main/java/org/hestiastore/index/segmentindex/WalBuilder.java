package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Fluent builder for {@link Wal}.
 */
public final class WalBuilder {

    private Boolean enabled;
    private WalDurabilityMode durabilityMode;
    private Long segmentSizeBytes;
    private Integer groupSyncDelayMillis;
    private Integer groupSyncMaxBatchBytes;
    private Long maxBytesBeforeForcedCheckpoint;
    private WalCorruptionPolicy corruptionPolicy;
    private Boolean epochSupport;
    private WalReplicationMode replicationMode;
    private String sourceNodeId;

    WalBuilder() {
    }

    /**
     * Enables or disables WAL.
     *
     * @param value desired enabled flag
     * @return this builder
     */
    public WalBuilder withEnabled(final boolean value) {
        this.enabled = value;
        return this;
    }

    /**
     * Sets durability mode.
     *
     * @param value durability mode
     * @return this builder
     */
    public WalBuilder withDurabilityMode(final WalDurabilityMode value) {
        this.durabilityMode = value;
        return this;
    }

    /**
     * Sets WAL segment rotation size in bytes.
     *
     * @param value segment size
     * @return this builder
     */
    public WalBuilder withSegmentSizeBytes(final long value) {
        this.segmentSizeBytes = value;
        return this;
    }

    /**
     * Sets group sync delay in milliseconds.
     *
     * @param value delay
     * @return this builder
     */
    public WalBuilder withGroupSyncDelayMillis(final int value) {
        this.groupSyncDelayMillis = value;
        return this;
    }

    /**
     * Sets group sync max batch bytes.
     *
     * @param value max batch bytes
     * @return this builder
     */
    public WalBuilder withGroupSyncMaxBatchBytes(final int value) {
        this.groupSyncMaxBatchBytes = value;
        return this;
    }

    /**
     * Sets maximum retained WAL bytes before forced checkpoint and
     * backpressure.
     *
     * @param value byte limit
     * @return this builder
     */
    public WalBuilder withMaxBytesBeforeForcedCheckpoint(final long value) {
        this.maxBytesBeforeForcedCheckpoint = value;
        return this;
    }

    /**
     * Sets corruption policy.
     *
     * @param value policy
     * @return this builder
     */
    public WalBuilder withCorruptionPolicy(final WalCorruptionPolicy value) {
        this.corruptionPolicy = value;
        return this;
    }

    /**
     * Sets reserved epoch support flag.
     *
     * @param value epoch support
     * @return this builder
     */
    public WalBuilder withEpochSupport(final boolean value) {
        this.epochSupport = value;
        return this;
    }

    /**
     * Sets replication mode.
     *
     * @param value replication mode
     * @return this builder
     */
    public WalBuilder withReplicationMode(final WalReplicationMode value) {
        this.replicationMode = value;
        return this;
    }

    /**
     * Sets source node id used for replicated records.
     *
     * @param value source node id
     * @return this builder
     */
    public WalBuilder withSourceNodeId(final String value) {
        this.sourceNodeId = value;
        return this;
    }

    /**
     * Builds immutable WAL settings.
     *
     * @return built WAL settings, or {@link Wal#EMPTY} when disabled
     */
    public Wal build() {
        final boolean effectiveEnabled = enabled == null ? true
                : enabled.booleanValue();
        if (!effectiveEnabled) {
            return Wal.EMPTY;
        }
        final WalDurabilityMode effectiveDurabilityMode = durabilityMode == null
                ? Wal.DEFAULT_DURABILITY_MODE
                : durabilityMode;
        final long effectiveSegmentSizeBytes = segmentSizeBytes == null
                ? Wal.DEFAULT_SEGMENT_SIZE_BYTES
                : segmentSizeBytes.longValue();
        final int effectiveGroupSyncDelayMillis = groupSyncDelayMillis == null
                ? Wal.DEFAULT_GROUP_SYNC_DELAY_MILLIS
                : groupSyncDelayMillis.intValue();
        final int effectiveGroupSyncMaxBatchBytes = groupSyncMaxBatchBytes == null
                ? Wal.DEFAULT_GROUP_SYNC_MAX_BATCH_BYTES
                : groupSyncMaxBatchBytes.intValue();
        final long effectiveMaxBytesBeforeForcedCheckpoint = maxBytesBeforeForcedCheckpoint == null
                ? Wal.DEFAULT_MAX_BYTES_BEFORE_FORCED_CHECKPOINT
                : maxBytesBeforeForcedCheckpoint.longValue();
        final WalCorruptionPolicy effectiveCorruptionPolicy = corruptionPolicy == null
                ? Wal.DEFAULT_CORRUPTION_POLICY
                : corruptionPolicy;
        final boolean effectiveEpochSupport = epochSupport == null ? false
                : epochSupport.booleanValue();
        final WalReplicationMode effectiveReplicationMode = replicationMode == null
                ? Wal.DEFAULT_REPLICATION_MODE
                : replicationMode;
        final String effectiveSourceNodeId = sourceNodeId == null
                ? Wal.DEFAULT_SOURCE_NODE_ID
                : sourceNodeId.trim();
        Vldtn.requireTrue(effectiveSegmentSizeBytes > 0L,
                "segmentSizeBytes must be greater than 0");
        Vldtn.requireTrue(effectiveMaxBytesBeforeForcedCheckpoint > 0L,
                "maxBytesBeforeForcedCheckpoint must be greater than 0");
        return new Wal(true, effectiveDurabilityMode,
                effectiveSegmentSizeBytes,
                Vldtn.requireGreaterThanOrEqualToZero(
                        effectiveGroupSyncDelayMillis,
                        "groupSyncDelayMillis"),
                Vldtn.requireGreaterThanZero(effectiveGroupSyncMaxBatchBytes,
                        "groupSyncMaxBatchBytes"),
                effectiveMaxBytesBeforeForcedCheckpoint,
                Vldtn.requireNonNull(effectiveCorruptionPolicy,
                        "corruptionPolicy"),
                effectiveEpochSupport,
                Vldtn.requireNonNull(effectiveReplicationMode,
                        "replicationMode"),
                effectiveSourceNodeId);
    }
}
