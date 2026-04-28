package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Fluent builder for {@link IndexWalConfiguration}.
 */
public final class IndexWalConfigurationBuilder {

    private Boolean enabled;
    private WalDurabilityMode durabilityMode;
    private Long segmentSizeBytes;
    private Integer groupSyncDelayMillis;
    private Integer groupSyncMaxBatchBytes;
    private Long maxBytesBeforeForcedCheckpoint;
    private WalCorruptionPolicy corruptionPolicy;
    private Boolean epochSupport;

    /**
     * Creates a new WAL configuration builder.
     */
    public IndexWalConfigurationBuilder() {
    }

    /**
     * Enables WAL with default values unless overridden.
     *
     * @return this builder
     */
    public IndexWalConfigurationBuilder enabled() {
        this.enabled = Boolean.TRUE;
        return this;
    }

    /**
     * Disables WAL.
     *
     * @return this builder
     */
    public IndexWalConfigurationBuilder disabled() {
        this.enabled = Boolean.FALSE;
        return this;
    }

    /**
     * Copies an existing WAL configuration into this section.
     *
     * @param value WAL configuration
     * @return this builder
     */
    public IndexWalConfigurationBuilder configuration(
            final IndexWalConfiguration value) {
        final IndexWalConfiguration wal =
                IndexWalConfiguration.orEmpty(value);
        if (!wal.isEnabled()) {
            return disabled();
        }
        this.enabled = Boolean.TRUE;
        this.durabilityMode = wal.getDurabilityMode();
        this.segmentSizeBytes = Long.valueOf(wal.getSegmentSizeBytes());
        this.groupSyncDelayMillis = Integer.valueOf(
                wal.getGroupSyncDelayMillis());
        this.groupSyncMaxBatchBytes = Integer.valueOf(
                wal.getGroupSyncMaxBatchBytes());
        this.maxBytesBeforeForcedCheckpoint = Long.valueOf(
                wal.getMaxBytesBeforeForcedCheckpoint());
        this.corruptionPolicy = wal.getCorruptionPolicy();
        this.epochSupport = Boolean.valueOf(wal.isEpochSupport());
        return this;
    }

    /**
     * Sets WAL durability mode.
     *
     * @param value durability mode
     * @return this builder
     */
    public IndexWalConfigurationBuilder durability(
            final WalDurabilityMode value) {
        markEnabled();
        this.durabilityMode = value;
        return this;
    }

    /**
     * Sets WAL segment size in bytes.
     *
     * @param value segment size in bytes
     * @return this builder
     */
    public IndexWalConfigurationBuilder segmentSizeBytes(
            final long value) {
        markEnabled();
        this.segmentSizeBytes = Long.valueOf(value);
        return this;
    }

    /**
     * Sets group sync delay in milliseconds.
     *
     * @param value group sync delay
     * @return this builder
     */
    public IndexWalConfigurationBuilder groupSyncDelayMillis(
            final int value) {
        markEnabled();
        this.groupSyncDelayMillis = Integer.valueOf(value);
        return this;
    }

    /**
     * Sets group sync max batch bytes.
     *
     * @param value max batch bytes
     * @return this builder
     */
    public IndexWalConfigurationBuilder groupSyncMaxBatchBytes(
            final int value) {
        markEnabled();
        this.groupSyncMaxBatchBytes = Integer.valueOf(value);
        return this;
    }

    /**
     * Sets max retained WAL bytes before forced checkpoint.
     *
     * @param value max bytes before forced checkpoint
     * @return this builder
     */
    public IndexWalConfigurationBuilder maxBytesBeforeForcedCheckpoint(
            final long value) {
        markEnabled();
        this.maxBytesBeforeForcedCheckpoint = Long.valueOf(value);
        return this;
    }

    /**
     * Sets corruption handling policy.
     *
     * @param value corruption policy
     * @return this builder
     */
    public IndexWalConfigurationBuilder corruptionPolicy(
            final WalCorruptionPolicy value) {
        markEnabled();
        this.corruptionPolicy = value;
        return this;
    }

    /**
     * Sets reserved epoch support flag.
     *
     * @param value epoch support flag
     * @return this builder
     */
    public IndexWalConfigurationBuilder epochSupport(
            final boolean value) {
        markEnabled();
        this.epochSupport = Boolean.valueOf(value);
        return this;
    }

    /**
     * Builds immutable WAL settings.
     *
     * @return built WAL settings
     */
    public IndexWalConfiguration build() {
        if (Boolean.FALSE.equals(enabled)) {
            return IndexWalConfiguration.EMPTY;
        }
        final WalDurabilityMode effectiveDurabilityMode = durabilityMode == null
                ? IndexWalConfiguration.DEFAULT_DURABILITY_MODE
                : durabilityMode;
        final long effectiveSegmentSizeBytes = segmentSizeBytes == null
                ? IndexWalConfiguration.DEFAULT_SEGMENT_SIZE_BYTES
                : segmentSizeBytes.longValue();
        final int effectiveGroupSyncDelayMillis = groupSyncDelayMillis == null
                ? IndexWalConfiguration.DEFAULT_GROUP_SYNC_DELAY_MILLIS
                : groupSyncDelayMillis.intValue();
        final int effectiveGroupSyncMaxBatchBytes = groupSyncMaxBatchBytes == null
                ? IndexWalConfiguration.DEFAULT_GROUP_SYNC_MAX_BATCH_BYTES
                : groupSyncMaxBatchBytes.intValue();
        final long effectiveMaxBytesBeforeForcedCheckpoint = maxBytesBeforeForcedCheckpoint == null
                ? IndexWalConfiguration.DEFAULT_MAX_BYTES_BEFORE_FORCED_CHECKPOINT
                : maxBytesBeforeForcedCheckpoint.longValue();
        final WalCorruptionPolicy effectiveCorruptionPolicy = corruptionPolicy == null
                ? IndexWalConfiguration.DEFAULT_CORRUPTION_POLICY
                : corruptionPolicy;
        final boolean effectiveEpochSupport = epochSupport != null
                && epochSupport.booleanValue();
        Vldtn.requireTrue(effectiveSegmentSizeBytes > 0L,
                "segmentSizeBytes must be greater than 0");
        Vldtn.requireTrue(effectiveMaxBytesBeforeForcedCheckpoint > 0L,
                "maxBytesBeforeForcedCheckpoint must be greater than 0");
        return new IndexWalConfiguration(true, effectiveDurabilityMode,
                effectiveSegmentSizeBytes,
                Vldtn.requireGreaterThanOrEqualToZero(
                        effectiveGroupSyncDelayMillis,
                        "groupSyncDelayMillis"),
                Vldtn.requireGreaterThanZero(effectiveGroupSyncMaxBatchBytes,
                        "groupSyncMaxBatchBytes"),
                effectiveMaxBytesBeforeForcedCheckpoint,
                Vldtn.requireNonNull(effectiveCorruptionPolicy,
                        "corruptionPolicy"),
                effectiveEpochSupport);
    }

    private void markEnabled() {
        this.enabled = Boolean.TRUE;
    }
}
