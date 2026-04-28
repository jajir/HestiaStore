package org.hestiastore.index.segmentindex;

/**
 * Builder section for WAL settings.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexWalConfigurationBuilder<K, V> {

    private Boolean enabled;
    private WalDurabilityMode durabilityMode;
    private Long segmentSizeBytes;
    private Integer groupSyncDelayMillis;
    private Integer groupSyncMaxBatchBytes;
    private Long maxBytesBeforeForcedCheckpoint;
    private WalCorruptionPolicy corruptionPolicy;
    private Boolean epochSupport;

    IndexWalConfigurationBuilder() {
    }

    /**
     * Enables WAL with default values unless overridden.
     *
     * @return this section builder
     */
    public IndexWalConfigurationBuilder<K, V> enabled() {
        this.enabled = Boolean.TRUE;
        return this;
    }

    /**
     * Disables WAL.
     *
     * @return this section builder
     */
    public IndexWalConfigurationBuilder<K, V> disabled() {
        this.enabled = Boolean.FALSE;
        return this;
    }

    /**
     * Copies an existing WAL configuration into this section.
     *
     * @param value WAL configuration
     * @return this section builder
     */
    public IndexWalConfigurationBuilder<K, V> configuration(final Wal value) {
        final Wal wal = Wal.orEmpty(value);
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
     * @return this section builder
     */
    public IndexWalConfigurationBuilder<K, V> durability(
            final WalDurabilityMode value) {
        markEnabled();
        this.durabilityMode = value;
        return this;
    }

    /**
     * Sets WAL segment size in bytes.
     *
     * @param value segment size in bytes
     * @return this section builder
     */
    public IndexWalConfigurationBuilder<K, V> segmentSizeBytes(
            final long value) {
        markEnabled();
        this.segmentSizeBytes = Long.valueOf(value);
        return this;
    }

    /**
     * Sets group sync delay in milliseconds.
     *
     * @param value group sync delay
     * @return this section builder
     */
    public IndexWalConfigurationBuilder<K, V> groupSyncDelayMillis(
            final int value) {
        markEnabled();
        this.groupSyncDelayMillis = Integer.valueOf(value);
        return this;
    }

    /**
     * Sets group sync max batch bytes.
     *
     * @param value max batch bytes
     * @return this section builder
     */
    public IndexWalConfigurationBuilder<K, V> groupSyncMaxBatchBytes(
            final int value) {
        markEnabled();
        this.groupSyncMaxBatchBytes = Integer.valueOf(value);
        return this;
    }

    /**
     * Sets max retained WAL bytes before forced checkpoint.
     *
     * @param value max bytes before forced checkpoint
     * @return this section builder
     */
    public IndexWalConfigurationBuilder<K, V> maxBytesBeforeForcedCheckpoint(
            final long value) {
        markEnabled();
        this.maxBytesBeforeForcedCheckpoint = Long.valueOf(value);
        return this;
    }

    /**
     * Sets corruption handling policy.
     *
     * @param value corruption policy
     * @return this section builder
     */
    public IndexWalConfigurationBuilder<K, V> corruptionPolicy(
            final WalCorruptionPolicy value) {
        markEnabled();
        this.corruptionPolicy = value;
        return this;
    }

    /**
     * Sets reserved epoch support flag.
     *
     * @param value epoch support flag
     * @return this section builder
     */
    public IndexWalConfigurationBuilder<K, V> epochSupport(
            final boolean value) {
        markEnabled();
        this.epochSupport = Boolean.valueOf(value);
        return this;
    }

    void applyTo(final IndexConfigurationBuilder<K, V> builder) {
        if (enabled == null) {
            return;
        }
        if (!enabled.booleanValue()) {
            builder.setWal(Wal.EMPTY);
            return;
        }
        WalBuilder walBuilder = Wal.builder();
        if (durabilityMode != null) {
            walBuilder = walBuilder.withDurabilityMode(durabilityMode);
        }
        if (segmentSizeBytes != null) {
            walBuilder = walBuilder
                    .withSegmentSizeBytes(segmentSizeBytes.longValue());
        }
        if (groupSyncDelayMillis != null) {
            walBuilder = walBuilder.withGroupSyncDelayMillis(
                    groupSyncDelayMillis.intValue());
        }
        if (groupSyncMaxBatchBytes != null) {
            walBuilder = walBuilder.withGroupSyncMaxBatchBytes(
                    groupSyncMaxBatchBytes.intValue());
        }
        if (maxBytesBeforeForcedCheckpoint != null) {
            walBuilder = walBuilder.withMaxBytesBeforeForcedCheckpoint(
                    maxBytesBeforeForcedCheckpoint.longValue());
        }
        if (corruptionPolicy != null) {
            walBuilder = walBuilder.withCorruptionPolicy(corruptionPolicy);
        }
        if (epochSupport != null) {
            walBuilder = walBuilder.withEpochSupport(
                    epochSupport.booleanValue());
        }
        builder.setWal(walBuilder.build());
    }

    private void markEnabled() {
        this.enabled = Boolean.TRUE;
    }
}
