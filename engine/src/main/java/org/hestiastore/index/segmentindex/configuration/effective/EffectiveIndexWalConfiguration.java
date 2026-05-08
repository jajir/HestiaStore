package org.hestiastore.index.segmentindex.configuration.effective;

import java.util.Objects;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.WalCorruptionPolicy;
import org.hestiastore.index.segmentindex.WalDurabilityMode;

/**
 * Fully resolved WAL configuration stored as part of an effective index
 * configuration.
 */
@SuppressWarnings("java:S107")
public final class EffectiveIndexWalConfiguration {

    /**
     * Null-object instance meaning WAL is disabled.
     */
    public static final EffectiveIndexWalConfiguration EMPTY =
            new EffectiveIndexWalConfiguration(false,
                    IndexWalConfiguration.DEFAULT_DURABILITY_MODE,
                    IndexWalConfiguration.DEFAULT_SEGMENT_SIZE_BYTES,
                    IndexWalConfiguration.DEFAULT_GROUP_SYNC_DELAY_MILLIS,
                    IndexWalConfiguration.DEFAULT_GROUP_SYNC_MAX_BATCH_BYTES,
                    IndexWalConfiguration.DEFAULT_MAX_BYTES_BEFORE_FORCED_CHECKPOINT,
                    IndexWalConfiguration.DEFAULT_CORRUPTION_POLICY, false);

    private final boolean enabled;
    private final WalDurabilityMode durabilityMode;
    private final long segmentSizeBytes;
    private final int groupSyncDelayMillis;
    private final int groupSyncMaxBatchBytes;
    private final long maxBytesBeforeForcedCheckpoint;
    private final WalCorruptionPolicy corruptionPolicy;
    private final boolean epochSupport;

    public EffectiveIndexWalConfiguration(final boolean enabled,
            final WalDurabilityMode durabilityMode,
            final long segmentSizeBytes, final int groupSyncDelayMillis,
            final int groupSyncMaxBatchBytes,
            final long maxBytesBeforeForcedCheckpoint,
            final WalCorruptionPolicy corruptionPolicy,
            final boolean epochSupport) {
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
    }

    public static EffectiveIndexWalConfiguration fromIndexWalConfiguration(
            final IndexWalConfiguration wal) {
        final IndexWalConfiguration source = IndexWalConfiguration.orEmpty(wal);
        if (!source.isEnabled()) {
            return EMPTY;
        }
        return new EffectiveIndexWalConfiguration(source.isEnabled(),
                source.getDurabilityMode(), source.getSegmentSizeBytes(),
                source.getGroupSyncDelayMillis(),
                source.getGroupSyncMaxBatchBytes(),
                source.getMaxBytesBeforeForcedCheckpoint(),
                source.getCorruptionPolicy(), source.isEpochSupport());
    }

    public static EffectiveIndexWalConfiguration orEmpty(
            final EffectiveIndexWalConfiguration wal) {
        return wal == null ? EMPTY : wal;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isEmpty() {
        return this == EMPTY || !enabled;
    }

    public WalDurabilityMode getDurabilityMode() {
        return durabilityMode;
    }

    public long getSegmentSizeBytes() {
        return segmentSizeBytes;
    }

    public int getGroupSyncDelayMillis() {
        return groupSyncDelayMillis;
    }

    public int getGroupSyncMaxBatchBytes() {
        return groupSyncMaxBatchBytes;
    }

    public long getMaxBytesBeforeForcedCheckpoint() {
        return maxBytesBeforeForcedCheckpoint;
    }

    public WalCorruptionPolicy getCorruptionPolicy() {
        return corruptionPolicy;
    }

    public boolean isEpochSupport() {
        return epochSupport;
    }

    public IndexWalConfiguration toIndexWalConfiguration() {
        if (!enabled) {
            return IndexWalConfiguration.EMPTY;
        }
        return IndexWalConfiguration.builder().durability(durabilityMode)
                .segmentSizeBytes(segmentSizeBytes)
                .groupSyncDelayMillis(groupSyncDelayMillis)
                .groupSyncMaxBatchBytes(groupSyncMaxBatchBytes)
                .maxBytesBeforeForcedCheckpoint(
                        maxBytesBeforeForcedCheckpoint)
                .corruptionPolicy(corruptionPolicy)
                .epochSupport(epochSupport).build();
    }

    @Override
    public int hashCode() {
        return Objects.hash(corruptionPolicy, durabilityMode, enabled,
                epochSupport, groupSyncDelayMillis, groupSyncMaxBatchBytes,
                maxBytesBeforeForcedCheckpoint, segmentSizeBytes);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof EffectiveIndexWalConfiguration other)) {
            return false;
        }
        return enabled == other.enabled && segmentSizeBytes == other.segmentSizeBytes
                && groupSyncDelayMillis == other.groupSyncDelayMillis
                && groupSyncMaxBatchBytes == other.groupSyncMaxBatchBytes
                && maxBytesBeforeForcedCheckpoint == other.maxBytesBeforeForcedCheckpoint
                && epochSupport == other.epochSupport
                && durabilityMode == other.durabilityMode
                && corruptionPolicy == other.corruptionPolicy;
    }

    @Override
    public String toString() {
        return "EffectiveIndexWalConfiguration{enabled=" + enabled
                + ", durabilityMode=" + durabilityMode
                + ", segmentSizeBytes=" + segmentSizeBytes
                + ", groupSyncDelayMillis=" + groupSyncDelayMillis
                + ", groupSyncMaxBatchBytes=" + groupSyncMaxBatchBytes
                + ", maxBytesBeforeForcedCheckpoint="
                + maxBytesBeforeForcedCheckpoint + ", corruptionPolicy="
                + corruptionPolicy + ", epochSupport=" + epochSupport + "}";
    }
}
