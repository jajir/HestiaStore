package org.hestiastore.index.segmentindex.configuration.effective;

import java.util.Map;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.tuning.ConfigurationSnapshot;
import org.hestiastore.index.segmentindex.tuning.RuntimeSettingKey;

/**
 * Fully resolved, validated index configuration used for persistence and
 * runtime construction.
 *
 * @param <K> key type
 * @param <V> value type
 */
@SuppressWarnings("java:S107")
public final class EffectiveIndexConfiguration<K, V> {

    private final EffectiveIndexIdentityConfiguration<K, V> identity;
    private final EffectiveIndexSegmentConfiguration segment;
    private final EffectiveIndexRuntimeTuningConfiguration runtimeTuning;
    private final EffectiveIndexBloomFilterConfiguration bloomFilter;
    private final EffectiveIndexMaintenanceConfiguration maintenance;
    private final EffectiveIndexIoConfiguration io;
    private final EffectiveIndexLoggingConfiguration logging;
    private final EffectiveIndexWalConfiguration wal;
    private final EffectiveIndexFilterConfiguration filters;

    public EffectiveIndexConfiguration(
            final EffectiveIndexIdentityConfiguration<K, V> identity,
            final EffectiveIndexSegmentConfiguration segment,
            final EffectiveIndexWritePathConfiguration writePath,
            final EffectiveIndexBloomFilterConfiguration bloomFilter,
            final EffectiveIndexMaintenanceConfiguration maintenance,
            final EffectiveIndexIoConfiguration io,
            final EffectiveIndexLoggingConfiguration logging,
            final EffectiveIndexWalConfiguration wal,
            final EffectiveIndexFilterConfiguration filters) {
        this.identity = Vldtn.requireNonNull(identity, "identity");
        this.segment = Vldtn.requireNonNull(segment, "segment");
        final EffectiveIndexWritePathConfiguration validatedWritePath = Vldtn
                .requireNonNull(writePath, "writePath");
        this.runtimeTuning = new EffectiveIndexRuntimeTuningConfiguration(
                this.segment.cachedSegmentLimit(), this.segment.cacheKeyLimit(),
                validatedWritePath);
        this.bloomFilter = Vldtn.requireNonNull(bloomFilter, "bloomFilter");
        this.maintenance = Vldtn.requireNonNull(maintenance, "maintenance");
        this.io = Vldtn.requireNonNull(io, "io");
        this.logging = Vldtn.requireNonNull(logging, "logging");
        this.wal = EffectiveIndexWalConfiguration.orEmpty(wal);
        this.filters = Vldtn.requireNonNull(filters, "filters");
        Vldtn.requireTrue(this.segment.maxKeys() >= 4,
                "MaxNumberOfKeysInSegment must be greater or equal to 4");
        Vldtn.requireTrue(this.segment.chunkKeyLimit() <= this.segment.maxKeys(),
                "MaxNumberOfKeysInSegmentChunk must be less or equal to MaxNumberOfKeysInSegment");
        Vldtn.requireTrue(this.maintenance.busyTimeoutMillis()
                >= this.maintenance.busyBackoffMillis(),
                "busyTimeoutMillis must be greater than or equal to busyBackoffMillis");
        validateWal(this.wal);
    }

    public EffectiveIndexIdentityConfiguration<K, V> identity() {
        return identity;
    }

    public EffectiveIndexSegmentConfiguration segment() {
        return segment;
    }

    public EffectiveIndexWritePathConfiguration writePath() {
        return runtimeTuning.writePath();
    }

    public EffectiveIndexBloomFilterConfiguration bloomFilter() {
        return bloomFilter;
    }

    public EffectiveIndexMaintenanceConfiguration maintenance() {
        return maintenance;
    }

    public EffectiveIndexIoConfiguration io() {
        return io;
    }

    public EffectiveIndexLoggingConfiguration logging() {
        return logging;
    }

    public EffectiveIndexWalConfiguration wal() {
        return wal;
    }

    public EffectiveIndexFilterConfiguration filters() {
        return filters;
    }

    public EffectiveIndexRuntimeTuningConfiguration runtimeTuning() {
        return runtimeTuning;
    }

    public EffectiveIndexConfiguration<K, V> withRuntimeTuning(
            final ConfigurationSnapshot snapshot) {
        final Map<RuntimeSettingKey, Integer> values = Vldtn
                .requireNonNull(snapshot, "snapshot").getValues();
        final EffectiveIndexWritePathConfiguration writePath =
                new EffectiveIndexWritePathConfiguration(
                        value(values,
                                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT),
                        value(values,
                                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE),
                        value(values,
                                RuntimeSettingKey.INDEX_BUFFERED_WRITE_KEY_LIMIT),
                        value(values,
                                RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD));
        final EffectiveIndexSegmentConfiguration segment =
                new EffectiveIndexSegmentConfiguration(this.segment.maxKeys(),
                        this.segment.chunkKeyLimit(),
                        value(values,
                                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE),
                        value(values,
                                RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE),
                        this.segment.deltaCacheFileLimit());
        return new EffectiveIndexConfiguration<>(identity, segment, writePath,
                bloomFilter, maintenance, io, logging, wal, filters);
    }

    private static int value(final Map<RuntimeSettingKey, Integer> values,
            final RuntimeSettingKey key) {
        return Vldtn.requireNonNull(values.get(key), key.name()).intValue();
    }

    private static void validateWal(final EffectiveIndexWalConfiguration wal) {
        if (!wal.isEnabled()) {
            return;
        }
        Vldtn.requireTrue(wal.getSegmentSizeBytes() > 0L,
                "IndexWalConfiguration segment size must be greater than zero.");
        Vldtn.requireGreaterThanOrEqualToZero(wal.getGroupSyncDelayMillis(),
                "groupSyncDelayMillis");
        Vldtn.requireGreaterThanZero(wal.getGroupSyncMaxBatchBytes(),
                "groupSyncMaxBatchBytes");
        Vldtn.requireTrue(wal.getMaxBytesBeforeForcedCheckpoint() > 0L,
                "IndexWalConfiguration max bytes before forced checkpoint must be greater than zero.");
    }
}
