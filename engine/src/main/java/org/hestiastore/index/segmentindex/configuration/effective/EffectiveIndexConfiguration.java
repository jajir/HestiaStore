package org.hestiastore.index.segmentindex.configuration.effective;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;

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
    private final IndexWalConfiguration wal;
    private final EffectiveIndexFilterConfiguration filters;
    private final EffectiveIndexChunkStoreCacheConfiguration chunkStoreCache;

    public EffectiveIndexConfiguration(
            final EffectiveIndexIdentityConfiguration<K, V> identity,
            final EffectiveIndexSegmentConfiguration segment,
            final EffectiveIndexWritePathConfiguration writePath,
            final EffectiveIndexBloomFilterConfiguration bloomFilter,
            final EffectiveIndexMaintenanceConfiguration maintenance,
            final EffectiveIndexIoConfiguration io,
            final EffectiveIndexLoggingConfiguration logging,
            final IndexWalConfiguration wal,
            final EffectiveIndexFilterConfiguration filters) {
        this(identity, segment, writePath, bloomFilter, maintenance, io,
                logging, wal, filters,
                new EffectiveIndexChunkStoreCacheConfiguration(0));
    }

    public EffectiveIndexConfiguration(
            final EffectiveIndexIdentityConfiguration<K, V> identity,
            final EffectiveIndexSegmentConfiguration segment,
            final EffectiveIndexWritePathConfiguration writePath,
            final EffectiveIndexBloomFilterConfiguration bloomFilter,
            final EffectiveIndexMaintenanceConfiguration maintenance,
            final EffectiveIndexIoConfiguration io,
            final EffectiveIndexLoggingConfiguration logging,
            final IndexWalConfiguration wal,
            final EffectiveIndexFilterConfiguration filters,
            final EffectiveIndexChunkStoreCacheConfiguration chunkStoreCache) {
        this.identity = Vldtn.requireNonNull(identity, "identity");
        this.segment = Vldtn.requireNonNull(segment, "segment");
        final EffectiveIndexWritePathConfiguration validatedWritePath = Vldtn
                .requireNonNull(writePath, "writePath");
        this.chunkStoreCache = Vldtn.requireNonNull(chunkStoreCache,
                "chunkStoreCache");
        this.runtimeTuning = new EffectiveIndexRuntimeTuningConfiguration(
                this.segment.cachedSegmentLimit(), this.segment.cacheKeyLimit(),
                validatedWritePath, this.chunkStoreCache);
        this.bloomFilter = Vldtn.requireNonNull(bloomFilter, "bloomFilter");
        this.maintenance = Vldtn.requireNonNull(maintenance, "maintenance");
        this.io = Vldtn.requireNonNull(io, "io");
        this.logging = Vldtn.requireNonNull(logging, "logging");
        this.wal = IndexWalConfiguration.orEmpty(wal);
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

    public IndexWalConfiguration wal() {
        return wal;
    }

    public EffectiveIndexFilterConfiguration filters() {
        return filters;
    }

    public EffectiveIndexRuntimeTuningConfiguration runtimeTuning() {
        return runtimeTuning;
    }

    public EffectiveIndexChunkStoreCacheConfiguration chunkStoreCache() {
        return chunkStoreCache;
    }

    private static void validateWal(final IndexWalConfiguration wal) {
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
