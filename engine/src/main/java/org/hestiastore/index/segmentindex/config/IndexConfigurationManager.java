package org.hestiastore.index.segmentindex.config;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationBuilder;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.IndexWalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Loads, merges, and validates index configuration values.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class IndexConfigurationManager<K, V> {

    private static final String INDEX_NAME_MDC_KEY = "index.name";
    private final IndexConfigurationStorage<K, V> confStorage;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public IndexConfigurationManager(
            final IndexConfigurationStorage<K, V> confStorage) {
        this.confStorage = Vldtn.requireNonNull(confStorage, "confStorage");
    }

    public IndexConfiguration<K, V> loadExisting() {
        return confStorage.load();
    }

    public Optional<IndexConfiguration<K, V>> tryToLoad() {
        if (confStorage.exists()) {
            return Optional.of(confStorage.load());
        } else {
            return Optional.empty();
        }
    }

    public IndexConfiguration<K, V> applyDefaults(
            final IndexConfiguration<K, V> conf) {
        validateRequiredDatatypesAndIndexName(conf);
        final IndexConfigurationBuilder<K, V> builder = makeBuilder(conf);
        applyTypeDescriptorDefaults(conf, builder);
        final Optional<IndexConfigurationContract> defaultsOpt = IndexConfigurationRegistry
                .get(conf.identity().keyClass());
        if (defaultsOpt.isEmpty()) {
            debugWithIndexContext(conf,
                    "There is no default configuration for key class '{}'",
                    conf.identity().keyClass());
            return validate(builder.build());
        }
        final IndexConfigurationContract defaults = defaultsOpt.get();
        applyCoreDefaults(conf, defaults, builder);
        final int effectiveMaxNumberOfKeysInSegmentCache = applySegmentCacheDefaults(
                conf, defaults, builder);
        final int effectiveActivePartitionLimit = applyActivePartitionDefaults(conf,
                builder, effectiveMaxNumberOfKeysInSegmentCache);
        applyPartitionBufferDefaults(conf, builder, effectiveActivePartitionLimit);
        applyRemainingSegmentDefaults(conf, defaults, builder);
        applyBloomAndIoDefaults(conf, defaults, builder);
        applyChunkFilterDefaults(conf, defaults, builder);
        return validate(builder.build());
    }

    private void applyTypeDescriptorDefaults(final IndexConfiguration<K, V> conf,
            final IndexConfigurationBuilder<K, V> builder) {
        if (conf.identity().keyTypeDescriptor() == null) {
            builder.identity(identity -> identity.keyTypeDescriptor(
                    DataTypeDescriptorRegistry
                            .getTypeDescriptor(conf.identity().keyClass())));
        }
        if (conf.identity().valueTypeDescriptor() == null) {
            builder.identity(identity -> identity.valueTypeDescriptor(
                    DataTypeDescriptorRegistry
                            .getTypeDescriptor(conf.identity().valueClass())));
        }
    }

    private void applyCoreDefaults(final IndexConfiguration<K, V> conf,
            final IndexConfigurationContract defaults,
            final IndexConfigurationBuilder<K, V> builder) {
        if (conf.logging().contextEnabled() == null) {
            builder.logging(logging -> logging
                    .contextEnabled(defaults.logging().contextEnabled()));
        }
        if (conf.maintenance().segmentThreads() == null) {
            builder.maintenance(maintenance -> maintenance.segmentThreads(
                    defaults.maintenance().segmentThreads()));
        }
        if (conf.maintenance().indexThreads() == null) {
            builder.maintenance(maintenance -> maintenance.indexThreads(
                    defaults.maintenance().indexThreads()));
        }
        if (conf.maintenance().registryLifecycleThreads() == null) {
            builder.maintenance(maintenance -> maintenance
                    .registryLifecycleThreads(
                            defaults.maintenance().registryLifecycleThreads()));
        }
        if (conf.maintenance().busyBackoffMillis() == null) {
            builder.maintenance(maintenance -> maintenance.busyBackoffMillis(
                    defaults.maintenance().busyBackoffMillis()));
        }
        if (conf.maintenance().busyTimeoutMillis() == null) {
            builder.maintenance(maintenance -> maintenance.busyTimeoutMillis(
                    defaults.maintenance().busyTimeoutMillis()));
        }
        if (conf.maintenance().backgroundAutoEnabled() == null) {
            builder.maintenance(maintenance -> maintenance.backgroundAutoEnabled(
                    defaults.maintenance().backgroundAutoEnabled()));
        }
        if (conf.wal() == null) {
            builder.wal(wal -> wal.configuration(defaults.wal()));
        }
        if (conf.writePath().segmentSplitKeyThreshold() == null) {
            builder.writePath(writePath -> writePath.segmentSplitKeyThreshold(
                    defaults.writePath().segmentSplitKeyThreshold()));
        }
    }

    private int applySegmentCacheDefaults(final IndexConfiguration<K, V> conf,
            final IndexConfigurationContract defaults,
            final IndexConfigurationBuilder<K, V> builder) {
        final int effectiveMaxNumberOfKeysInSegmentCache = conf
                .segment().cacheKeyLimit() == null
                        ? defaults.segment().cacheKeyLimit()
                        : conf.segment().cacheKeyLimit();
        if (conf.segment().cacheKeyLimit() == null) {
            builder.segment(segment -> segment.cacheKeyLimit(
                    effectiveMaxNumberOfKeysInSegmentCache));
        }
        return effectiveMaxNumberOfKeysInSegmentCache;
    }

    private int applyActivePartitionDefaults(final IndexConfiguration<K, V> conf,
            final IndexConfigurationBuilder<K, V> builder,
            final int effectiveMaxNumberOfKeysInSegmentCache) {
        if (conf.writePath().segmentWriteCacheKeyLimit() == null) {
            final int effectiveActivePartitionLimit = Math.max(1,
                    effectiveMaxNumberOfKeysInSegmentCache / 2);
            builder.writePath(writePath -> writePath.segmentWriteCacheKeyLimit(
                    effectiveActivePartitionLimit));
            return effectiveActivePartitionLimit;
        }
        return conf.writePath().segmentWriteCacheKeyLimit();
    }

    private void applyPartitionBufferDefaults(
            final IndexConfiguration<K, V> conf,
            final IndexConfigurationBuilder<K, V> builder,
            final int effectiveActivePartitionLimit) {
        if (conf.writePath()
                .segmentWriteCacheKeyLimitDuringMaintenance() == null) {
            final int effectivePartitionBufferLimit = Math.max(
                    (int) Math.ceil(effectiveActivePartitionLimit * 1.4),
                    effectiveActivePartitionLimit + 1);
            builder.writePath(writePath -> writePath
                    .maintenanceWriteCacheKeyLimit(
                            effectivePartitionBufferLimit));
        }
    }

    private void applyRemainingSegmentDefaults(
            final IndexConfiguration<K, V> conf,
            final IndexConfigurationContract defaults,
            final IndexConfigurationBuilder<K, V> builder) {
        if (conf.segment().maxKeys() == null) {
            builder.segment(segment -> segment
                    .maxKeys(defaults.segment().maxKeys()));
        }
        if (conf.segment().cachedSegmentLimit() == null) {
            builder.segment(segment -> segment.cachedSegmentLimit(
                    defaults.segment().cachedSegmentLimit()));
        }
        if (conf.segment().chunkKeyLimit() == null) {
            builder.segment(segment -> segment
                    .chunkKeyLimit(defaults.segment().chunkKeyLimit()));
        }
        if (conf.segment().deltaCacheFileLimit() == null) {
            builder.segment(segment -> segment.deltaCacheFileLimit(
                    defaults.segment().deltaCacheFileLimit()));
        }
    }

    private void applyBloomAndIoDefaults(final IndexConfiguration<K, V> conf,
            final IndexConfigurationContract defaults,
            final IndexConfigurationBuilder<K, V> builder) {
        if (conf.bloomFilter().indexSizeBytes() == null) {
            builder.bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(
                    defaults.bloomFilter().indexSizeBytes()));
        }
        if (conf.bloomFilter().hashFunctions() == null) {
            builder.bloomFilter(bloomFilter -> bloomFilter.hashFunctions(
                    defaults.bloomFilter().hashFunctions()));
        }
        if (conf.bloomFilter().falsePositiveProbability() == null) {
            builder.bloomFilter(bloomFilter -> bloomFilter
                    .falsePositiveProbability(
                            defaults.bloomFilter()
                                    .falsePositiveProbability()));
        }
        if (conf.io().diskBufferSizeBytes() == null) {
            builder.io(io -> io
                    .diskBufferSizeBytes(defaults.io().diskBufferSizeBytes()));
        }
    }

    private void applyChunkFilterDefaults(final IndexConfiguration<K, V> conf,
            final IndexConfigurationContract defaults,
            final IndexConfigurationBuilder<K, V> builder) {
        if (conf.filters().encodingChunkFilterSpecs().isEmpty()) {
            builder.filters(filters -> filters
                    .encodingFilterSpecs(
                            defaults.filters().encodingChunkFilterSpecs()));
        }
        if (conf.filters().decodingChunkFilterSpecs().isEmpty()) {
            builder.filters(filters -> filters
                    .decodingFilterSpecs(
                            defaults.filters().decodingChunkFilterSpecs()));
        }
    }

    /**
     * Saves the configuration to the storage.
     * 
     * @param indexConfiguration configuration to save
     * @throws IllegalArgumentException when given parameter try to overrinde
     */
    public void save(final IndexConfiguration<K, V> indexConfiguration) {
        confStorage.save(validate(indexConfiguration));
    }

    /**
     * Merges the configuration with the stored one.
     * 
     * @throws IllegalArgumentException when given parameter try to overrinde
     * @param indexConf parameter that can't be overriden
     * 
     * @return
     */
    public IndexConfiguration<K, V> mergeWithStored(
            final IndexConfiguration<K, V> indexConf) {
        final IndexConfiguration<K, V> storedConf = confStorage.load();
        final IndexConfigurationBuilder<K, V> builder = makeBuilder(storedConf);
        builder.filters(filters -> filters.chunkFilterProviderResolver(
                indexConf.filters().getChunkFilterProviderResolver()));
        validateThatFixedPropertiesAreNotOverridden(storedConf, indexConf);
        boolean dirty = false;
        dirty |= applyBasicOverrides(builder, storedConf, indexConf);
        dirty |= applyThreadAndBusyOverrides(builder, storedConf, indexConf);
        dirty |= applyBooleanOverrides(builder, storedConf, indexConf);
        final IndexConfiguration<K, V> merged = builder.build();
        if (dirty) {
            confStorage.save(merged);
        }
        return validate(merged);
    }

    private boolean applyBasicOverrides(
            final IndexConfigurationBuilder<K, V> builder,
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        boolean dirty = false;
        dirty |= applyIf(isIndexNameOverridden(storedConf, indexConf),
                () -> builder.identity(identity -> identity
                        .name(indexConf.identity().name())));
        dirty |= applyIf(isDiskIoBufferSizeOverridden(storedConf, indexConf),
                () -> builder.io(io -> io.diskBufferSizeBytes(
                        indexConf.io().diskBufferSizeBytes())));
        dirty |= applyIf(
                isMaxNumberOfKeysInSegmentCacheOverridden(storedConf, indexConf),
                () -> builder.segment(segment -> segment.cacheKeyLimit(
                        indexConf.segment().cacheKeyLimit())));
        dirty |= applyIf(
                isMaxNumberOfKeysInActivePartitionOverridden(storedConf,
                        indexConf),
                () -> builder.writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(indexConf.writePath()
                                .segmentWriteCacheKeyLimit())));
        dirty |= applyIf(
                isMaxNumberOfKeysInPartitionBufferOverridden(
                        storedConf, indexConf),
                () -> builder.writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(indexConf.writePath()
                                .segmentWriteCacheKeyLimitDuringMaintenance())));
        dirty |= applyIf(isMaxNumberOfDeltaCacheFilesOverridden(storedConf,
                indexConf), () -> builder.segment(segment -> segment
                        .deltaCacheFileLimit(
                                indexConf.segment().deltaCacheFileLimit())));
        return dirty;
    }

    private boolean applyThreadAndBusyOverrides(
            final IndexConfigurationBuilder<K, V> builder,
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        boolean dirty = false;
        dirty |= applyIf(
                isPositiveOverride(
                        indexConf.maintenance().segmentThreads(),
                        storedConf.maintenance().segmentThreads()),
                () -> builder.maintenance(maintenance -> maintenance
                        .segmentThreads(
                                indexConf.maintenance().segmentThreads())));
        dirty |= applyIf(
                isPositiveOverride(indexConf.maintenance().indexThreads(),
                        storedConf.maintenance().indexThreads()),
                () -> builder.maintenance(maintenance -> maintenance
                        .indexThreads(indexConf.maintenance().indexThreads())));
        dirty |= applyIf(
                isPositiveOverride(
                        indexConf.maintenance().registryLifecycleThreads(),
                        storedConf.maintenance().registryLifecycleThreads()),
                () -> builder.maintenance(maintenance -> maintenance
                        .registryLifecycleThreads(indexConf.maintenance()
                                .registryLifecycleThreads())));
        dirty |= applyIf(
                isPositiveOverride(indexConf.maintenance().busyBackoffMillis(),
                        storedConf.maintenance().busyBackoffMillis()),
                () -> builder.maintenance(maintenance -> maintenance
                        .busyBackoffMillis(
                                indexConf.maintenance().busyBackoffMillis())));
        dirty |= applyIf(
                isPositiveOverride(indexConf.maintenance().busyTimeoutMillis(),
                        storedConf.maintenance().busyTimeoutMillis()),
                () -> builder.maintenance(maintenance -> maintenance
                        .busyTimeoutMillis(
                                indexConf.maintenance().busyTimeoutMillis())));
        return dirty;
    }

    private boolean applyBooleanOverrides(
            final IndexConfigurationBuilder<K, V> builder,
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        boolean dirty = false;
        dirty |= applyIf(
                isBooleanOverride(indexConf.maintenance().backgroundAutoEnabled(),
                        storedConf.maintenance().backgroundAutoEnabled()),
                () -> builder.maintenance(maintenance -> maintenance
                        .backgroundAutoEnabled(indexConf.maintenance()
                                .backgroundAutoEnabled())));
        dirty |= applyIf(
                isBooleanOverride(indexConf.logging().contextEnabled(),
                        storedConf.logging().contextEnabled()),
                () -> builder.logging(logging -> logging
                        .contextEnabled(indexConf.logging().contextEnabled())));
        return dirty;
    }

    private boolean applyIf(final boolean condition, final Runnable applier) {
        if (!condition) {
            return false;
        }
        applier.run();
        return true;
    }

    private static boolean isPositiveOverride(final Integer candidate,
            final Integer stored) {
        return candidate != null && candidate > 0 && !candidate.equals(stored);
    }

    private static boolean isBooleanOverride(final Boolean candidate,
            final Boolean stored) {
        return candidate != null && !candidate.equals(stored);
    }

    private boolean isIndexNameOverridden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.identity().name() != null
                && !indexConf.identity().name()
                        .equals(storedConf.identity().name());
    }

    private boolean isDiskIoBufferSizeOverridden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.io().diskBufferSizeBytes() != null
                && indexConf.io().diskBufferSizeBytes() > 0
                && !indexConf.io().diskBufferSizeBytes()
                        .equals(storedConf.io().diskBufferSizeBytes());
    }

    private boolean isMaxNumberOfKeysInSegmentCacheOverridden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.segment().cacheKeyLimit() != null
                && indexConf.segment().cacheKeyLimit() > 0
                && !indexConf.segment().cacheKeyLimit()
                        .equals(storedConf.segment().cacheKeyLimit());
    }

    private boolean isMaxNumberOfKeysInActivePartitionOverridden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.writePath().segmentWriteCacheKeyLimit() != null
                && indexConf.writePath().segmentWriteCacheKeyLimit() > 0
                && !indexConf.writePath().segmentWriteCacheKeyLimit()
                        .equals(storedConf.writePath()
                                .segmentWriteCacheKeyLimit());
    }

    private boolean isMaxNumberOfKeysInPartitionBufferOverridden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.writePath()
                .segmentWriteCacheKeyLimitDuringMaintenance() != null
                && indexConf.writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance() > 0
                && !indexConf.writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance()
                        .equals(storedConf.writePath()
                                .segmentWriteCacheKeyLimitDuringMaintenance());
    }

    private boolean isMaxNumberOfDeltaCacheFilesOverridden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.segment().deltaCacheFileLimit() != null
                && indexConf.segment().deltaCacheFileLimit() > 0
                && !indexConf.segment().deltaCacheFileLimit()
                        .equals(storedConf.segment().deltaCacheFileLimit());
    }

    void validateThatFixedPropertiesAreNotOverridden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        validateClassNotChanged(indexConf.identity().keyClass(),
                storedConf.identity().keyClass(), "KeyClass");
        validateClassNotChanged(indexConf.identity().valueClass(),
                storedConf.identity().valueClass(), "ValueClass");

        throwIfChanged(
                isChanged(indexConf.identity().keyTypeDescriptor(),
                        storedConf.identity().keyTypeDescriptor()),
                "KeyTypeDescriptor", storedConf.identity().keyTypeDescriptor(),
                indexConf.identity().keyTypeDescriptor());
        throwIfChanged(
                isChanged(indexConf.identity().valueTypeDescriptor(),
                        storedConf.identity().valueTypeDescriptor()),
                "ValueTypeDescriptor",
                storedConf.identity().valueTypeDescriptor(),
                indexConf.identity().valueTypeDescriptor());
        throwIfChanged(
                isPositiveOverride(indexConf.segment().maxKeys(),
                        storedConf.segment().maxKeys()),
                "MaxNumberOfKeysInSegment",
                storedConf.segment().maxKeys(),
                indexConf.segment().maxKeys());
        throwIfChanged(
                isPositiveOverride(indexConf.segment().chunkKeyLimit(),
                        storedConf.segment().chunkKeyLimit()),
                "MaxNumberOfKeysInSegmentChunk",
                storedConf.segment().chunkKeyLimit(),
                indexConf.segment().chunkKeyLimit());
        throwIfChanged(
                isPositiveOverride(indexConf.bloomFilter().indexSizeBytes(),
                        storedConf.bloomFilter().indexSizeBytes()),
                "BloomFilterIndexSizeInBytes",
                storedConf.bloomFilter().indexSizeBytes(),
                indexConf.bloomFilter().indexSizeBytes());
        throwIfChanged(
                isPositiveOverride(indexConf.bloomFilter().hashFunctions(),
                        storedConf.bloomFilter().hashFunctions()),
                "BloomFilterNumberOfHashFunctions",
                storedConf.bloomFilter().hashFunctions(),
                indexConf.bloomFilter().hashFunctions());
        throwIfChanged(
                isChanged(
                        indexConf.bloomFilter().falsePositiveProbability(),
                        storedConf.bloomFilter().falsePositiveProbability()),
                "BloomFilterProbabilityOfFalsePositive",
                storedConf.bloomFilter().falsePositiveProbability(),
                indexConf.bloomFilter().falsePositiveProbability());
        throwIfChanged(
                chunkFiltersChanged(
                        indexConf.filters().encodingChunkFilterSpecs(),
                        storedConf.filters().encodingChunkFilterSpecs()),
                "EncodingChunkFilters",
                storedConf.filters().encodingChunkFilterSpecs(),
                indexConf.filters().encodingChunkFilterSpecs());
        throwIfChanged(
                chunkFiltersChanged(
                        indexConf.filters().decodingChunkFilterSpecs(),
                        storedConf.filters().decodingChunkFilterSpecs()),
                "DecodingChunkFilters",
                storedConf.filters().decodingChunkFilterSpecs(),
                indexConf.filters().decodingChunkFilterSpecs());
        throwIfChanged(
                isChanged(indexConf.wal(), storedConf.wal()),
                "IndexWalConfiguration", storedConf.wal(), indexConf.wal());
    }

    private boolean chunkFiltersChanged(final List<ChunkFilterSpec> indexFilters,
            final List<ChunkFilterSpec> storedFilters) {
        if (indexFilters == null) {
            return false;
        }
        if (indexFilters.isEmpty()) {
            return false;
        }
        return !canonicalizeChunkFilterSpecs(indexFilters)
                .equals(canonicalizeChunkFilterSpecs(storedFilters));
    }

    private List<ChunkFilterSpec> canonicalizeChunkFilterSpecs(
            final List<ChunkFilterSpec> specs) {
        return Vldtn.requireNonNull(specs, "specs").stream()
                .map(ChunkFilterSpecs::canonicalize).toList();
    }

    private void validateClassNotChanged(final Class<?> requested,
            final Class<?> stored, final String propertyName) {
        if (requested == null) {
            return;
        }
        throwIfChanged(!requested.equals(stored), propertyName,
                stored.getName(), requested.getName());
    }

    private static <T> boolean isChanged(final T candidate, final T stored) {
        return candidate != null && !candidate.equals(stored);
    }

    private void throwIfChanged(final boolean wasChanged,
            final String propertyName, Object storedValue, Object newValue) {
        if (wasChanged) {
            throw new IllegalArgumentException(String.format(
                    "Value of '%s' is already set"
                            + " to '%s' and can't be changed to '%s'",
                    propertyName, storedValue, newValue));
        }
    }

    private IndexConfiguration<K, V> validate(
            final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(conf, "configuration");
        validateDatatypesAndIndexName(conf);
        validateMandatoryFields(conf);
        validateSegmentLimits(conf);
        validatePartitionBufferLimits(conf);
        validateDiskIoBuffer(conf);
        validateChunkFilters(conf);
        validateThreading(conf);
        validateBusyPolicy(conf);
        Vldtn.requireNonNull(conf.maintenance().backgroundAutoEnabled(),
                "backgroundMaintenanceAutoEnabled");
        Vldtn.requireNonNull(conf.wal(), "wal");
        validateWal(conf.wal());
        return conf;
    }

    private void validateWal(final IndexWalConfiguration wal) {
        if (!wal.isEnabled()) {
            return;
        }
        if (wal.getSegmentSizeBytes() <= 0L) {
            throw new IllegalArgumentException(
                    "IndexWalConfiguration segment size must be greater than zero.");
        }
        if (wal.getGroupSyncDelayMillis() < 0) {
            throw new IllegalArgumentException(
                    "IndexWalConfiguration group sync delay must be greater "
                            + "than or equal to zero.");
        }
        if (wal.getGroupSyncMaxBatchBytes() <= 0) {
            throw new IllegalArgumentException(
                    "IndexWalConfiguration group sync max batch bytes must be "
                            + "greater than zero.");
        }
        if (wal.getMaxBytesBeforeForcedCheckpoint() <= 0L) {
            throw new IllegalArgumentException(
                    "IndexWalConfiguration max bytes before forced checkpoint "
                            + "must be greater than zero.");
        }
    }

    private void validateMandatoryFields(final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(conf.logging().contextEnabled(),
                "isContextLoggingEnabled");
    }

    private void validateSegmentLimits(final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(conf.segment().maxKeys(),
                "MaxNumberOfKeysInSegment");
        if (conf.segment().maxKeys() < 4) {
            throw new IllegalArgumentException(
                    "Max number of keys in segment must be at least 4.");
        }
        Vldtn.requireNonNull(conf.writePath().segmentSplitKeyThreshold(),
                "MaxNumberOfKeysInPartitionBeforeSplit");
        if (conf.writePath().segmentSplitKeyThreshold() < 1) {
            throw new IllegalArgumentException(
                    "Max number of keys in partition before split must be at least 1.");
        }
        Vldtn.requireNonNull(conf.segment().cachedSegmentLimit(),
                "MaxNumberOfSegmentsInCache");
        if (conf.segment().cachedSegmentLimit() < 3) {
            throw new IllegalArgumentException(
                    "Max number of segments in cache must be at least 3.");
        }
        Vldtn.requireNonNull(conf.segment().deltaCacheFileLimit(),
                "MaxNumberOfDeltaCacheFiles");
        if (conf.segment().deltaCacheFileLimit() < 1) {
            throw new IllegalArgumentException(
                    "Max number of delta cache files must be at least 1.");
        }
    }

    private void validatePartitionBufferLimits(final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(conf.writePath().segmentWriteCacheKeyLimit(),
                "MaxNumberOfKeysInActivePartition");
        if (conf.writePath().segmentWriteCacheKeyLimit() < 1) {
            throw new IllegalArgumentException(
                    "Max number of keys in active partition (legacy-named routed write-cache limit) must be at least 1.");
        }
        final int segmentWriteCacheKeyLimit = conf.writePath()
                .segmentWriteCacheKeyLimit();
        final Integer maintenanceWriteCacheKeyLimit = conf.writePath()
                .segmentWriteCacheKeyLimitDuringMaintenance();
        final int effectiveMaxDuringMaintenance = maintenanceWriteCacheKeyLimit == null
                        ? Math.max(
                                segmentWriteCacheKeyLimit * 2,
                                segmentWriteCacheKeyLimit + 1)
                        : maintenanceWriteCacheKeyLimit.intValue();
        if (effectiveMaxDuringMaintenance <= segmentWriteCacheKeyLimit) {
            throw new IllegalArgumentException(
                    "Max number of keys in partition buffer (legacy-named per-segment backlog limit) must be greater than the active partition limit.");
        }
    }

    private void validateDiskIoBuffer(final IndexConfiguration<K, V> conf) {
        final Integer diskBufferSizeBytes = conf.io().diskBufferSizeBytes();
        Vldtn.requireNonNull(diskBufferSizeBytes, "DiskIoBufferSize");
        if (diskBufferSizeBytes <= 0) {
            throw new IllegalArgumentException(String.format(
                    "Parameter 'diskIoBufferSize' with value '%s'"
                            + " can't be smaller or equal to zero.",
                    diskBufferSizeBytes));
        }
        if (diskBufferSizeBytes % 1024 != 0) {
            throw new IllegalArgumentException(String.format(
                    "Parameter 'diskIoBufferSize' with value '%s'"
                            + " can't be divided by 1024 without reminder",
                    diskBufferSizeBytes));
        }
    }

    private void validateChunkFilters(final IndexConfiguration<K, V> conf) {
        if (conf.filters().encodingChunkFilterSpecs() == null
                || conf.filters().encodingChunkFilterSpecs().isEmpty()) {
            throw new IllegalArgumentException(
                    "Encoding chunk filters must not be empty.");
        }
        if (conf.filters().decodingChunkFilterSpecs() == null
                || conf.filters().decodingChunkFilterSpecs().isEmpty()) {
            throw new IllegalArgumentException(
                    "Decoding chunk filters must not be empty.");
        }
    }

    private void validateThreading(final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(conf.maintenance().segmentThreads(),
                "numberOfSegmentMaintenanceThreads");
        if (conf.maintenance().segmentThreads() < 1) {
            throw new IllegalArgumentException(
                    "Segment maintenance threads must be at least 1.");
        }
        Vldtn.requireNonNull(conf.maintenance().indexThreads(),
                "indexMaintenanceThreads");
        if (conf.maintenance().indexThreads() < 1) {
            throw new IllegalArgumentException(
                    "Index maintenance threads must be at least 1.");
        }
        Vldtn.requireNonNull(conf.maintenance().registryLifecycleThreads(),
                "registryLifecycleThreads");
        if (conf.maintenance().registryLifecycleThreads() < 1) {
            throw new IllegalArgumentException(
                    "Registry lifecycle threads must be at least 1.");
        }
    }

    private void validateBusyPolicy(final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(conf.maintenance().busyBackoffMillis(),
                "indexBusyBackoffMillis");
        if (conf.maintenance().busyBackoffMillis() < 1) {
            throw new IllegalArgumentException(
                    "Index busy backoff must be at least 1 ms.");
        }
        Vldtn.requireNonNull(conf.maintenance().busyTimeoutMillis(),
                "indexBusyTimeoutMillis");
        if (conf.maintenance().busyTimeoutMillis() < 1) {
            throw new IllegalArgumentException(
                    "Index busy timeout must be at least 1 ms.");
        }
    }

    private void validateRequiredDatatypesAndIndexName(
            final IndexConfiguration<K, V> conf) {
        if (conf.identity().keyClass() == null) {
            throw new IllegalArgumentException("Key class wasn't specified");
        }
        if (conf.identity().valueClass() == null) {
            throw new IllegalArgumentException("Value class wasn't specified");
        }
        Vldtn.requireNotBlank(conf.identity().name(), "indexName");
    }

    private void validateDatatypesAndIndexName(
            final IndexConfiguration<K, V> conf) {
        validateRequiredDatatypesAndIndexName(conf);
        Vldtn.requireNotBlank(conf.identity().keyTypeDescriptor(),
                "keyTypeDescriptor");
        Vldtn.requireNotBlank(conf.identity().valueTypeDescriptor(),
                "valueTypeDescriptor");
    }

    private IndexConfigurationBuilder<K, V> makeBuilder(
            final IndexConfiguration<K, V> conf) {
        return IndexConfiguration.<K, V>builder()
                .identity(identity -> identity
                        .keyClass(conf.identity().keyClass())
                        .valueClass(conf.identity().valueClass())
                        .keyTypeDescriptor(
                                conf.identity().keyTypeDescriptor())
                        .valueTypeDescriptor(
                                conf.identity().valueTypeDescriptor())
                        .name(conf.identity().name()))
                .logging(logging -> logging
                        .contextEnabled(conf.logging().contextEnabled()))
                .maintenance(maintenance -> maintenance
                        .segmentThreads(conf.maintenance().segmentThreads())
                        .indexThreads(conf.maintenance().indexThreads())
                        .registryLifecycleThreads(conf.maintenance()
                                .registryLifecycleThreads())
                        .busyBackoffMillis(
                                conf.maintenance().busyBackoffMillis())
                        .busyTimeoutMillis(
                                conf.maintenance().busyTimeoutMillis())
                        .backgroundAutoEnabled(
                                conf.maintenance().backgroundAutoEnabled()))
                .wal(wal -> wal.configuration(conf.wal()))
                .segment(segment -> segment
                        .cachedSegmentLimit(
                                conf.segment().cachedSegmentLimit())
                        .maxKeys(conf.segment().maxKeys())
                        .cacheKeyLimit(conf.segment().cacheKeyLimit())
                        .chunkKeyLimit(conf.segment().chunkKeyLimit())
                        .deltaCacheFileLimit(
                                conf.segment().deltaCacheFileLimit()))
                .writePath(writePath -> writePath
                        .segmentSplitKeyThreshold(conf.writePath()
                                .segmentSplitKeyThreshold())
                        .segmentWriteCacheKeyLimit(conf.writePath()
                                .segmentWriteCacheKeyLimit())
                        .maintenanceWriteCacheKeyLimit(conf.writePath()
                                .segmentWriteCacheKeyLimitDuringMaintenance())
                        .legacyImmutableRunLimit(
                                conf.runtimeTuning().legacyImmutableRunLimit())
                        .indexBufferedWriteKeyLimit(conf.writePath()
                                .indexBufferedWriteKeyLimit()))
                .io(io -> io.diskBufferSizeBytes(
                        conf.io().diskBufferSizeBytes()))
                .bloomFilter(bloomFilter -> bloomFilter
                        .hashFunctions(conf.bloomFilter().hashFunctions())
                        .indexSizeBytes(conf.bloomFilter().indexSizeBytes())
                        .falsePositiveProbability(conf.bloomFilter()
                                .falsePositiveProbability()))
                .filters(filters -> filters
                        .chunkFilterProviderResolver(conf.filters()
                                .getChunkFilterProviderResolver())
                        .encodingFilterSpecs(
                                conf.filters().encodingChunkFilterSpecs())
                        .decodingFilterSpecs(
                                conf.filters().decodingChunkFilterSpecs()));
    }

    private void debugWithIndexContext(final IndexConfiguration<K, V> conf,
            final String message, final Object arg) {
        if (!logger.isDebugEnabled()) {
            return;
        }
        final String previousIndexName = MDC.get(INDEX_NAME_MDC_KEY);
        final boolean contextApplied = applyIndexContext(conf);
        try {
            logger.debug(message, arg);
        } finally {
            if (contextApplied) {
                restorePreviousIndexName(previousIndexName);
            }
        }
    }

    private static <K, V> boolean applyIndexContext(
            final IndexConfiguration<K, V> conf) {
        if (!Boolean.TRUE.equals(conf.logging().contextEnabled())) {
            return false;
        }
        final String indexName = normalizeIndexName(conf.identity().name());
        if (indexName == null) {
            return false;
        }
        MDC.put(INDEX_NAME_MDC_KEY, indexName);
        return true;
    }

    private static String normalizeIndexName(final String indexName) {
        if (indexName == null) {
            return null;
        }
        final String normalized = indexName.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private static void restorePreviousIndexName(
            final String previousIndexName) {
        if (previousIndexName == null) {
            MDC.remove(INDEX_NAME_MDC_KEY);
            return;
        }
        MDC.put(INDEX_NAME_MDC_KEY, previousIndexName);
    }

}
