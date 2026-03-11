package org.hestiastore.index.segmentindex.config;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationBuilder;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.Wal;
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
                .get(conf.getKeyClass());
        if (defaultsOpt.isEmpty()) {
            debugWithIndexContext(conf,
                    "There is no default configuration for key class '{}'",
                    conf.getKeyClass());
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
        if (conf.getKeyTypeDescriptor() == null) {
            builder.withKeyTypeDescriptor(DataTypeDescriptorRegistry
                    .getTypeDescriptor(conf.getKeyClass()));
        }
        if (conf.getValueTypeDescriptor() == null) {
            builder.withValueTypeDescriptor(DataTypeDescriptorRegistry
                    .getTypeDescriptor(conf.getValueClass()));
        }
    }

    private void applyCoreDefaults(final IndexConfiguration<K, V> conf,
            final IndexConfigurationContract defaults,
            final IndexConfigurationBuilder<K, V> builder) {
        if (conf.isContextLoggingEnabled() == null) {
            builder.withContextLoggingEnabled(
                    defaults.isContextLoggingEnabled());
        }
        if (conf.getIndexWorkerThreadCount() == null) {
            builder.withIndexWorkerThreadCount(defaults.getIndexWorkerThreadCount());
        }
        if (conf.getNumberOfSegmentIndexMaintenanceThreads() == null) {
            builder.withNumberOfSegmentIndexMaintenanceThreads(
                    defaults.getNumberOfSegmentIndexMaintenanceThreads());
        }
        if (conf.getNumberOfIndexMaintenanceThreads() == null) {
            builder.withNumberOfIndexMaintenanceThreads(
                    defaults.getNumberOfIndexMaintenanceThreads());
        }
        if (conf.getNumberOfRegistryLifecycleThreads() == null) {
            builder.withNumberOfRegistryLifecycleThreads(
                    defaults.getNumberOfRegistryLifecycleThreads());
        }
        if (conf.getIndexBusyBackoffMillis() == null) {
            builder.withIndexBusyBackoffMillis(
                    defaults.getIndexBusyBackoffMillis());
        }
        if (conf.getIndexBusyTimeoutMillis() == null) {
            builder.withIndexBusyTimeoutMillis(
                    defaults.getIndexBusyTimeoutMillis());
        }
        if (conf.isSegmentMaintenanceAutoEnabled() == null) {
            builder.withSegmentMaintenanceAutoEnabled(
                    defaults.isSegmentMaintenanceAutoEnabled());
        }
        if (conf.getWal() == null) {
            builder.withWal(defaults.getWal());
        }
        if (conf.getMaxNumberOfKeysInPartitionBeforeSplit() == null) {
            builder.withMaxNumberOfKeysInPartitionBeforeSplit(
                    defaults.getMaxNumberOfKeysInPartitionBeforeSplit());
        }
    }

    private int applySegmentCacheDefaults(final IndexConfiguration<K, V> conf,
            final IndexConfigurationContract defaults,
            final IndexConfigurationBuilder<K, V> builder) {
        final int effectiveMaxNumberOfKeysInSegmentCache = conf
                .getMaxNumberOfKeysInSegmentCache() == null
                        ? defaults.getMaxNumberOfKeysInSegmentCache()
                        : conf.getMaxNumberOfKeysInSegmentCache();
        if (conf.getMaxNumberOfKeysInSegmentCache() == null) {
            builder.withMaxNumberOfKeysInSegmentCache(
                    effectiveMaxNumberOfKeysInSegmentCache);
        }
        return effectiveMaxNumberOfKeysInSegmentCache;
    }

    private int applyActivePartitionDefaults(final IndexConfiguration<K, V> conf,
            final IndexConfigurationBuilder<K, V> builder,
            final int effectiveMaxNumberOfKeysInSegmentCache) {
        if (conf.getMaxNumberOfKeysInActivePartition() == null) {
            final int effectiveActivePartitionLimit = Math.max(1,
                    effectiveMaxNumberOfKeysInSegmentCache / 2);
            builder.withMaxNumberOfKeysInActivePartition(
                    effectiveActivePartitionLimit);
            return effectiveActivePartitionLimit;
        }
        return conf.getMaxNumberOfKeysInActivePartition();
    }

    private void applyPartitionBufferDefaults(
            final IndexConfiguration<K, V> conf,
            final IndexConfigurationBuilder<K, V> builder,
            final int effectiveActivePartitionLimit) {
        if (conf.getMaxNumberOfKeysInPartitionBuffer() == null) {
            final int effectivePartitionBufferLimit = Math.max(
                    (int) Math.ceil(effectiveActivePartitionLimit * 1.4),
                    effectiveActivePartitionLimit + 1);
            builder.withMaxNumberOfKeysInPartitionBuffer(
                    effectivePartitionBufferLimit);
        }
    }

    private void applyRemainingSegmentDefaults(
            final IndexConfiguration<K, V> conf,
            final IndexConfigurationContract defaults,
            final IndexConfigurationBuilder<K, V> builder) {
        if (conf.getMaxNumberOfKeysInSegment() == null) {
            builder.withMaxNumberOfKeysInSegment(
                    defaults.getMaxNumberOfKeysInSegment());
        }
        if (conf.getMaxNumberOfSegmentsInCache() == null) {
            builder.withMaxNumberOfSegmentsInCache(
                    defaults.getMaxNumberOfSegmentsInCache());
        }
        if (conf.getMaxNumberOfKeysInSegmentChunk() == null) {
            builder.withMaxNumberOfKeysInSegmentChunk(
                    defaults.getMaxNumberOfKeysInSegmentChunk());
        }
        if (conf.getMaxNumberOfDeltaCacheFiles() == null) {
            builder.withMaxNumberOfDeltaCacheFiles(
                    defaults.getMaxNumberOfDeltaCacheFiles());
        }
    }

    private void applyBloomAndIoDefaults(final IndexConfiguration<K, V> conf,
            final IndexConfigurationContract defaults,
            final IndexConfigurationBuilder<K, V> builder) {
        if (conf.getBloomFilterIndexSizeInBytes() == null) {
            builder.withBloomFilterIndexSizeInBytes(
                    defaults.getBloomFilterIndexSizeInBytes());
        }
        if (conf.getBloomFilterNumberOfHashFunctions() == null) {
            builder.withBloomFilterNumberOfHashFunctions(
                    defaults.getBloomFilterNumberOfHashFunctions());
        }
        if (conf.getBloomFilterProbabilityOfFalsePositive() == null) {
            builder.withBloomFilterProbabilityOfFalsePositive(
                    defaults.getBloomFilterProbabilityOfFalsePositive());
        }
        if (conf.getDiskIoBufferSize() == null) {
            builder.withDiskIoBufferSizeInBytes(
                    defaults.getDiskIoBufferSizeInBytes());
        }
    }

    private void applyChunkFilterDefaults(final IndexConfiguration<K, V> conf,
            final IndexConfigurationContract defaults,
            final IndexConfigurationBuilder<K, V> builder) {
        if (conf.getEncodingChunkFilters().isEmpty()) {
            builder.withEncodingFilters(defaults.getEncodingChunkFilters());
        }
        if (conf.getDecodingChunkFilters().isEmpty()) {
            builder.withDecodingFilters(defaults.getDecodingChunkFilters());
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
                () -> builder.withName(indexConf.getIndexName()));
        dirty |= applyIf(isDiskIoBufferSizeOverridden(storedConf, indexConf),
                () -> builder.withDiskIoBufferSizeInBytes(
                        indexConf.getDiskIoBufferSize()));
        dirty |= applyIf(
                isMaxNumberOfKeysInSegmentCacheOverridden(storedConf, indexConf),
                () -> builder.withMaxNumberOfKeysInSegmentCache(
                        indexConf.getMaxNumberOfKeysInSegmentCache()));
        dirty |= applyIf(
                isMaxNumberOfKeysInActivePartitionOverridden(storedConf,
                        indexConf),
                () -> builder.withMaxNumberOfKeysInActivePartition(
                        indexConf.getMaxNumberOfKeysInActivePartition()));
        dirty |= applyIf(
                isMaxNumberOfKeysInPartitionBufferOverridden(
                        storedConf, indexConf),
                () -> builder.withMaxNumberOfKeysInPartitionBuffer(
                        indexConf.getMaxNumberOfKeysInPartitionBuffer()));
        dirty |= applyIf(isMaxNumberOfDeltaCacheFilesOverridden(storedConf,
                indexConf), () -> builder.withMaxNumberOfDeltaCacheFiles(
                        indexConf.getMaxNumberOfDeltaCacheFiles()));
        return dirty;
    }

    private boolean applyThreadAndBusyOverrides(
            final IndexConfigurationBuilder<K, V> builder,
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        boolean dirty = false;
        dirty |= applyIf(
                isPositiveOverride(indexConf.getIndexWorkerThreadCount(),
                        storedConf.getIndexWorkerThreadCount()),
                () -> builder.withIndexWorkerThreadCount(
                        indexConf.getIndexWorkerThreadCount()));
        dirty |= applyIf(
                isPositiveOverride(
                        indexConf.getNumberOfSegmentIndexMaintenanceThreads(),
                        storedConf.getNumberOfSegmentIndexMaintenanceThreads()),
                () -> builder.withNumberOfSegmentIndexMaintenanceThreads(
                        indexConf.getNumberOfSegmentIndexMaintenanceThreads()));
        dirty |= applyIf(
                isPositiveOverride(indexConf.getNumberOfIndexMaintenanceThreads(),
                        storedConf.getNumberOfIndexMaintenanceThreads()),
                () -> builder.withNumberOfIndexMaintenanceThreads(
                        indexConf.getNumberOfIndexMaintenanceThreads()));
        dirty |= applyIf(
                isPositiveOverride(
                        indexConf.getNumberOfRegistryLifecycleThreads(),
                        storedConf.getNumberOfRegistryLifecycleThreads()),
                () -> builder.withNumberOfRegistryLifecycleThreads(
                        indexConf.getNumberOfRegistryLifecycleThreads()));
        dirty |= applyIf(
                isPositiveOverride(indexConf.getIndexBusyBackoffMillis(),
                        storedConf.getIndexBusyBackoffMillis()),
                () -> builder.withIndexBusyBackoffMillis(
                        indexConf.getIndexBusyBackoffMillis()));
        dirty |= applyIf(
                isPositiveOverride(indexConf.getIndexBusyTimeoutMillis(),
                        storedConf.getIndexBusyTimeoutMillis()),
                () -> builder.withIndexBusyTimeoutMillis(
                        indexConf.getIndexBusyTimeoutMillis()));
        return dirty;
    }

    private boolean applyBooleanOverrides(
            final IndexConfigurationBuilder<K, V> builder,
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        boolean dirty = false;
        dirty |= applyIf(
                isBooleanOverride(indexConf.isSegmentMaintenanceAutoEnabled(),
                        storedConf.isSegmentMaintenanceAutoEnabled()),
                () -> builder.withSegmentMaintenanceAutoEnabled(
                        indexConf.isSegmentMaintenanceAutoEnabled()));
        dirty |= applyIf(
                isBooleanOverride(indexConf.isContextLoggingEnabled(),
                        storedConf.isContextLoggingEnabled()),
                () -> builder.withContextLoggingEnabled(
                        indexConf.isContextLoggingEnabled()));
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
        return indexConf.getIndexName() != null
                && !indexConf.getIndexName().equals(storedConf.getIndexName());
    }

    private boolean isDiskIoBufferSizeOverridden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.getDiskIoBufferSize() != null
                && indexConf.getDiskIoBufferSize() > 0
                && !indexConf.getDiskIoBufferSize()
                        .equals(storedConf.getDiskIoBufferSize());
    }

    private boolean isMaxNumberOfKeysInSegmentCacheOverridden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.getMaxNumberOfKeysInSegmentCache() != null
                && indexConf.getMaxNumberOfKeysInSegmentCache() > 0
                && !indexConf.getMaxNumberOfKeysInSegmentCache()
                        .equals(storedConf.getMaxNumberOfKeysInSegmentCache());
    }

    private boolean isMaxNumberOfKeysInActivePartitionOverridden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.getMaxNumberOfKeysInActivePartition() != null
                && indexConf.getMaxNumberOfKeysInActivePartition() > 0
                && !indexConf.getMaxNumberOfKeysInActivePartition()
                        .equals(storedConf.getMaxNumberOfKeysInActivePartition());
    }

    private boolean isMaxNumberOfKeysInPartitionBufferOverridden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.getMaxNumberOfKeysInPartitionBuffer() != null
                && indexConf.getMaxNumberOfKeysInPartitionBuffer() > 0
                && !indexConf.getMaxNumberOfKeysInPartitionBuffer()
                        .equals(storedConf.getMaxNumberOfKeysInPartitionBuffer());
    }

    private boolean isMaxNumberOfDeltaCacheFilesOverridden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.getMaxNumberOfDeltaCacheFiles() != null
                && indexConf.getMaxNumberOfDeltaCacheFiles() > 0
                && !indexConf.getMaxNumberOfDeltaCacheFiles()
                        .equals(storedConf.getMaxNumberOfDeltaCacheFiles());
    }

    void validateThatFixedPropertiesAreNotOverridden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        validateClassNotChanged(indexConf.getKeyClass(), storedConf.getKeyClass(),
                "KeyClass");
        validateClassNotChanged(indexConf.getValueClass(),
                storedConf.getValueClass(), "ValueClass");

        throwIfChanged(
                isChanged(indexConf.getKeyTypeDescriptor(),
                        storedConf.getKeyTypeDescriptor()),
                "KeyTypeDescriptor", storedConf.getKeyTypeDescriptor(),
                indexConf.getKeyTypeDescriptor());
        throwIfChanged(
                isChanged(indexConf.getValueTypeDescriptor(),
                        storedConf.getValueTypeDescriptor()),
                "ValueTypeDescriptor", storedConf.getValueTypeDescriptor(),
                indexConf.getValueTypeDescriptor());
        throwIfChanged(
                isPositiveOverride(indexConf.getMaxNumberOfKeysInSegment(),
                        storedConf.getMaxNumberOfKeysInSegment()),
                "MaxNumberOfKeysInSegment",
                storedConf.getMaxNumberOfKeysInSegment(),
                indexConf.getMaxNumberOfKeysInSegment());
        throwIfChanged(
                isPositiveOverride(indexConf.getMaxNumberOfKeysInSegmentChunk(),
                        storedConf.getMaxNumberOfKeysInSegmentChunk()),
                "MaxNumberOfKeysInSegmentChunk",
                storedConf.getMaxNumberOfKeysInSegmentChunk(),
                indexConf.getMaxNumberOfKeysInSegmentChunk());
        throwIfChanged(
                isPositiveOverride(indexConf.getBloomFilterIndexSizeInBytes(),
                        storedConf.getBloomFilterIndexSizeInBytes()),
                "BloomFilterIndexSizeInBytes",
                storedConf.getBloomFilterIndexSizeInBytes(),
                indexConf.getBloomFilterIndexSizeInBytes());
        throwIfChanged(
                isPositiveOverride(indexConf.getBloomFilterNumberOfHashFunctions(),
                        storedConf.getBloomFilterNumberOfHashFunctions()),
                "BloomFilterNumberOfHashFunctions",
                storedConf.getBloomFilterNumberOfHashFunctions(),
                indexConf.getBloomFilterNumberOfHashFunctions());
        throwIfChanged(
                isChanged(indexConf.getBloomFilterProbabilityOfFalsePositive(),
                        storedConf.getBloomFilterProbabilityOfFalsePositive()),
                "BloomFilterProbabilityOfFalsePositive",
                storedConf.getBloomFilterProbabilityOfFalsePositive(),
                indexConf.getBloomFilterProbabilityOfFalsePositive());
        throwIfChanged(
                chunkFiltersChanged(indexConf.getEncodingChunkFilters(),
                        storedConf.getEncodingChunkFilters()),
                "EncodingChunkFilters", storedConf.getEncodingChunkFilters(),
                indexConf.getEncodingChunkFilters());
        throwIfChanged(
                chunkFiltersChanged(indexConf.getDecodingChunkFilters(),
                        storedConf.getDecodingChunkFilters()),
                "DecodingChunkFilters", storedConf.getDecodingChunkFilters(),
                indexConf.getDecodingChunkFilters());
        throwIfChanged(
                isChanged(indexConf.getWal(), storedConf.getWal()), "Wal",
                storedConf.getWal(), indexConf.getWal());
    }

    static final Comparator<ChunkFilter> chunkFilterCmp = (f1, f2) -> f1
            .getClass().getName().compareTo(f2.getClass().getName());

    private <T> boolean equalLists(List<T> a, List<T> b,
            Comparator<? super T> cmp) {
        if (a.size() != b.size())
            return false;
        for (int i = 0; i < a.size(); i++) {
            if (cmp.compare(a.get(i), b.get(i)) != 0)
                return false;
        }
        return true;
    }

    private boolean chunkFiltersChanged(final List<ChunkFilter> indexFilters,
            final List<ChunkFilter> storedFilters) {
        if (indexFilters == null) {
            return false;
        }
        if (indexFilters.isEmpty()) {
            return false;
        }
        return !equalLists(indexFilters, storedFilters, chunkFilterCmp);
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
        Vldtn.requireNonNull(conf.isSegmentMaintenanceAutoEnabled(),
                "segmentMaintenanceAutoEnabled");
        Vldtn.requireNonNull(conf.getWal(), "wal");
        validateWal(conf.getWal());
        return conf;
    }

    private void validateWal(final Wal wal) {
        if (!wal.isEnabled()) {
            return;
        }
        if (wal.getSegmentSizeBytes() <= 0L) {
            throw new IllegalArgumentException(
                    "Wal segment size must be greater than zero.");
        }
        if (wal.getGroupSyncDelayMillis() < 0) {
            throw new IllegalArgumentException(
                    "Wal group sync delay must be greater than or equal to zero.");
        }
        if (wal.getGroupSyncMaxBatchBytes() <= 0) {
            throw new IllegalArgumentException(
                    "Wal group sync max batch bytes must be greater than zero.");
        }
        if (wal.getMaxBytesBeforeForcedCheckpoint() <= 0L) {
            throw new IllegalArgumentException(
                    "Wal max bytes before forced checkpoint must be greater than zero.");
        }
    }

    private void validateMandatoryFields(final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(conf.isContextLoggingEnabled(),
                "isContextLoggingEnabled");
    }

    private void validateSegmentLimits(final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(conf.getMaxNumberOfKeysInSegment(),
                "MaxNumberOfKeysInSegment");
        if (conf.getMaxNumberOfKeysInSegment() < 4) {
            throw new IllegalArgumentException(
                    "Max number of keys in segment must be at least 4.");
        }
        Vldtn.requireNonNull(conf.getMaxNumberOfKeysInPartitionBeforeSplit(),
                "MaxNumberOfKeysInPartitionBeforeSplit");
        if (conf.getMaxNumberOfKeysInPartitionBeforeSplit() < 1) {
            throw new IllegalArgumentException(
                    "Max number of keys in partition before split must be at least 1.");
        }
        Vldtn.requireNonNull(conf.getMaxNumberOfSegmentsInCache(),
                "MaxNumberOfSegmentsInCache");
        if (conf.getMaxNumberOfSegmentsInCache() < 3) {
            throw new IllegalArgumentException(
                    "Max number of segments in cache must be at least 3.");
        }
        Vldtn.requireNonNull(conf.getMaxNumberOfDeltaCacheFiles(),
                "MaxNumberOfDeltaCacheFiles");
        if (conf.getMaxNumberOfDeltaCacheFiles() < 1) {
            throw new IllegalArgumentException(
                    "Max number of delta cache files must be at least 1.");
        }
    }

    private void validatePartitionBufferLimits(final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(conf.getMaxNumberOfKeysInActivePartition(),
                "MaxNumberOfKeysInActivePartition");
        if (conf.getMaxNumberOfKeysInActivePartition() < 1) {
            throw new IllegalArgumentException(
                    "Max number of keys in active partition must be at least 1.");
        }
        final int effectiveMaxDuringMaintenance = conf
                .getMaxNumberOfKeysInPartitionBuffer() == null
                        ? Math.max(
                                conf.getMaxNumberOfKeysInActivePartition() * 2,
                                conf.getMaxNumberOfKeysInActivePartition()
                                        + 1)
                        : conf.getMaxNumberOfKeysInPartitionBuffer();
        if (effectiveMaxDuringMaintenance <= conf
                .getMaxNumberOfKeysInActivePartition()) {
            throw new IllegalArgumentException(
                    "Max number of keys in partition buffer must be greater than the active partition limit.");
        }
    }

    private void validateDiskIoBuffer(final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(conf.getDiskIoBufferSize(), "DiskIoBufferSize");
        if (conf.getDiskIoBufferSize() <= 0) {
            throw new IllegalArgumentException(String.format(
                    "Parameter 'diskIoBufferSize' with value '%s'"
                            + " can't be smaller or equal to zero.",
                    conf.getDiskIoBufferSize()));
        }
        if (conf.getDiskIoBufferSize() % 1024 != 0) {
            throw new IllegalArgumentException(String.format(
                    "Parameter 'diskIoBufferSize' with value '%s'"
                            + " can't be divided by 1024 without reminder",
                    conf.getDiskIoBufferSize()));
        }
    }

    private void validateChunkFilters(final IndexConfiguration<K, V> conf) {
        if (conf.getEncodingChunkFilters() == null
                || conf.getEncodingChunkFilters().isEmpty()) {
            throw new IllegalArgumentException(
                    "Encoding chunk filters must not be empty.");
        }
        if (conf.getDecodingChunkFilters() == null
                || conf.getDecodingChunkFilters().isEmpty()) {
            throw new IllegalArgumentException(
                    "Decoding chunk filters must not be empty.");
        }
    }

    private void validateThreading(final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(conf.getIndexWorkerThreadCount(),
                "indexWorkerThreadCount");
        if (conf.getIndexWorkerThreadCount() < 1) {
            throw new IllegalArgumentException(
                    "Index worker thread count must be at least 1.");
        }
        Vldtn.requireNonNull(conf.getNumberOfSegmentIndexMaintenanceThreads(),
                "segmentIndexMaintenanceThreads");
        if (conf.getNumberOfSegmentIndexMaintenanceThreads() < 1) {
            throw new IllegalArgumentException(
                    "Segment index maintenance threads must be at least 1.");
        }
        Vldtn.requireNonNull(conf.getNumberOfIndexMaintenanceThreads(),
                "indexMaintenanceThreads");
        if (conf.getNumberOfIndexMaintenanceThreads() < 1) {
            throw new IllegalArgumentException(
                    "Index maintenance threads must be at least 1.");
        }
        Vldtn.requireNonNull(conf.getNumberOfRegistryLifecycleThreads(),
                "registryLifecycleThreads");
        if (conf.getNumberOfRegistryLifecycleThreads() < 1) {
            throw new IllegalArgumentException(
                    "Registry lifecycle threads must be at least 1.");
        }
    }

    private void validateBusyPolicy(final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(conf.getIndexBusyBackoffMillis(),
                "indexBusyBackoffMillis");
        if (conf.getIndexBusyBackoffMillis() < 1) {
            throw new IllegalArgumentException(
                    "Index busy backoff must be at least 1 ms.");
        }
        Vldtn.requireNonNull(conf.getIndexBusyTimeoutMillis(),
                "indexBusyTimeoutMillis");
        if (conf.getIndexBusyTimeoutMillis() < 1) {
            throw new IllegalArgumentException(
                    "Index busy timeout must be at least 1 ms.");
        }
    }

    private void validateRequiredDatatypesAndIndexName(
            final IndexConfiguration<K, V> conf) {
        if (conf.getKeyClass() == null) {
            throw new IllegalArgumentException("Key class wasn't specified");
        }
        if (conf.getValueClass() == null) {
            throw new IllegalArgumentException("Value class wasn't specified");
        }
        Vldtn.requireNotBlank(conf.getIndexName(), "indexName");
    }

    private void validateDatatypesAndIndexName(
            final IndexConfiguration<K, V> conf) {
        validateRequiredDatatypesAndIndexName(conf);
        Vldtn.requireNotBlank(conf.getKeyTypeDescriptor(), "keyTypeDescriptor");
        Vldtn.requireNotBlank(conf.getValueTypeDescriptor(),
                "valueTypeDescriptor");
    }

    private IndexConfigurationBuilder<K, V> makeBuilder(
            final IndexConfiguration<K, V> conf) {
        final IndexConfigurationBuilder<K, V> builder = IndexConfiguration
                .<K, V>builder()//
                .withKeyClass(conf.getKeyClass()) //
                .withValueClass(conf.getValueClass())//
                .withKeyTypeDescriptor(conf.getKeyTypeDescriptor())//
                .withValueTypeDescriptor(conf.getValueTypeDescriptor())//
                .withContextLoggingEnabled(conf.isContextLoggingEnabled())//
                .withIndexWorkerThreadCount(conf.getIndexWorkerThreadCount())//
                .withNumberOfSegmentIndexMaintenanceThreads(
                        conf.getNumberOfSegmentIndexMaintenanceThreads())//
                .withNumberOfIndexMaintenanceThreads(
                        conf.getNumberOfIndexMaintenanceThreads())//
                .withNumberOfRegistryLifecycleThreads(
                        conf.getNumberOfRegistryLifecycleThreads())//
                .withIndexBusyBackoffMillis(conf.getIndexBusyBackoffMillis())//
                .withIndexBusyTimeoutMillis(conf.getIndexBusyTimeoutMillis())//
                .withSegmentMaintenanceAutoEnabled(
                        conf.isSegmentMaintenanceAutoEnabled())//
                .withWal(conf.getWal())//
                .withName(conf.getIndexName())//

                // SegmentIndex runtime properties
                .withMaxNumberOfSegmentsInCache(
                        conf.getMaxNumberOfSegmentsInCache())//
                .withMaxNumberOfKeysInSegment(
                        conf.getMaxNumberOfKeysInSegment())//
                .withMaxNumberOfKeysInPartitionBeforeSplit(
                        conf.getMaxNumberOfKeysInPartitionBeforeSplit())//
                .withDiskIoBufferSizeInBytes(conf.getDiskIoBufferSize())//

                // Segment properties
                .withMaxNumberOfKeysInSegmentCache(
                        conf.getMaxNumberOfKeysInSegmentCache())//
                .withMaxNumberOfKeysInActivePartition(
                        conf.getMaxNumberOfKeysInActivePartition())//
                .withMaxNumberOfKeysInPartitionBuffer(
                        conf.getMaxNumberOfKeysInPartitionBuffer())//
                .withMaxNumberOfKeysInSegmentChunk(
                        conf.getMaxNumberOfKeysInSegmentChunk())//
                .withMaxNumberOfDeltaCacheFiles(
                        conf.getMaxNumberOfDeltaCacheFiles())//

                // Segment bloom filter properties
                .withBloomFilterNumberOfHashFunctions(
                        conf.getBloomFilterNumberOfHashFunctions())//
                .withBloomFilterIndexSizeInBytes(
                        conf.getBloomFilterIndexSizeInBytes())//
                .withBloomFilterProbabilityOfFalsePositive(
                        conf.getBloomFilterProbabilityOfFalsePositive());

        conf.getEncodingChunkFilters().forEach(builder::addEncodingFilter);
        conf.getDecodingChunkFilters().forEach(builder::addDecodingFilter);
        return builder;
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
        if (!Boolean.TRUE.equals(conf.isContextLoggingEnabled())) {
            return false;
        }
        final String indexName = normalizeIndexName(conf.getIndexName());
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
