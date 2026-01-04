package org.hestiastore.index.segmentindex;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexConfigurationManager<K, V> {

    private final IndexConfiguratonStorage<K, V> confStorage;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    IndexConfigurationManager(
            final IndexConfiguratonStorage<K, V> confStorage) {
        this.confStorage = Vldtn.requireNonNull(confStorage, "confStorage");
    }

    IndexConfiguration<K, V> loadExisting() {
        return confStorage.load();
    }

    Optional<IndexConfiguration<K, V>> tryToLoad() {
        if (confStorage.exists()) {
            return Optional.of(confStorage.load());
        } else {
            return Optional.empty();
        }
    }

    IndexConfiguration<K, V> applyDefaults(
            final IndexConfiguration<K, V> conf) {
        validateDatatypesAndIndexName(conf);
        final IndexConfigurationBuilder<K, V> builder = makeBuilder(conf);
        if (conf.getKeyTypeDescriptor() == null) {
            builder.withKeyTypeDescriptor(DataTypeDescriptorRegistry
                    .getTypeDescriptor(conf.getKeyClass()));
        }
        if (conf.getValueTypeDescriptor() == null) {
            builder.withValueTypeDescriptor(DataTypeDescriptorRegistry
                    .getTypeDescriptor(conf.getValueClass()));
        }
        final Optional<IndexConfigurationContract> oDefaults = IndexConfigurationRegistry
                .get(conf.getKeyClass());
        if (oDefaults.isEmpty()) {
            logger.debug("There is no default configuration for key class '{}'",
                    conf.getKeyClass());
            return validate(builder.build());
        }
        final IndexConfigurationContract defaults = oDefaults.get();
        if (conf.isContextLoggingEnabled() == null) {
            builder.withContextLoggingEnabled(
                    defaults.isContextLoggingEnabled());
        }
        if (conf.getNumberOfThreads() == null) {
            builder.withNumberOfCpuThreads(defaults.getNumberOfThreads());
        }
        if (conf.getNumberOfIoThreads() == null) {
            builder.withNumberOfIoThreads(defaults.getNumberOfIoThreads());
        }
        if (conf.getMaxNumberOfKeysInSegment() == null) {
            builder.withMaxNumberOfKeysInSegment(
                    defaults.getMaxNumberOfKeysInSegment());
        }
        final int effectiveMaxNumberOfKeysInSegmentCache = conf
                .getMaxNumberOfKeysInSegmentCache() == null
                        ? defaults.getMaxNumberOfKeysInSegmentCache()
                        : conf.getMaxNumberOfKeysInSegmentCache();
        if (conf.getMaxNumberOfKeysInSegmentCache() == null) {
            builder.withMaxNumberOfKeysInSegmentCache(
                    effectiveMaxNumberOfKeysInSegmentCache);
        }
        final int effectiveWriteCacheSize;
        if (conf.getMaxNumberOfKeysInSegmentWriteCache() == null) {
            effectiveWriteCacheSize = Math.max(1,
                    effectiveMaxNumberOfKeysInSegmentCache / 2);
            builder.withMaxNumberOfKeysInSegmentWriteCache(
                    effectiveWriteCacheSize);
        } else {
            effectiveWriteCacheSize = conf.getMaxNumberOfKeysInSegmentWriteCache();
        }
        if (conf.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush() == null) {
            final int effectiveFlushBackpressure = Math.max(
                    (int) Math.ceil(effectiveWriteCacheSize * 1.4),
                    effectiveWriteCacheSize + 1);
            builder.withMaxNumberOfKeysInSegmentWriteCacheDuringFlush(
                    effectiveFlushBackpressure);
        }
        if (conf.getMaxNumberOfKeysInCache() == null) {
            builder.withMaxNumberOfKeysInCache(
                    defaults.getMaxNumberOfKeysInCache());
        }
        if (conf.getMaxNumberOfSegmentsInCache() == null) {
            builder.withMaxNumberOfSegmentsInCache(
                    defaults.getMaxNumberOfSegmentsInCache());
        }
        if (conf.getMaxNumberOfKeysInSegmentChunk() == null) {
            builder.withMaxNumberOfKeysInSegmentChunk(
                    defaults.getMaxNumberOfKeysInSegmentChunk());
        }
        if (conf.getMaxNumberOfKeysInSegmentChunk() == null) {
            builder.withMaxNumberOfKeysInSegmentChunk(
                    defaults.getMaxNumberOfKeysInSegmentChunk());
        }
        // bloom filter
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
        if (conf.getEncodingChunkFilters().isEmpty()) {
            builder.withEncodingFilters(defaults.getEncodingChunkFilters());
        }
        if (conf.getDecodingChunkFilters().isEmpty()) {
            builder.withDecodingFilters(defaults.getDecodingChunkFilters());
        }
        return validate(builder.build());
    }

    /**
     * Saves the configuration to the storage.
     * 
     * @param indexConfiguration configuration to save
     * @throws IllegalArgumentException when given parameter try to overrinde
     */
    void save(final IndexConfiguration<K, V> indexConfiguration) {
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
    IndexConfiguration<K, V> mergeWithStored(
            final IndexConfiguration<K, V> indexConf) {
        final IndexConfiguration<K, V> storedConf = confStorage.load();

        final IndexConfigurationBuilder<K, V> builder = makeBuilder(storedConf);
        boolean dirty = false;

        validateThatFixPropertiesAreNotOverriden(storedConf, indexConf);

        if (isIndexNameOverriden(storedConf, indexConf)) {
            builder.withName(indexConf.getIndexName());
            dirty = true;
        }

        if (isDiskIoBufferSizeOverriden(storedConf, indexConf)) {
            builder.withDiskIoBufferSizeInBytes(
                    indexConf.getDiskIoBufferSize());
            dirty = true;
        }

        if (isMaxNumberOfKeysInSegmentCacheOverriden(storedConf, indexConf)) {
            builder.withMaxNumberOfKeysInSegmentCache(
                    indexConf.getMaxNumberOfKeysInSegmentCache());
            dirty = true;
        }

        if (isMaxNumberOfKeysInSegmentWriteCacheOverriden(storedConf,
                indexConf)) {
            builder.withMaxNumberOfKeysInSegmentWriteCache(
                    indexConf.getMaxNumberOfKeysInSegmentWriteCache());
            dirty = true;
        }
        if (isMaxNumberOfKeysInSegmentWriteCacheDuringFlushOverriden(
                storedConf, indexConf)) {
            builder.withMaxNumberOfKeysInSegmentWriteCacheDuringFlush(
                    indexConf.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush());
            dirty = true;
        }

        if (isMaxNumberOfKeysInCacheOverriden(storedConf, indexConf)) {
            builder.withMaxNumberOfKeysInCache(
                    indexConf.getMaxNumberOfKeysInCache());
            dirty = true;
        }
        if (indexConf.getNumberOfThreads() != null
                && indexConf.getNumberOfThreads() > 0
                && !indexConf.getNumberOfThreads()
                        .equals(storedConf.getNumberOfThreads())) {
            builder.withNumberOfCpuThreads(indexConf.getNumberOfThreads());
            dirty = true;
        }
        if (indexConf.getNumberOfIoThreads() != null
                && indexConf.getNumberOfIoThreads() > 0
                && !indexConf.getNumberOfIoThreads()
                        .equals(storedConf.getNumberOfIoThreads())) {
            builder.withNumberOfIoThreads(indexConf.getNumberOfIoThreads());
            dirty = true;
        }

        if (indexConf.isContextLoggingEnabled() != null
                && !indexConf.isContextLoggingEnabled()
                        .equals(storedConf.isContextLoggingEnabled())) {
            builder.withContextLoggingEnabled(
                    indexConf.isContextLoggingEnabled());
            dirty = true;
        }

        if (dirty) {
            confStorage.save(builder.build());
        }
        return validate(builder.build());
    }

    private boolean isIndexNameOverriden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.getIndexName() != null
                && !indexConf.getIndexName().equals(storedConf.getIndexName());
    }

    private boolean isDiskIoBufferSizeOverriden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.getDiskIoBufferSize() != null
                && indexConf.getDiskIoBufferSize() > 0
                && !indexConf.getDiskIoBufferSize()
                        .equals(storedConf.getDiskIoBufferSize());
    }

    private boolean isMaxNumberOfKeysInSegmentCacheOverriden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.getMaxNumberOfKeysInSegmentCache() != null
                && indexConf.getMaxNumberOfKeysInSegmentCache() > 0
                && !indexConf.getMaxNumberOfKeysInSegmentCache()
                        .equals(storedConf.getMaxNumberOfKeysInSegmentCache());
    }

    private boolean isMaxNumberOfKeysInSegmentWriteCacheOverriden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.getMaxNumberOfKeysInSegmentWriteCache() != null
                && indexConf.getMaxNumberOfKeysInSegmentWriteCache() > 0
                && !indexConf.getMaxNumberOfKeysInSegmentWriteCache()
                        .equals(storedConf
                                .getMaxNumberOfKeysInSegmentWriteCache());
    }

    private boolean isMaxNumberOfKeysInSegmentWriteCacheDuringFlushOverriden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush() != null
                && indexConf.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush() > 0
                && !indexConf.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush()
                        .equals(storedConf
                                .getMaxNumberOfKeysInSegmentWriteCacheDuringFlush());
    }

    private boolean isMaxNumberOfKeysInCacheOverriden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        return indexConf.getMaxNumberOfKeysInCache() != null
                && indexConf.getMaxNumberOfKeysInCache() > 0
                && !indexConf.getMaxNumberOfKeysInCache()
                        .equals(storedConf.getMaxNumberOfKeysInCache());
    }

    void validateThatFixPropertiesAreNotOverriden(
            final IndexConfiguration<K, V> storedConf,
            final IndexConfiguration<K, V> indexConf) {
        if (indexConf.getKeyClass() != null) {
            validateThatWasntChanged(
                    !indexConf.getKeyClass().equals(storedConf.getKeyClass()), //
                    "KeyClass", //
                    storedConf.getKeyClass().getName(), //
                    indexConf.getKeyClass().getName());
        }

        if (indexConf.getValueClass() != null) {
            validateThatWasntChanged(
                    !indexConf.getValueClass()
                            .equals(storedConf.getValueClass()), //
                    "ValueClass", //
                    storedConf.getValueClass().getName(), //
                    indexConf.getValueClass().getName());
        }

        validateThatWasntChanged(
                indexConf.getKeyTypeDescriptor() != null
                        && !indexConf.getKeyTypeDescriptor()
                                .equals(storedConf.getKeyTypeDescriptor()), //
                "KeyTypeDescriptor", //
                storedConf.getKeyTypeDescriptor(), //
                indexConf.getKeyTypeDescriptor());

        validateThatWasntChanged(
                indexConf.getValueTypeDescriptor() != null
                        && !indexConf.getValueTypeDescriptor()
                                .equals(storedConf.getValueTypeDescriptor()), //
                "ValueTypeDescriptor", //
                storedConf.getValueTypeDescriptor(), //
                indexConf.getValueTypeDescriptor());

        validateThatWasntChanged(
                indexConf.getMaxNumberOfKeysInSegment() != null
                        && indexConf.getMaxNumberOfKeysInSegment() > 0
                        && !indexConf.getMaxNumberOfKeysInSegment().equals(
                                storedConf.getMaxNumberOfKeysInSegment()), //
                "MaxNumberOfKeysInSegment", //
                storedConf.getMaxNumberOfKeysInSegment(), //
                indexConf.getMaxNumberOfKeysInSegment());

        validateThatWasntChanged(
                indexConf.getMaxNumberOfKeysInSegmentChunk() != null
                        && indexConf.getMaxNumberOfKeysInSegmentChunk() > 0
                        && !indexConf.getMaxNumberOfKeysInSegmentChunk().equals(
                                storedConf.getMaxNumberOfKeysInSegmentChunk()), //
                "MaxNumberOfKeysInSegmentChunk", //
                storedConf.getMaxNumberOfKeysInSegmentChunk(), //
                indexConf.getMaxNumberOfKeysInSegmentChunk());

        validateThatWasntChanged(
                indexConf.getBloomFilterIndexSizeInBytes() != null
                        && indexConf.getBloomFilterIndexSizeInBytes() > 0
                        && !indexConf.getBloomFilterIndexSizeInBytes().equals(
                                storedConf.getBloomFilterIndexSizeInBytes()), //
                "BloomFilterIndexSizeInBytes", //
                storedConf.getBloomFilterIndexSizeInBytes(), //
                indexConf.getBloomFilterIndexSizeInBytes());

        validateThatWasntChanged(
                indexConf.getBloomFilterNumberOfHashFunctions() != null
                        && indexConf.getBloomFilterNumberOfHashFunctions() > 0
                        && !indexConf.getBloomFilterNumberOfHashFunctions()
                                .equals(storedConf
                                        .getBloomFilterNumberOfHashFunctions()), //
                "BloomFilterNumberOfHashFunctions", //
                storedConf.getBloomFilterNumberOfHashFunctions(), //
                indexConf.getBloomFilterNumberOfHashFunctions());

        validateThatWasntChanged(indexConf
                .getBloomFilterProbabilityOfFalsePositive() != null
                && !indexConf.getBloomFilterProbabilityOfFalsePositive().equals(
                        storedConf.getBloomFilterProbabilityOfFalsePositive()), //
                "BloomFilterProbabilityOfFalsePositive", //
                storedConf.getBloomFilterProbabilityOfFalsePositive(), //
                indexConf.getBloomFilterProbabilityOfFalsePositive());

        validateThatWasntChanged(
                wasntChanged(indexConf.getEncodingChunkFilters(),
                        storedConf.getEncodingChunkFilters()),
                "EncodingChunkFilters", storedConf.getEncodingChunkFilters(),
                indexConf.getEncodingChunkFilters());

        validateThatWasntChanged(
                wasntChanged(indexConf.getDecodingChunkFilters(),
                        storedConf.getDecodingChunkFilters()),
                "DecodingChunkFilters", storedConf.getDecodingChunkFilters(),
                indexConf.getDecodingChunkFilters());
    }

    final static Comparator<ChunkFilter> chunkFilterCmp = (f1, f2) -> {
        int c = f1.getClass().getName().compareTo(f2.getClass().getName());
        return c;
    };

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

    private boolean wasntChanged(final List<ChunkFilter> indexFilters,
            final List<ChunkFilter> storedFilters) {
        if (indexFilters == null) {
            return false;
        }
        if (indexFilters.isEmpty()) {
            return false;
        }
        if (indexFilters.isEmpty()) {
            return false;
        }
        return !equalLists(indexFilters, storedFilters, chunkFilterCmp);
    }

    private void validateThatWasntChanged(final boolean wasChanged,
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
        Vldtn.requireNonNull(conf.getKeyTypeDescriptor(), "keyTypeDescriptor");
        Vldtn.requireNonNull(conf.getValueTypeDescriptor(),
                "valueTypeDescriptor");
        Vldtn.requireNonNull(conf.isContextLoggingEnabled(),
                "isContextLoggingEnabled");

        Vldtn.requireNonNull(conf.getMaxNumberOfKeysInCache(),
                "MaxNumberOfKeysInCache");
        if (conf.getMaxNumberOfKeysInCache() < 3) {
            throw new IllegalArgumentException(
                    "Max number of keys in cache must be at least 3.");
        }

        Vldtn.requireNonNull(conf.getMaxNumberOfKeysInSegment(),
                "MaxNumberOfKeysInSegment");
        if (conf.getMaxNumberOfKeysInSegment() < 4) {
            throw new IllegalArgumentException(
                    "Max number of keys in segment must be at least 4.");
        }

        Vldtn.requireNonNull(conf.getMaxNumberOfSegmentsInCache(),
                "MaxNumberOfSegmentsInCache");
        if (conf.getMaxNumberOfSegmentsInCache() < 3) {
            throw new IllegalArgumentException(
                    "Max number of segments in cache must be at least 3.");
        }

        Vldtn.requireNonNull(conf.getMaxNumberOfKeysInSegmentWriteCache(),
                "MaxNumberOfKeysInSegmentWriteCache");
        if (conf.getMaxNumberOfKeysInSegmentWriteCache() < 1) {
            throw new IllegalArgumentException(
                    "Max number of keys in segment write cache must be at least 1.");
        }
        final int effectiveMaxDuringFlush = conf
                .getMaxNumberOfKeysInSegmentWriteCacheDuringFlush() == null
                        ? Math.max(
                                conf.getMaxNumberOfKeysInSegmentWriteCache()
                                        * 2,
                                conf.getMaxNumberOfKeysInSegmentWriteCache()
                                        + 1)
                        : conf.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush();
        if (effectiveMaxDuringFlush <= conf.getMaxNumberOfKeysInSegmentWriteCache()) {
            throw new IllegalArgumentException(
                    "Max number of keys in segment write cache during maintenance must be greater than the flush threshold.");
        }

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
        Vldtn.requireNonNull(conf.getNumberOfThreads(), "numberOfThreads");
        if (conf.getNumberOfThreads() < 1) {
            throw new IllegalArgumentException(
                    "Number of threads must be at least 1.");
        }
        Vldtn.requireNonNull(conf.getNumberOfIoThreads(), "numberOfIoThreads");
        if (conf.getNumberOfIoThreads() < 1) {
            throw new IllegalArgumentException(
                    "Number of IO threads must be at least 1.");
        }
        return conf;
    }

    private void validateDatatypesAndIndexName(
            final IndexConfiguration<K, V> conf) {
        if (conf.getKeyClass() == null) {
            throw new IllegalArgumentException("Key class wasn't specified");
        }
        if (conf.getValueClass() == null) {
            throw new IllegalArgumentException("Value class wasn't specified");
        }
        if (conf.getIndexName() == null) {
            throw new IllegalArgumentException("Index name is null.");
        }

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
                .withNumberOfCpuThreads(conf.getNumberOfThreads())//
                .withNumberOfIoThreads(conf.getNumberOfIoThreads())//
                .withName(conf.getIndexName())//

                // SegmentIndex runtime properties
                .withMaxNumberOfKeysInCache(conf.getMaxNumberOfKeysInCache())//
                .withMaxNumberOfSegmentsInCache(
                        conf.getMaxNumberOfSegmentsInCache())//
                .withMaxNumberOfKeysInSegment(
                        conf.getMaxNumberOfKeysInSegment())//
                .withDiskIoBufferSizeInBytes(conf.getDiskIoBufferSize())//

                // Segment properties
                .withMaxNumberOfKeysInSegmentCache(
                        conf.getMaxNumberOfKeysInSegmentCache())//
                .withMaxNumberOfKeysInSegmentWriteCache(
                        conf.getMaxNumberOfKeysInSegmentWriteCache())//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringFlush(
                        conf.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush())//
                .withMaxNumberOfKeysInSegmentChunk(
                        conf.getMaxNumberOfKeysInSegmentChunk())//

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

}
