package com.coroptis.index.sst;

import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Vldtn;

public class IndexConfigurationManager<K, V> {

    private final IndexConfiguratonStorage<K, V> confStorage;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    IndexConfigurationManager(
            final IndexConfiguratonStorage<K, V> confStorage) {
        this.confStorage = Objects.requireNonNull(confStorage,
                "IndexConfiguratonStorage cannot be null");
        // private constructor to prevent instantiation
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
        final Optional<IndexConfigurationDefault> oDefaults = IndexConfigurationRegistry
                .get(conf.getKeyClass());
        if (oDefaults.isEmpty()) {
            logger.debug("There is no default configuration for key class '{}'",
                    conf.getKeyClass());
            return builder.build();
        }
        final IndexConfigurationDefault defaults = oDefaults.get();
        if (conf.isLogEnabled() == null) {
            builder.withLogEnabled(defaults.isLogEnabled());
        }
        if (conf.isThreadSafe() == null) {
            builder.withThreadSafe(defaults.isThreadSafe());
        }
        if (conf.getMaxNumberOfKeysInSegment() == null) {
            builder.withMaxNumberOfKeysInSegment(
                    defaults.getMaxNumberOfKeysInSegment());
        }
        if (conf.getMaxNumberOfKeysInSegmentCache() == null) {
            builder.withMaxNumberOfKeysInSegmentCache(
                    defaults.getMaxNumberOfKeysInSegmentCache());
        }
        if (conf.getMaxNumberOfKeysInSegmentCacheDuringFlushing() == null) {
            builder.withMaxNumberOfKeysInSegmentCacheDuringFlushing(
                    defaults.getMaxNumberOfKeysInSegmentCacheDuringFlushing());
        }
        if (conf.getMaxNumberOfKeysInCache() == null) {
            builder.withMaxNumberOfKeysInCache(
                    defaults.getMaxNumberOfKeysInCache());
        }
        if (conf.getMaxNumberOfSegmentsInCache() == null) {
            builder.withMaxNumberOfSegmentsInCache(
                    defaults.getMaxNumberOfSegmentsInCache());
        }
        if (conf.getMaxNumberOfKeysInSegmentIndexPage() == null) {
            builder.withMaxNumberOfKeysInSegmentIndexPage(
                    defaults.getMaxNumberOfKeysInSegmentIndexPage());
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
        return builder.build();
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

        if (indexConf.getKeyClass() != null
                && !indexConf.getKeyClass().equals(storedConf.getKeyClass())) {
            throw new IllegalArgumentException(String.format(
                    "Key class is already set to '%s' and can't be changed to '%s'",
                    storedConf.getKeyClass().getName(),
                    indexConf.getKeyClass().getName()));
        }

        if (indexConf.getValueClass() != null && !indexConf.getValueClass()
                .equals(storedConf.getValueClass())) {
            throw new IllegalArgumentException(String.format(
                    "Value class is already set to '%s' and can't be changed to '%s'",
                    storedConf.getValueClass().getName(),
                    indexConf.getValueClass().getName()));
        }

        if (indexConf.getKeyTypeDescriptor() != null
                && !indexConf.getKeyTypeDescriptor()
                        .equals(storedConf.getKeyTypeDescriptor())) {
            throw new IllegalArgumentException(String.format(
                    "Key type descriptor is already set to '%s' and can't be changed to '%s'",
                    storedConf.getKeyTypeDescriptor(),
                    indexConf.getKeyTypeDescriptor()));
        }

        if (indexConf.getValueTypeDescriptor() != null
                && !indexConf.getValueTypeDescriptor()
                        .equals(storedConf.getValueTypeDescriptor())) {
            throw new IllegalArgumentException(String.format(
                    "Value type descriptor is already set to '%s' and can't be changed to '%s'",
                    storedConf.getValueTypeDescriptor(),
                    indexConf.getValueTypeDescriptor()));
        }

        if (indexConf.getMaxNumberOfKeysInSegment() != null
                && indexConf.getMaxNumberOfKeysInSegment() > 0
                && indexConf.getMaxNumberOfKeysInSegment() != storedConf
                        .getMaxNumberOfKeysInSegment()) {
            throw new IllegalArgumentException(String.format(
                    "Value of MaxNumberOfKeysInSegment is already set"
                            + " to '%s' and can't be changed to '%s'",
                    storedConf.getMaxNumberOfKeysInSegment(),
                    indexConf.getMaxNumberOfKeysInSegment()));
        }

        if (indexConf.getMaxNumberOfKeysInSegmentIndexPage() != null
                && indexConf.getMaxNumberOfKeysInSegmentIndexPage() > 0
                && indexConf
                        .getMaxNumberOfKeysInSegmentIndexPage() != storedConf
                                .getMaxNumberOfKeysInSegmentIndexPage()) {
            throw new IllegalArgumentException(String.format(
                    "Value of MaxNumberOfKeysInSegmentIndexPage is already set"
                            + " to '%s' and can't be changed to '%s'",
                    storedConf.getMaxNumberOfKeysInSegmentIndexPage(),
                    indexConf.getMaxNumberOfKeysInSegmentIndexPage()));
        }

        if (indexConf.getBloomFilterIndexSizeInBytes() != null
                && indexConf.getBloomFilterIndexSizeInBytes() > 0
                && indexConf.getBloomFilterIndexSizeInBytes() != storedConf
                        .getBloomFilterIndexSizeInBytes()) {
            throw new IllegalArgumentException(String.format(
                    "Value of BloomFilterIndexSizeInBytes is already set"
                            + " to '%s' and can't be changed to '%s'",
                    storedConf.getBloomFilterIndexSizeInBytes(),
                    indexConf.getBloomFilterIndexSizeInBytes()));
        }

        if (indexConf.getBloomFilterNumberOfHashFunctions() != null
                && indexConf.getBloomFilterNumberOfHashFunctions() > 0
                && indexConf.getBloomFilterNumberOfHashFunctions() != storedConf
                        .getBloomFilterNumberOfHashFunctions()) {
            throw new IllegalArgumentException(String.format(
                    "Value of BloomFilterNumberOfHashFunctions is already set"
                            + " to '%s' and can't be changed to '%s'",
                    storedConf.getBloomFilterNumberOfHashFunctions(),
                    indexConf.getBloomFilterNumberOfHashFunctions()));
        }

        if (indexConf.getBloomFilterProbabilityOfFalsePositive() != null
                && indexConf
                        .getBloomFilterProbabilityOfFalsePositive() != storedConf
                                .getBloomFilterProbabilityOfFalsePositive()) {
            throw new IllegalArgumentException(String.format(
                    "Value of BloomFilterProbabilityOfFalsePositive is already set"
                            + " to '%s' and can't be changed to '%s'",
                    storedConf.getBloomFilterProbabilityOfFalsePositive(),
                    indexConf.getBloomFilterProbabilityOfFalsePositive()));
        }

        if (indexConf.getIndexName() != null && !indexConf.getIndexName()
                .equals(storedConf.getIndexName())) {
            builder.withName(indexConf.getIndexName());
            dirty = true;
        }

        if (indexConf.getDiskIoBufferSize() != null
                && indexConf.getDiskIoBufferSize() > 0
                && !indexConf.getDiskIoBufferSize()
                        .equals(storedConf.getDiskIoBufferSize())) {
            builder.withDiskIoBufferSizeInBytes(
                    indexConf.getDiskIoBufferSize());
            dirty = true;
        }

        if (indexConf.getMaxNumberOfKeysInSegmentCache() != null
                && indexConf.getMaxNumberOfKeysInSegmentCache() > 0
                && !indexConf.getMaxNumberOfKeysInSegmentCache().equals(
                        storedConf.getMaxNumberOfKeysInSegmentCache())) {
            builder.withMaxNumberOfKeysInSegmentCache(
                    indexConf.getMaxNumberOfKeysInSegmentCache());
            dirty = true;
        }

        if (indexConf.getMaxNumberOfKeysInSegmentCacheDuringFlushing() != null
                && indexConf
                        .getMaxNumberOfKeysInSegmentCacheDuringFlushing() > 0
                && !indexConf.getMaxNumberOfKeysInSegmentCacheDuringFlushing()
                        .equals(storedConf
                                .getMaxNumberOfKeysInSegmentCacheDuringFlushing())) {
            builder.withMaxNumberOfKeysInSegmentCacheDuringFlushing(
                    indexConf.getMaxNumberOfKeysInSegmentCacheDuringFlushing());
            dirty = true;
        }

        if (indexConf.getMaxNumberOfKeysInCache() != null
                && indexConf.getMaxNumberOfKeysInCache() > 0
                && !indexConf.getMaxNumberOfKeysInCache()
                        .equals(storedConf.getMaxNumberOfKeysInCache())) {
            builder.withMaxNumberOfKeysInCache(
                    indexConf.getMaxNumberOfKeysInCache());
            dirty = true;
        }

        if (indexConf.isLogEnabled() != null && !indexConf.isLogEnabled()
                .equals(storedConf.isLogEnabled())) {
            builder.withLogEnabled(indexConf.isLogEnabled());
            dirty = true;
        }

        if (indexConf.isThreadSafe() != null && !indexConf.isThreadSafe()
                .equals(storedConf.isThreadSafe())) {
            builder.withThreadSafe(indexConf.isThreadSafe());
            dirty = true;
        }

        if (dirty) {
            confStorage.save(builder.build());
        }
        return builder.build();
    }

    private IndexConfiguration<K, V> validate(IndexConfiguration<K, V> conf) {
        validateDatatypesAndIndexName(conf);
        if (conf.getKeyTypeDescriptor() == null) {
            throw new IllegalArgumentException("Key type descriptor is null.");
        }
        if (conf.getValueTypeDescriptor() == null) {
            throw new IllegalArgumentException(
                    "Value type descriptor is null.");
        }
        if (conf.isThreadSafe() == null) {
            throw new IllegalArgumentException("Value of thread safe is null.");
        }
        if (conf.isLogEnabled() == null) {
            throw new IllegalArgumentException("Value of log enable is null.");
        }

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

        Vldtn.requireNonNull(
                conf.getMaxNumberOfKeysInSegmentCacheDuringFlushing(),
                "MaxNumberOfKeysInSegmentCacheDuringFlushing");
        if (conf.getMaxNumberOfKeysInSegmentCacheDuringFlushing() < 3) {
            throw new IllegalArgumentException(
                    "Max number of keys in segment cache during flushing must be at least 3.");
        }
        if (conf.getMaxNumberOfKeysInSegmentCacheDuringFlushing() < conf
                .getMaxNumberOfKeysInSegmentCache()) {
            throw new IllegalArgumentException(
                    "Max number of keys in segment cache during flushing must be greater than max number of keys in segment cache.");
        }

        Vldtn.requireNonNull(conf.getDiskIoBufferSize(), "DiskIoBufferSize");
        if (conf.getDiskIoBufferSize() % 1024 != 0) {
            throw new IllegalArgumentException(String.format(
                    "Parameter 'diskIoBufferSize' vith value '%s'"
                            + " can't be divided by 1024 without reminder",
                    conf.getDiskIoBufferSize()));
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
        IndexConfigurationBuilder<K, V> builder = IndexConfiguration
                .<K, V>builder()//
                .withKeyClass(conf.getKeyClass()) //
                .withValueClass(conf.getValueClass())//
                .withKeyTypeDescriptor(conf.getKeyTypeDescriptor())//
                .withValueTypeDescriptor(conf.getValueTypeDescriptor())//
                .withLogEnabled(conf.isLogEnabled())//
                .withThreadSafe(conf.isThreadSafe())//
                .withName(conf.getIndexName())//

                // Index runtime properties
                .withMaxNumberOfKeysInCache(conf.getMaxNumberOfKeysInCache())//
                .withMaxNumberOfSegmentsInCache(
                        conf.getMaxNumberOfSegmentsInCache())//
                .withMaxNumberOfKeysInSegment(
                        conf.getMaxNumberOfKeysInSegment())//
                .withDiskIoBufferSizeInBytes(conf.getDiskIoBufferSize())//

                // Segment properties
                .withMaxNumberOfKeysInSegmentCache(
                        conf.getMaxNumberOfKeysInSegmentCache())//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(
                        conf.getMaxNumberOfKeysInSegmentCacheDuringFlushing())//
                .withMaxNumberOfKeysInSegmentIndexPage(
                        conf.getMaxNumberOfKeysInSegmentIndexPage())//

                // Segment bloom filter properties
                .withBloomFilterNumberOfHashFunctions(
                        conf.getBloomFilterNumberOfHashFunctions())//
                .withBloomFilterIndexSizeInBytes(
                        conf.getBloomFilterIndexSizeInBytes())//
                .withBloomFilterProbabilityOfFalsePositive(
                        conf.getBloomFilterProbabilityOfFalsePositive())//
        ;
        return builder;
    }

}
