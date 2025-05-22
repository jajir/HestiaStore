package com.coroptis.index.sst;

import java.util.Objects;
import java.util.Optional;

public class IndexConfigurationManager<K, V> {

    private final IndexConfiguratonStorage<K, V> confStorage;

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
            final IndexConfiguration<K, V> indexConfiguration) {
        final IndexConfigurationBuilder<K, V> builder = makeBuilder(
                indexConfiguration);
        // TODO all defaults shoudl be applied
        if (indexConfiguration.isLogEnabled() == null) {
            builder.withLogEnabled(false);
        }
        if (indexConfiguration.isThreadSafe() == null) {
            builder.withThreadSafe(false);
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
        // TODO verify stored conf, user can change it manually
        // FIXME it should allow to change more properties
        // TODO openin index in non existing place shoudl provide better message

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

        if (indexConf.getMaxNumberOfKeysInSegment() > 0
                && indexConf.getMaxNumberOfKeysInSegment() != storedConf
                        .getMaxNumberOfKeysInSegment()) {
            throw new IllegalArgumentException(String.format(
                    "Value of MaxNumberOfKeysInSegment is already set"
                            + " to '%s' and can't be changed to '%s'",
                    storedConf.getMaxNumberOfKeysInSegment(),
                    indexConf.getMaxNumberOfKeysInSegment()));
        }

        if (indexConf.getMaxNumberOfKeysInSegmentIndexPage() > 0 && indexConf
                .getMaxNumberOfKeysInSegmentIndexPage() != storedConf
                        .getMaxNumberOfKeysInSegmentIndexPage()) {
            throw new IllegalArgumentException(String.format(
                    "Value of MaxNumberOfKeysInSegmentIndexPage is already set"
                            + " to '%s' and can't be changed to '%s'",
                    storedConf.getMaxNumberOfKeysInSegmentIndexPage(),
                    indexConf.getMaxNumberOfKeysInSegmentIndexPage()));
        }

        if (indexConf.getBloomFilterIndexSizeInBytes() > 0
                && indexConf.getBloomFilterIndexSizeInBytes() != storedConf
                        .getBloomFilterIndexSizeInBytes()) {
            throw new IllegalArgumentException(String.format(
                    "Value of BloomFilterIndexSizeInBytes is already set"
                            + " to '%s' and can't be changed to '%s'",
                    storedConf.getBloomFilterIndexSizeInBytes(),
                    indexConf.getBloomFilterIndexSizeInBytes()));
        }

        if (indexConf.getBloomFilterNumberOfHashFunctions() > 0
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

        if (indexConf.getDiskIoBufferSize() > 0 && indexConf
                .getDiskIoBufferSize() != storedConf.getDiskIoBufferSize()) {
            builder.withDiskIoBufferSizeInBytes(
                    indexConf.getDiskIoBufferSize());
            dirty = true;
        }

        if (indexConf.getMaxNumberOfKeysInSegmentCache() > 0
                && indexConf.getMaxNumberOfKeysInSegmentCache() != storedConf
                        .getMaxNumberOfKeysInSegmentCache()) {
            builder.withMaxNumberOfKeysInSegmentCache(
                    indexConf.getMaxNumberOfKeysInSegmentCache());
            dirty = true;
        }

        if (indexConf.getMaxNumberOfKeysInSegmentCacheDuringFlushing() > 0
                && indexConf
                        .getMaxNumberOfKeysInSegmentCacheDuringFlushing() != storedConf
                                .getMaxNumberOfKeysInSegmentCacheDuringFlushing()) {
            builder.withMaxNumberOfKeysInSegmentCacheDuringFlushing(
                    indexConf.getMaxNumberOfKeysInSegmentCacheDuringFlushing());
            dirty = true;
        }

        if (indexConf.getMaxNumberOfKeysInCache() > 0
                && indexConf.getMaxNumberOfKeysInCache() != storedConf
                        .getMaxNumberOfKeysInCache()) {
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
        if (conf.getKeyClass() == null) {
            throw new IllegalArgumentException("Key class wasn't specified");
        }
        if (conf.getValueClass() == null) {
            throw new IllegalArgumentException("Value class wasn't specified");
        }
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
        if (conf.getMaxNumberOfKeysInCache() < 3) {
            throw new IllegalArgumentException(
                    "Max number of keys in cache must be at least 3.");
        }
        if (conf.getMaxNumberOfKeysInSegment() < 4) {
            throw new IllegalArgumentException(
                    "Max number of keys in segment must be at least 4.");
        }
        if (conf.getMaxNumberOfSegmentsInCache() < 3) {
            throw new IllegalArgumentException(
                    "Max number of segments in cache must be at least 3.");
        }
        if (conf.getMaxNumberOfKeysInSegmentCacheDuringFlushing() < 3) {
            throw new IllegalArgumentException(
                    "Max number of keys in segment cache during flushing must be at least 3.");
        }
        if (conf.getMaxNumberOfKeysInSegmentCacheDuringFlushing() < conf
                .getMaxNumberOfKeysInSegmentCache()) {
            throw new IllegalArgumentException(
                    "Max number of keys in segment cache during flushing must be greater than max number of keys in segment cache.");
        }
        if (conf.getDiskIoBufferSize() % 1024 != 0) {
            throw new IllegalArgumentException(String.format(
                    "Parameter 'diskIoBufferSize' vith value '%s'"
                            + " can't be divided by 1024 without reminder",
                    conf.getDiskIoBufferSize()));
        }
        if (conf.getIndexName() == null) {
            throw new IllegalArgumentException("Index name is null.");
        }
        return conf;
    }

    private IndexConfigurationBuilder<K, V> makeBuilder(
            final IndexConfiguration<K, V> storedConf) {
        return IndexConfiguration.<K, V>builder()//
                .withKeyClass(storedConf.getKeyClass()) //
                .withValueClass(storedConf.getValueClass())//
                .withKeyTypeDescriptor(storedConf.getKeyTypeDescriptor())//
                .withValueTypeDescriptor(storedConf.getValueTypeDescriptor())//
                .withLogEnabled(storedConf.isLogEnabled())//
                .withThreadSafe(storedConf.isThreadSafe())//
                .withName(storedConf.getIndexName())//

                // Index runtime properties
                .withMaxNumberOfKeysInCache(
                        storedConf.getMaxNumberOfKeysInCache())//
                .withMaxNumberOfSegmentsInCache(
                        storedConf.getMaxNumberOfSegmentsInCache())//
                .withMaxNumberOfKeysInSegment(
                        storedConf.getMaxNumberOfKeysInSegment())//
                .withDiskIoBufferSizeInBytes(storedConf.getDiskIoBufferSize())//

                // Segment properties
                .withMaxNumberOfKeysInSegmentCache(
                        storedConf.getMaxNumberOfKeysInSegmentCache())//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(storedConf
                        .getMaxNumberOfKeysInSegmentCacheDuringFlushing())//
                .withMaxNumberOfKeysInSegmentIndexPage(
                        storedConf.getMaxNumberOfKeysInSegmentIndexPage())//

                // Segment bloom filter properties
                .withBloomFilterNumberOfHashFunctions(
                        storedConf.getBloomFilterNumberOfHashFunctions())//
                .withBloomFilterIndexSizeInBytes(
                        storedConf.getBloomFilterIndexSizeInBytes())//
                .withBloomFilterProbabilityOfFalsePositive(
                        storedConf.getBloomFilterProbabilityOfFalsePositive())//
        ;
    }

}
