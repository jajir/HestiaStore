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

    void save(IndexConfiguration<K, V> indexConfiguration) {
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

        // FIXME it should allow to change more properties
        // TODO openin index in non existing place shoudl provide better message

        final IndexConfigurationBuilder<K, V> builder = IndexConfiguration
                .<K, V>builder()//
                .withKeyClass(storedConf.getKeyClass()) //
                .withValueClass(storedConf.getValueClass())//
                .withKeyTypeDescriptor(storedConf.getKeyTypeDescriptor())//
                .withValueTypeDescriptor(storedConf.getValueTypeDescriptor())//
                .withLogEnabled(storedConf.isLogEnabled())//
                .withThreadSafe(storedConf.isThreadSafe())//
                .withName(storedConf.getIndexName())//

                // Index runtime properties
                .withMaxNumberOfKeysInCache(
                        storedConf.getMaxNumberOfSegmentsInCache())//
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

        boolean dirty = false;

        if (indexConf.getIndexName() != null && !indexConf.getIndexName()
                .equals(storedConf.getIndexName())) {
            builder.withName(indexConf.getIndexName());
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
                    "Max number of segments in cache must be at least 2.");
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

}
