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

    IndexConfiguration<K, V> loadExisting(
            final IndexConfiguration<K, V> userConf) {
        Objects.requireNonNull(userConf, "IndexConfiguration cannot be null");
        // final IndexConfiguration<K, V> conf = confStorage.load();
        // FIXME NYI
        throw new UnsupportedOperationException(
                "This method is not implemented yet");
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

    IndexConfiguration<K, V> mergeWithStored(
            final IndexConfiguration<K, V> indexConf) {
        return null;
    }

    private IndexConfiguration<K, V> validate(IndexConfiguration<K, V> conf) {
        if (conf.getKeyClass() == null) {
            throw new IllegalArgumentException("Key class wasn't specified");
        }
        if (conf.getValueClass() == null) {
            throw new IllegalArgumentException("Value class wasn't specified");
        }
        if (conf.getKeyTypeDescriptor() == null) {
            throw new IllegalArgumentException("Key type descriptor is null. "
                    + "Set key type descriptor of key class.");
        }
        if (conf.getValueTypeDescriptor() == null) {
            throw new IllegalArgumentException("Value type descriptor is null. "
                    + "Set value type descriptor of value class.");
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
            throw new IllegalArgumentException("index name is required");
        }
        return conf;
    }

}
