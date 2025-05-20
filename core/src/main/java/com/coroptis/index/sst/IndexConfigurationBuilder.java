package com.coroptis.index.sst;

import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.datatype.TypeDescriptor;

public class IndexConfigurationBuilder<K, V> {

    private final static long DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = 200_000;
    private final static long DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = -1;
    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_INDEX_PAGE = 5_000;

    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT = 10_000_000;
    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_CACHE = 1_000_000;
    private final static int DEFAULT_MAX_NUMBER_OF_SEGMENTS_IN_CACHE = 10;

    private final static boolean DEFAULT_INDEX_SYNCHRONIZED = false;

    private final static int DEFAULT_DISK_IO_BUFFER_SIZE_IN_BYTES = 1024 * 4;

    private long maxNumberOfKeysInSegmentCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;
    private long maxNumberOfKeysInSegmentCacheDuringFlushing = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING;
    private int maxNumberOfKeysInSegmentIndexPage = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_INDEX_PAGE;
    private int maxNumberOfKeysInCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_CACHE;
    private int maxNumberOfKeysInSegment = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT;
    private int maxNumberOfSegmentsInCache = DEFAULT_MAX_NUMBER_OF_SEGMENTS_IN_CACHE;

    private Integer bloomFilterNumberOfHashFunctions;
    private Integer bloomFilterIndexSizeInBytes;
    private Double bloomFilterProbabilityOfFalsePositive = null;
    private boolean isIndexSynchronized = DEFAULT_INDEX_SYNCHRONIZED;

    private int diskIoBufferSizeInBytes = DEFAULT_DISK_IO_BUFFER_SIZE_IN_BYTES;

    private String indexName = null;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private boolean useFullLog = false;
    private boolean customConfWasUsed = false;
    private String memoryConf = null;

    IndexConfigurationBuilder() {

    }

    public IndexConfigurationBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        return this;
    }

    public IndexConfigurationBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        return this;
    }

    public IndexConfigurationBuilder<K, V> withKeyClass(
            final Class<K> keyClass) {
        this.keyClass = Objects.requireNonNull(keyClass);
        return this;
    }

    public IndexConfigurationBuilder<K, V> withValueClass(
            final Class<V> valueClass) {
        this.valueClass = Objects.requireNonNull(valueClass);
        return this;
    }

    public IndexConfigurationBuilder<K, V> withName(final String indexName) {
        this.indexName = Objects.requireNonNull(indexName);
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegmentCache(
            final long maxNumberOfKeysInSegmentCache) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegmentIndexPage(
            final int maxNumberOfKeysInSegmentIndexPage) {
        this.maxNumberOfKeysInSegmentIndexPage = maxNumberOfKeysInSegmentIndexPage;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInCache(
            final int maxNumberOfKeysInCache) {
        this.maxNumberOfKeysInCache = maxNumberOfKeysInCache;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegment(
            final int maxNumberOfKeysInSegment) {
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfSegmentsInCache(
            final int maxNumberOfSegmentsInCache) {
        this.maxNumberOfSegmentsInCache = maxNumberOfSegmentsInCache;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegmentCacheDuringFlushing(
            final int maxNumberOfKeysInSegmentCacheDuringFlushing) {
        this.maxNumberOfKeysInSegmentCacheDuringFlushing = maxNumberOfKeysInSegmentCacheDuringFlushing;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withBloomFilterNumberOfHashFunctions(
            final int bloomFilterNumberOfHashFunctions) {
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withBloomFilterProbabilityOfFalsePositive(
            final Double probabilityOfFalsePositive) {
        this.bloomFilterProbabilityOfFalsePositive = probabilityOfFalsePositive;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withIsIndexSynchronized(
            final boolean isIndexSynchronized) {
        this.isIndexSynchronized = isIndexSynchronized;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withBloomFilterIndexSizeInBytes(
            final int bloomFilterIndexSizeInBytes) {
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withDiskIoBufferSizeInBytes(
            final int diskIoBufferSizeInBytes) {
        this.diskIoBufferSizeInBytes = diskIoBufferSizeInBytes;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withCustomConf() {
        this.customConfWasUsed = true;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withConf(
            final String memoryConfiguration) {
        this.memoryConf = memoryConfiguration;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withUseFullLog(
            final boolean useFullLog) {
        this.useFullLog = useFullLog;
        return this;
    }

    public IndexConfiguration build() {
        if (keyClass == null) {
            throw new IllegalArgumentException("Key class wasn't specified");
        }
        if (valueClass == null) {
            throw new IllegalArgumentException("Value class wasn't specified");
        }
        if (keyTypeDescriptor == null) {
            this.keyTypeDescriptor = DataTypeDescriptorRegistry
                    .getTypeDescriptor(this.keyClass);
        }
        if (valueTypeDescriptor == null) {
            this.valueTypeDescriptor = DataTypeDescriptorRegistry
                    .getTypeDescriptor(this.valueClass);
        }
        if (maxNumberOfKeysInSegmentCache < 3) {
            throw new IllegalArgumentException(
                    "Max number of keys in segment cache must be at least 3.");
        }
        if (maxNumberOfKeysInSegmentCacheDuringFlushing == DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING) {
            maxNumberOfKeysInSegmentCacheDuringFlushing = maxNumberOfKeysInCache;
        } else {
            if (maxNumberOfKeysInSegmentCacheDuringFlushing < 3) {
                throw new IllegalArgumentException(
                        "Max number of keys in segment cache during flushing must be at least 3.");
            }
            if (maxNumberOfKeysInSegmentCacheDuringFlushing < maxNumberOfKeysInSegmentCache) {
                throw new IllegalArgumentException(
                        "Max number of keys in segment cache during flushing must be greater than max number of keys in segment cache.");
            }
        }
        if (maxNumberOfKeysInSegment < 4) {
            throw new IllegalArgumentException(
                    "Max number of keys in segment must be at least 4.");
        }
        if (maxNumberOfSegmentsInCache < 3) {
            throw new IllegalArgumentException(
                    "Max number of segments in cache must be at least 2.");
        }
        if (indexName == null) {
            throw new IllegalArgumentException("index name is required");
        }

        if (!customConfWasUsed) {
            final Optional<BuilderConfiguration> oConf = BuilderConfigurationRegistry
                    .get(keyClass, memoryConf);
            if (oConf.isPresent()) {
                final BuilderConfiguration conf = oConf.get();
                maxNumberOfKeysInSegmentCache = conf
                        .getMaxNumberOfKeysInSegmentCache();
                maxNumberOfKeysInSegmentCacheDuringFlushing = conf
                        .getMaxNumberOfKeysInSegmentCacheDuringFlushing();
                maxNumberOfKeysInSegmentIndexPage = conf
                        .getMaxNumberOfKeysInSegmentIndexPage();
                maxNumberOfKeysInCache = conf.getMaxNumberOfKeysInCache();
                maxNumberOfKeysInSegment = conf.getMaxNumberOfKeysInSegment();
                maxNumberOfSegmentsInCache = conf
                        .getMaxNumberOfSegmentsInCache();
                diskIoBufferSizeInBytes = conf.getIndexBufferSizeInBytes();
                bloomFilterIndexSizeInBytes = conf
                        .getBloomFilterIndexSizeInBytes();
                bloomFilterNumberOfHashFunctions = conf
                        .getBloomFilterNumberOfHashFunctions();
                bloomFilterProbabilityOfFalsePositive = conf
                        .getBloomFilterProbabilityOfFalsePositive();
            } else {
                throw new IllegalStateException(String.format(
                        "Configuration for key class '%s' "
                                + "and memory configuration '%s' was not specified.",
                        keyClass.getName(), memoryConf));
            }
        }
        final IndexConfiguration indexConf = new IndexConfiguration(
                maxNumberOfKeysInSegmentCache,
                maxNumberOfKeysInSegmentCacheDuringFlushing,
                maxNumberOfKeysInSegmentIndexPage, maxNumberOfKeysInCache,
                maxNumberOfKeysInSegment, maxNumberOfSegmentsInCache, indexName,
                bloomFilterNumberOfHashFunctions, bloomFilterIndexSizeInBytes,
                bloomFilterProbabilityOfFalsePositive, diskIoBufferSizeInBytes,
                isIndexSynchronized, useFullLog);
        if (keyTypeDescriptor == null) {
            throw new IllegalArgumentException("Key type descriptor is null. "
                    + "Set key type descriptor of key class.");
        }
        if (valueTypeDescriptor == null) {
            throw new IllegalArgumentException("Value type descriptor is null. "
                    + "Set value type descriptor of value class.");
        }
        return indexConf;
    }

}
