package com.coroptis.index.sst;

import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.log.Log;

public class IndexBuilder<K, V> {

    private final static long DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = 200_000;
    private final static long DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = -1;
    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_INDEX_PAGE = 5_000;

    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT = 10_000_000;
    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_CACHE = 1_000_000;
    private final static int DEFAULT_MAX_NUMBER_OF_SEGMENTS_IN_CACHE = 10;

    private final static int DEFAULT_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = 1_000;
    private final static int DEFAULT_BLOOM_FILTER_INDEX_SIZE_IN_BYTES = 10_000;
    private final static boolean DEFAULT_INDEX_SYNCHRONIZED = false;

    private final static int DEFAULT_INDEX_BUFEER_SIZE_IN_BYTES = 1024 * 4;

    private long maxNumberOfKeysInSegmentCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;
    private long maxNumberOfKeysInSegmentCacheDuringFlushing = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING;
    private int maxNumberOfKeysInSegmentIndexPage = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_INDEX_PAGE;
    private int maxNumberOfKeysInCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_CACHE;
    private int maxNumberOfKeysInSegment = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT;
    private int maxNumberOfSegmentsInCache = DEFAULT_MAX_NUMBER_OF_SEGMENTS_IN_CACHE;
    private int bloomFilterNumberOfHashFunctions = DEFAULT_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS;
    private int bloomFilterIndexSizeInBytes = DEFAULT_BLOOM_FILTER_INDEX_SIZE_IN_BYTES;
    private boolean isIndexSynchronized = DEFAULT_INDEX_SYNCHRONIZED;

    private int indexBufferSizeInBytes = DEFAULT_INDEX_BUFEER_SIZE_IN_BYTES;

    private Directory directory;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private boolean useFullLog = false;

    IndexBuilder() {

    }

    public IndexBuilder<K, V> withDirectory(final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public IndexBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        return this;
    }

    public IndexBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        return this;
    }

    public IndexBuilder<K, V> withMaxNumberOfKeysInSegmentCache(
            final long maxNumberOfKeysInSegmentCache) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        return this;
    }

    public IndexBuilder<K, V> withMaxNumberOfKeysInSegmentIndexPage(
            final int maxNumberOfKeysInSegmentIndexPage) {
        this.maxNumberOfKeysInSegmentIndexPage = maxNumberOfKeysInSegmentIndexPage;
        return this;
    }

    public IndexBuilder<K, V> withMaxNumberOfKeysInCache(
            final int maxNumberOfKeysInCache) {
        this.maxNumberOfKeysInCache = maxNumberOfKeysInCache;
        return this;
    }

    public IndexBuilder<K, V> withMaxNumberOfKeysInSegment(
            final int maxNumberOfKeysInSegment) {
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        return this;
    }

    public IndexBuilder<K, V> setMaxNumberOfSegmentsInCache(
            final int maxNumberOfSegmentsInCache) {
        this.maxNumberOfSegmentsInCache = maxNumberOfSegmentsInCache;
        return this;
    }

    public IndexBuilder<K, V> withBloomFilterNumberOfHashFunctions(
            final int bloomFilterNumberOfHashFunctions) {
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        return this;
    }

    public IndexBuilder<K, V> withIsIndexSynchronized(
            final boolean isIndexSynchronized) {
        this.isIndexSynchronized = isIndexSynchronized;
        return this;
    }

    public IndexBuilder<K, V> withBloomFilterIndexSizeInBytes(
            final int bloomFilterIndexSizeInBytes) {
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        return this;
    }

    public IndexBuilder<K, V> withIndexBufferSizeInBytes(
            final int indexBufferSizeInBytes) {
        this.indexBufferSizeInBytes = indexBufferSizeInBytes;
        return this;
    }

    public IndexBuilder<K, V> withUseFullLog(final boolean useFullLog) {
        this.useFullLog = useFullLog;
        return this;
    }

    public Index<K, V> build() {
        if (maxNumberOfKeysInSegmentCacheDuringFlushing == DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING) {
            maxNumberOfKeysInSegmentCacheDuringFlushing = maxNumberOfKeysInCache;
        }
        final SsstIndexConf conf = new SsstIndexConf(
                maxNumberOfKeysInSegmentCache,
                maxNumberOfKeysInSegmentCacheDuringFlushing,
                maxNumberOfKeysInSegmentIndexPage, maxNumberOfKeysInCache,
                maxNumberOfKeysInSegment, maxNumberOfSegmentsInCache,
                bloomFilterNumberOfHashFunctions, bloomFilterIndexSizeInBytes,
                indexBufferSizeInBytes);

        Log<K, V> log = null;
        if (useFullLog) {
            log = Log.<K, V>builder().withDirectory(directory)
                    .withFileName("log")
                    .withKeyTypeDescriptor(keyTypeDescriptor)
                    .withValueReader(valueTypeDescriptor.getTypeReader())
                    .withValueWriter(valueTypeDescriptor.getTypeWriter())
                    .build();
        } else {
            log = Log.<K, V>builder().buildEmpty();
        }
        final SstIndexImpl<K, V> index = new SstIndexImpl<>(directory,
                keyTypeDescriptor, valueTypeDescriptor, conf, log);
        if (isIndexSynchronized) {
            return new SstIndexSynchronized<>(index);
        } else {
            return index;
        }
    }

}
