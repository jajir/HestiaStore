package com.coroptis.index.sst;

import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;

public class SstIndexBuilder<K, V> {

    private final static long DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = 200_000;
    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_INDEX_PAGE = 5_000;

    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT = 10_000_000;
    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_CACHE = 1_000_000;

    private long maxNumberOfKeysInSegmentCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;
    private int maxNumberOfKeysInSegmentIndexPage = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_INDEX_PAGE;
    private int maxNumberOfKeysInCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_CACHE;
    private int maxNumberOfKeysInSegment = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT;

    private Directory directory;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;

    SstIndexBuilder() {

    }

    public SstIndexBuilder<K, V> withDirectory(final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public SstIndexBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        return this;
    }

    public SstIndexBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        return this;
    }

    public SstIndexBuilder<K, V> withMaxNumberOfKeysInSegmentCache(
            long maxNumberOfKeysInSegmentCache) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        return this;
    }

    public SstIndexBuilder<K, V> withMaxNumberOfKeysInSegmentIndexPage(
            int maxNumberOfKeysInSegmentIndexPage) {
        this.maxNumberOfKeysInSegmentIndexPage = maxNumberOfKeysInSegmentIndexPage;
        return this;
    }

    public SstIndexBuilder<K, V> withMaxNumberOfKeysInCache(
            int maxNumberOfKeysInCache) {
        this.maxNumberOfKeysInCache = maxNumberOfKeysInCache;
        return this;
    }

    public SstIndexBuilder<K, V> withMaxNumberOfKeysInSegment(
            int maxNumberOfKeysInSegment) {
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        return this;
    }

    public SstIndexImpl<K, V> build() {
        final SsstIndexConf conf = new SsstIndexConf(
                maxNumberOfKeysInSegmentCache,
                maxNumberOfKeysInSegmentIndexPage, maxNumberOfKeysInCache,
                maxNumberOfKeysInSegment);
        return new SstIndexImpl<>(directory, keyTypeDescriptor,
                valueTypeDescriptor, conf);
    }

}
