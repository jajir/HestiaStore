package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.scarceindex.ScarceIndex;

/**
 * Object use in memory cache and bloom filter. Only one instance for one
 * segment should be in memory at the time.
 * 
 * This object can be cached in memory.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentSearcherCore<K, V> implements CloseableResource {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentIndexSearcher<K, V> segmentIndexSearcher;
    private final SegmentCacheDataProvider<K, V> segmentCacheDataProvider;

    public SegmentSearcherCore(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentIndexSearcher<K, V> segmentIndexSearcher,
            final SegmentCacheDataProvider<K, V> segmentCacheDataProvider) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentCacheDataProvider = Objects.requireNonNull(
                segmentCacheDataProvider,
                "Segment cached data provider is required");
        this.segmentIndexSearcher = Objects
                .requireNonNull(segmentIndexSearcher);
    }

    private SegmentDeltaCache<K, V> getDeltaCache() {
        return segmentCacheDataProvider.getSegmentDeltaCache();
    };

    private ScarceIndex<K> getScarceIndex() {
        return segmentCacheDataProvider.getScarceIndex();
    };

    BloomFilter<K> getBloomFilter() {
        return segmentCacheDataProvider.getBloomFilter();
    }

    public K getMaxKey() {
        return getScarceIndex().getMaxKey();
    }

    public K getMinKey() {
        return getScarceIndex().getMinKey();
    }

    public V get(final K key) {
        // look in cache
        final V out = getDeltaCache().get(key);
        if (segmentFiles.getValueTypeDescriptor().isTombstone(out)) {
            return null;
        }

        // look in bloom filter
        if (out == null) {
            if (getBloomFilter().isNotStored(key)) {
                /*
                 * It;s sure that key is not in index.
                 */
                return null;
            }
        }

        // look in index file
        if (out == null) {
            final Integer position = getScarceIndex().get(key);
            if (position == null) {
                return null;
            }
            return segmentIndexSearcher.search(key, position);
        }
        return out;
    }

    void addPairIntoCache(final Pair<K, V> pair) {
        getDeltaCache().put(pair);
    }

    @Deprecated
    SegmentDeltaCache<K, V> getCache() {
        return getDeltaCache();
    }

    @Override
    public void close() {
        segmentIndexSearcher.close();
    }

}
