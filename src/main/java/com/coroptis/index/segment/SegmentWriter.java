package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.segmentcache.SegmentCache;

/**
 * Allow to write new data to index.
 * 
 * Doesn't need to write to bloom filter because data are searched from cache
 * and than from checked against bloom filter.
 * 
 * @author honza
 *
 */
public class SegmentWriter<K, V> implements PairWriter<K, V> {

    private final Segment<K, V> segment;
    private final SegmentCache<K, V> cache;

    SegmentWriter(final Segment<K, V> segment) {
        this.segment = Objects.requireNonNull(segment);
        this.cache = Objects.requireNonNull(segment.getCache());
    }

    @Override
    public void put(final Pair<K, V> pair) {
        Objects.requireNonNull(pair);
        cache.put(pair);
    }

    @Override
    public void close() {
        segment.flush();
    }

}
