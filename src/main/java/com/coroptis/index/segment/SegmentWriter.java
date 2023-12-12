package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.bloomfilter.BloomFilterWriter;
import com.coroptis.index.cache.UniqueCache;

/**
 */
public class SegmentWriter<K, V> implements PairWriter<K, V> {

    private final Segment<K, V> segment;
    private final UniqueCache<K, V> cache;
    private final BloomFilterWriter<K> bloomFilterWriter;

    SegmentWriter(final Segment<K, V> segment) {
        this.segment = Objects.requireNonNull(segment);
        this.cache = Objects.requireNonNull(segment.getCache());
        bloomFilterWriter = segment.openBloomFilterWriter();
    }

    @Override
    public void put(final Pair<K, V> pair) {
        Objects.requireNonNull(pair);
        cache.put(pair);
        bloomFilterWriter.write(pair.getKey());
    }

    @Override
    public void close() {
        segment.flush();
        bloomFilterWriter.close();
    }

}
