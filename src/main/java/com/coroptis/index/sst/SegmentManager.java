package com.coroptis.index.sst;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentDataProvider;
import com.coroptis.index.segment.SegmentId;

public class SegmentManager<K, V> {

    private final Map<SegmentId, Segment<K, V>> segments = new HashMap<>();

    private final SsstIndexConf conf;
    private final Directory directory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final SegmentDataCache<K, V> segmentDataCache;

    SegmentManager(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SsstIndexConf conf,
            final SegmentDataCache<K, V> segmentDataCache) {
        this.directory = Objects.requireNonNull(directory);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.conf = Objects.requireNonNull(conf);
        this.segmentDataCache = Objects.requireNonNull(segmentDataCache);
    }

    public Segment<K, V> getSegment(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId, "Segment id is required");
        Segment<K, V> out = segments.get(segmentId);
        if (out == null) {
            out = instantiateSegment(segmentId);
            segments.put(segmentId, out);
        }
        return out;
    }

    private Segment<K, V> instantiateSegment(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId, "Segment id is required");
        SegmentDataProvider<K, V> dataProvider = new SegmentDataProviderFromMainCache<>(
                segmentId, this, segmentDataCache);
        final Segment<K, V> out = Segment.<K, V>builder()
                .withDirectory(directory).withId(segmentId)
                .withKeyTypeDescriptor(keyTypeDescriptor)
                .withMaxNumberOfKeysInSegmentCache(
                        conf.getMaxNumberOfKeysInSegmentCache())
                .withMaxNumberOfKeysInIndexPage(
                        conf.getMaxNumberOfKeysInSegmentIndexPage())
                .withValueTypeDescriptor(valueTypeDescriptor)
                .withBloomFilterNumberOfHashFunctions(
                        conf.getBloomFilterNumberOfHashFunctions())
                .withBloomFilterIndexSizeInBytes(
                        conf.getBloomFilterIndexSizeInBytes())
                .withSegmentDataProvider(dataProvider).build();
        return out;
    }

    Directory getDirectory() {
        return directory;
    }

}
