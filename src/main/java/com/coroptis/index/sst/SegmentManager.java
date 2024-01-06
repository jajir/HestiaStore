package com.coroptis.index.sst;

import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.cache.Cache;
import com.coroptis.index.cache.CacheLru;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentFiles;
import com.coroptis.index.segment.SegmentId;

public class SegmentManager<K, V> {

    final Cache<SegmentId, Segment<K, V>> cache;

    private final SsstIndexConf conf;
    private final Directory directory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    SegmentManager(final Directory directory,
            TypeDescriptor<K> keyTypeDescriptor,
            TypeDescriptor<V> valueTypeDescriptor, final SsstIndexConf conf) {
        this.directory = Objects.requireNonNull(directory);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.conf = Objects.requireNonNull(conf);
        cache = new CacheLru<>(conf.getMaxNumberOfSegmentsInCache(),
                (segmenId, segment) -> {
                    segment.close();
                });
    }

    public Segment<K, V> getSegment(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId, "Segment id is required");
        final Optional<Segment<K, V>> oSegment = cache.get(segmentId);
        if (oSegment.isEmpty()) {
            final Segment<K, V> out = instantiateSegment(segmentId);
            cache.put(segmentId, out);
            return out;
        } else {
            return oSegment.get();
        }
    }

    public boolean isInCache(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId, "Segment id is required");
        final Optional<Segment<K, V>> oSegment = cache.get(segmentId);
        return oSegment.isPresent();
    }

    public SegmentFiles<K, V> getSegmentFiles(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId, "Segment id is required");
        return new SegmentFiles<K, V>(directory, segmentId, keyTypeDescriptor,
                valueTypeDescriptor);
    }

    private Segment<K, V> instantiateSegment(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId, "Segment id is required");
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
                .build();
        return out;
    }

    Directory getDirectory() {
        return directory;
    }

}
