package org.hestiastore.index.sst;

import java.util.HashMap;
import java.util.Map;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentConf;
import org.hestiastore.index.segment.SegmentDataFactory;
import org.hestiastore.index.segment.SegmentDataFactoryImpl;
import org.hestiastore.index.segment.SegmentDataProvider;
import org.hestiastore.index.segment.SegmentDataSupplier;
import org.hestiastore.index.segment.SegmentFiles;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentPropertiesManager;

public class SegmentRegistry<K, V> {

    private final Map<SegmentId, Segment<K, V>> segments = new HashMap<>();

    private final IndexConfiguration<K, V> conf;
    private final Directory directory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final SegmentDataCache<K, V> segmentDataCache;

    SegmentRegistry(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final SegmentDataCache<K, V> segmentDataCache) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.segmentDataCache = Vldtn.requireNonNull(segmentDataCache,
                "segmentDataCache");
    }

    public Segment<K, V> getSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Segment<K, V> out = segments.get(segmentId);
        if (out == null) {
            out = instantiateSegment(segmentId);
            segments.put(segmentId, out);
        }
        return out;
    }

    private Segment<K, V> instantiateSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");

        SegmentConf segmentConf = new SegmentConf(
                conf.getMaxNumberOfKeysInSegmentCache(),
                conf.getMaxNumberOfKeysInSegmentCacheDuringFlushing(),
                conf.getMaxNumberOfKeysInSegmentIndexPage(),
                conf.getBloomFilterNumberOfHashFunctions(),
                conf.getBloomFilterIndexSizeInBytes(),
                conf.getBloomFilterProbabilityOfFalsePositive(),
                conf.getDiskIoBufferSize());

        final SegmentPropertiesManager segmentPropertiesManager = new SegmentPropertiesManager(
                directory, segmentId);

        final SegmentFiles<K, V> segmentFiles = new SegmentFiles<>(directory,
                segmentId, keyTypeDescriptor, valueTypeDescriptor,
                conf.getDiskIoBufferSize());

        final SegmentDataSupplier<K, V> segmentDataSupplier = new SegmentDataSupplier<>(
                segmentFiles, segmentConf, segmentPropertiesManager);

        final SegmentDataFactory<K, V> segmentDataFactory = new SegmentDataFactoryImpl<>(
                segmentDataSupplier);

        final SegmentDataProvider<K, V> dataProvider = new SegmentDataProviderFromMainCache<>(
                segmentId, segmentDataCache, segmentDataFactory);

        return Segment.<K, V>builder().withDirectory(directory)
                .withId(segmentId).withKeyTypeDescriptor(keyTypeDescriptor)
                .withSegmentDataProvider(dataProvider)//
                .withSegmentConf(segmentConf)//
                .withSegmentFiles(segmentFiles)//
                .withSegmentPropertiesManager(segmentPropertiesManager)//
                .withMaxNumberOfKeysInSegmentCache(
                        conf.getMaxNumberOfKeysInSegmentCache())//
                .withMaxNumberOfKeysInIndexPage(
                        conf.getMaxNumberOfKeysInSegmentIndexPage())//
                .withValueTypeDescriptor(valueTypeDescriptor)//
                .withBloomFilterNumberOfHashFunctions(
                        conf.getBloomFilterNumberOfHashFunctions())//
                .withBloomFilterIndexSizeInBytes(
                        conf.getBloomFilterIndexSizeInBytes())//
                .withSegmentDataProvider(dataProvider)//
                .withDiskIoBufferSize(conf.getDiskIoBufferSize())//
                .build();
    }

    Directory getDirectory() {
        return directory;
    }

    public void close() {
        segmentDataCache.invalidateAll();
    }

}
