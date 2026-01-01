package org.hestiastore.index.segmentindex;

import java.util.HashMap;
import java.util.Map;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentBuilder;
import org.hestiastore.index.segment.SegmentConf;
import org.hestiastore.index.segment.SegmentDataSupplier;
import org.hestiastore.index.segment.SegmentFiles;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentPropertiesManager;
import org.hestiastore.index.segment.SegmentResources;
import org.hestiastore.index.segment.SegmentResourcesImpl;
import org.hestiastore.index.segment.SegmentSynchronizationAdapter;

public class SegmentRegistry<K, V> {

    private final Map<SegmentId, Segment<K, V>> segments = new HashMap<>();

    private final IndexConfiguration<K, V> conf;
    private final AsyncDirectory directoryFacade;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    SegmentRegistry(final AsyncDirectory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
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

    public void removeSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final Segment<K, V> segment = segments.remove(segmentId);
        if (segment != null && !segment.wasClosed()) {
            segment.close();
        }
        deleteSegmentFiles(segmentId);
    }

    void evictSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final Segment<K, V> segment = segments.remove(segmentId);
        if (segment != null && !segment.wasClosed()) {
            segment.close();
        }
    }

    SegmentBuilder<K, V> newSegmentBuilder(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final SegmentConf segmentConf = buildSegmentConf();
        final SegmentPropertiesManager segmentPropertiesManager = newSegmentPropertiesManager(
                segmentId);
        final SegmentFiles<K, V> segmentFiles = newSegmentFiles(segmentId);
        final SegmentDataSupplier<K, V> segmentDataSupplier = new SegmentDataSupplier<>(
                segmentFiles, segmentConf, segmentPropertiesManager);
        final SegmentResources<K, V> dataProvider = new SegmentResourcesImpl<>(
                segmentDataSupplier);

        return Segment.<K, V>builder()//
                .withAsyncDirectory(directoryFacade)//
                .withId(segmentId)//
                .withKeyTypeDescriptor(keyTypeDescriptor)//
                .withSegmentResources(dataProvider)//
                .withSegmentConf(segmentConf)//
                .withSegmentFiles(segmentFiles)//
                .withSegmentPropertiesManager(segmentPropertiesManager)//
                .withMaxNumberOfKeysInSegmentCache(
                        conf.getMaxNumberOfKeysInSegmentCache().intValue())//
                .withMaxNumberOfKeysInSegmentWriteCache(
                        conf.getMaxNumberOfKeysInSegmentWriteCache()
                                .intValue())//
                .withMaxNumberOfKeysInSegmentChunk(
                        conf.getMaxNumberOfKeysInSegmentChunk())//
                .withValueTypeDescriptor(valueTypeDescriptor)//
                .withBloomFilterNumberOfHashFunctions(
                        conf.getBloomFilterNumberOfHashFunctions())//
                .withBloomFilterIndexSizeInBytes(
                        conf.getBloomFilterIndexSizeInBytes())//
                .withDiskIoBufferSize(conf.getDiskIoBufferSize());
    }

    SegmentFiles<K, V> newSegmentFiles(final SegmentId segmentId) {
        return new SegmentFiles<>(directoryFacade, segmentId, keyTypeDescriptor,
                valueTypeDescriptor, conf.getDiskIoBufferSize(),
                conf.getEncodingChunkFilters(), conf.getDecodingChunkFilters());
    }

    SegmentPropertiesManager newSegmentPropertiesManager(
            final SegmentId segmentId) {
        return new SegmentPropertiesManager(directoryFacade, segmentId);
    }

    private SegmentConf buildSegmentConf() {
        return new SegmentConf(conf.getMaxNumberOfKeysInSegmentCache().intValue(),
                conf.getMaxNumberOfKeysInSegmentWriteCache().intValue(),
                conf.getMaxNumberOfKeysInSegmentChunk(),
                conf.getBloomFilterNumberOfHashFunctions(),
                conf.getBloomFilterIndexSizeInBytes(),
                conf.getBloomFilterProbabilityOfFalsePositive(),
                conf.getDiskIoBufferSize(), conf.getEncodingChunkFilters(),
                conf.getDecodingChunkFilters());
    }

    private Segment<K, V> instantiateSegment(final SegmentId segmentId) {
        final Segment<K, V> segment = newSegmentBuilder(segmentId).build();
        return new SegmentSynchronizationAdapter<>(segment);
    }

    private void deleteSegmentFiles(final SegmentId segmentId) {
        final SegmentFiles<K, V> segmentFiles = newSegmentFiles(segmentId);
        final SegmentPropertiesManager segmentPropertiesManager = newSegmentPropertiesManager(
                segmentId);
        segmentFiles.deleteAllFiles(segmentPropertiesManager);
    }

    public void close() {
    }

}
