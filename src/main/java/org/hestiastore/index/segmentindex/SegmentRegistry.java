package org.hestiastore.index.segmentindex;

import java.util.HashMap;
import java.util.Map;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentConf;
import org.hestiastore.index.segment.SegmentResources;
import org.hestiastore.index.segment.SegmentDataSupplier;
import org.hestiastore.index.segment.SegmentFiles;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentPropertiesManager;
import org.hestiastore.index.segment.SegmentResourcesImpl;
import org.hestiastore.index.segment.SegmentSynchronizationAdapter;

public class SegmentRegistry<K, V> {

    private final Map<SegmentId, Segment<K, V>> segments = new HashMap<>();

    private final IndexConfiguration<K, V> conf;
    private final Directory directory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    SegmentRegistry(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
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

    private Segment<K, V> instantiateSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");

        SegmentConf segmentConf = new SegmentConf(
                conf.getMaxNumberOfKeysInSegmentCache().intValue(),
                conf.getMaxNumberOfKeysInSegmentCacheDuringFlushing()
                        .intValue(),
                conf.getMaxNumberOfKeysInSegmentChunk(),
                conf.getBloomFilterNumberOfHashFunctions(),
                conf.getBloomFilterIndexSizeInBytes(),
                conf.getBloomFilterProbabilityOfFalsePositive(),
                conf.getDiskIoBufferSize(), conf.getEncodingChunkFilters(),
                conf.getDecodingChunkFilters());

        final SegmentPropertiesManager segmentPropertiesManager = new SegmentPropertiesManager(
                directory, segmentId);

        final SegmentFiles<K, V> segmentFiles = new SegmentFiles<>(directory,
                segmentId, keyTypeDescriptor, valueTypeDescriptor,
                conf.getDiskIoBufferSize(), conf.getEncodingChunkFilters(),
                conf.getDecodingChunkFilters());

        final SegmentDataSupplier<K, V> segmentDataSupplier = new SegmentDataSupplier<>(
                segmentFiles, segmentConf, segmentPropertiesManager);

        final SegmentResources<K, V> dataProvider = new SegmentResourcesImpl<>(
                segmentDataSupplier);

        final Segment<K, V> segment = Segment.<K, V>builder()
                .withDirectory(directory)
                .withId(segmentId).withKeyTypeDescriptor(keyTypeDescriptor)
                .withSegmentResources(dataProvider)//
                .withSegmentConf(segmentConf)//
                .withSegmentFiles(segmentFiles)//
                .withSegmentPropertiesManager(segmentPropertiesManager)//
                .withMaxNumberOfKeysInSegmentCache(
                        conf.getMaxNumberOfKeysInSegmentCache().intValue())//
                .withMaxNumberOfKeysInSegmentChunk(
                        conf.getMaxNumberOfKeysInSegmentChunk())//
                .withValueTypeDescriptor(valueTypeDescriptor)//
                .withBloomFilterNumberOfHashFunctions(
                        conf.getBloomFilterNumberOfHashFunctions())//
                .withBloomFilterIndexSizeInBytes(
                        conf.getBloomFilterIndexSizeInBytes())//
                .withDiskIoBufferSize(conf.getDiskIoBufferSize())//
                .build();
        return new SegmentSynchronizationAdapter<>(segment);
    }

    private void deleteSegmentFiles(final SegmentId segmentId) {
        final SegmentFiles<K, V> segmentFiles = new SegmentFiles<>(directory,
                segmentId, keyTypeDescriptor, valueTypeDescriptor,
                conf.getDiskIoBufferSize(), conf.getEncodingChunkFilters(),
                conf.getDecodingChunkFilters());
        final SegmentPropertiesManager segmentPropertiesManager = new SegmentPropertiesManager(
                directory, segmentId);
        segmentFiles.deleteAllFiles(segmentPropertiesManager);
    }

    Directory getDirectory() {
        return directory;
    }

    public void close() {
    }

}
