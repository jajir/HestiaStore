package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.scarceindex.ScarceIndex;

/**
 * When any getter is called than new instance of object is created and
 * returned.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentDataSupplier<K, V> {

        private final SegmentFiles<K, V> segmentFiles;
        private final SegmentConf segmentConf;
        private final SegmentPropertiesManager segmentPropertiesManager;

        public SegmentDataSupplier(final SegmentFiles<K, V> segmentFiles,
                        final SegmentConf segmentConf,
                        final SegmentPropertiesManager segmentPropertiesManager) {
                this.segmentFiles = Objects.requireNonNull(segmentFiles);
                this.segmentConf = Objects.requireNonNull(segmentConf);
                this.segmentPropertiesManager = Objects
                                .requireNonNull(segmentPropertiesManager);
        }

        public SegmentDeltaCache<K, V> getSegmentDeltaCache() {
                return new SegmentDeltaCache<>(
                                segmentFiles.getKeyTypeDescriptor(),
                                segmentFiles, segmentPropertiesManager);
        }

        public BloomFilter<K> getBloomFilter() {
                return BloomFilter.<K>builder()
                                .withBloomFilterFileName(segmentFiles
                                                .getBloomFilterFileName())
                                .withConvertorToBytes(segmentFiles
                                                .getKeyTypeDescriptor()
                                                .getConvertorToBytes())
                                .withDirectory(segmentFiles.getDirectory())
                                .withIndexSizeInBytes(segmentConf
                                                .getBloomFilterIndexSizeInBytes())
                                .withNumberOfHashFunctions(segmentConf
                                                .getBloomFilterNumberOfHashFunctions())
                                .withProbabilityOfFalsePositive(segmentConf
                                                .getBloomFilterProbabilityOfFalsePositive())
                                .build();
        }

        public ScarceIndex<K> getScarceIndex() {
                return ScarceIndex.<K>builder()
                                .withDirectory(segmentFiles.getDirectory())
                                .withFileName(segmentFiles.getScarceFileName())
                                .withKeyTypeDescriptor(segmentFiles
                                                .getKeyTypeDescriptor())
                                .build();
        }

}
