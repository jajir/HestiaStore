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
public class SegmentDataDirectLoader<K, V>
                implements SegmentDataProvider<K, V> {

        private final SegmentFiles<K, V> segmentFiles;
        private final SegmentConf segmentConf;
        private final SegmentPropertiesManager segmentPropertiesManager;

        SegmentDataDirectLoader(final SegmentFiles<K, V> segmentFiles,
                        final SegmentConf segmentConf,
                        final SegmentPropertiesManager segmentPropertiesManager) {
                this.segmentFiles = Objects.requireNonNull(segmentFiles);
                this.segmentConf = Objects.requireNonNull(segmentConf);
                this.segmentPropertiesManager = Objects
                                .requireNonNull(segmentPropertiesManager);
        }

        @Override
        public SegmentDeltaCache<K, V> getSegmentDeltaCache() {
                return new SegmentDeltaCache<>(
                                segmentFiles.getKeyTypeDescriptor(),
                                segmentFiles, segmentPropertiesManager);
        }

        @Override
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

        @Override
        public ScarceIndex<K> getScarceIndex() {
                return ScarceIndex.<K>builder()
                                .withDirectory(segmentFiles.getDirectory())
                                .withFileName(segmentFiles.getScarceFileName())
                                .withKeyTypeDescriptor(segmentFiles
                                                .getKeyTypeDescriptor())
                                .build();
        }

        @Override
        public void invalidate() {
                // intentionally do nothing
        }

        /**
         * class always return new instance, so it's always loaded. Method
         * return <code>true</code>.
         */
        @Override
        public boolean isLoaded() {
                return true;
        }

}
