package com.coroptis.index.sst;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentConf;
import com.coroptis.index.segment.SegmentDataFactory;
import com.coroptis.index.segment.SegmentDataFactoryImpl;
import com.coroptis.index.segment.SegmentDataProvider;
import com.coroptis.index.segment.SegmentDataSupplier;
import com.coroptis.index.segment.SegmentFiles;
import com.coroptis.index.segment.SegmentId;
import com.coroptis.index.segment.SegmentPropertiesManager;

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
                this.keyTypeDescriptor = Objects
                                .requireNonNull(keyTypeDescriptor);
                this.valueTypeDescriptor = Objects
                                .requireNonNull(valueTypeDescriptor);
                this.conf = Objects.requireNonNull(conf);
                this.segmentDataCache = Objects
                                .requireNonNull(segmentDataCache);
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

                SegmentConf segmentConf = new SegmentConf(
                                conf.getMaxNumberOfKeysInSegmentCache(),
                                conf.getMaxNumberOfKeysInSegmentCacheDuringFlushing(),
                                conf.getMaxNumberOfKeysInSegmentIndexPage(),
                                conf.getBloomFilterNumberOfHashFunctions(),
                                conf.getBloomFilterIndexSizeInBytes(),
                                conf.getBloomFilterProbabilityOfFalsePositive(),
                                conf.getMaxNumberOfKeysInSegmentCache());
                // FIXME meaning of last parameter is not clear

                final SegmentPropertiesManager segmentPropertiesManager = new SegmentPropertiesManager(
                                directory, segmentId);

                final SegmentFiles<K, V> segmentFiles = new SegmentFiles<>(
                                directory, segmentId, keyTypeDescriptor,
                                valueTypeDescriptor,
                                conf.getFileReadingBufferSizeInBytes());

                final SegmentDataSupplier<K, V> segmentDataSupplier = new SegmentDataSupplier<>(
                                segmentFiles, segmentConf,
                                segmentPropertiesManager);

                final SegmentDataFactory<K, V> segmentDataFactory = new SegmentDataFactoryImpl<>(
                                segmentDataSupplier);

                final SegmentDataProvider<K, V> dataProvider = new SegmentDataProviderFromMainCache<>(
                                segmentId, segmentDataCache, segmentDataFactory);

                final Segment<K, V> out = Segment.<K, V>builder()
                                .withDirectory(directory).withId(segmentId)
                                .withKeyTypeDescriptor(keyTypeDescriptor)
                                .withSegmentDataProvider(dataProvider)//
                                .withSegmentConf(segmentConf)//
                                .withSegmentFiles(segmentFiles)//
                                .withSegmentPropertiesManager(segmentPropertiesManager)//
                                .withMaxNumberOfKeysInSegmentCache(conf
                                                .getMaxNumberOfKeysInSegmentCache())
                                .withMaxNumberOfKeysInIndexPage(conf
                                                .getMaxNumberOfKeysInSegmentIndexPage())
                                .withValueTypeDescriptor(valueTypeDescriptor)
                                .withBloomFilterNumberOfHashFunctions(conf
                                                .getBloomFilterNumberOfHashFunctions())
                                .withBloomFilterIndexSizeInBytes(conf
                                                .getBloomFilterIndexSizeInBytes())
                                .withSegmentDataProvider(dataProvider)
                                .withIndexBufferSizeInBytes(conf
                                                .getFileReadingBufferSizeInBytes())
                                .build();
                return out;
        }

        Directory getDirectory() {
                return directory;
        }

        public void close() {
                segmentDataCache.invalidateAll();
        }

}
