package com.coroptis.index.segment;

import java.util.Objects;

/**
 * Class is responsible for creating new objects with complex segment
 * dependencies.
 */
public class SegmentManager<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentConf segmentConf;
    private final SegmentDataProvider<K, V> segmentCacheDataProvider;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;

    public SegmentManager(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentConf segmentConf,
            final SegmentDataProvider<K, V> segmentCacheDataProvider,
            final SegmentDeltaCacheController<K, V> deltaCacheController) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.segmentCacheDataProvider = Objects
                .requireNonNull(segmentCacheDataProvider);
        this.deltaCacheController = Objects
                .requireNonNull(deltaCacheController);
    }

    /**
     * Allows to re-write all data in segment.
     * 
     * @return segment writer object
     */
    public SegmentFullWriter<K, V> createSegmentFullWriter() {
        return new SegmentFullWriter<>(segmentFiles, segmentPropertiesManager,
                segmentConf.getMaxNumberOfKeysInIndexPage(),
                segmentCacheDataProvider, deltaCacheController);
    }

    /**
     * Create new segment.
     * 
     * @param segmentId rqeuired segment id
     * @return initialized segment
     */
    public Segment<K, V> createSegment(SegmentId segmentId) {
        return Segment.<K, V>builder()
                .withDirectory(segmentFiles.getDirectory())//
                .withId(segmentId)//
                .withKeyTypeDescriptor(segmentFiles.getKeyTypeDescriptor())//
                .withValueTypeDescriptor(segmentFiles.getValueTypeDescriptor())//
                .withSegmentConf(new SegmentConf(segmentConf))//
                .build();
    }
}