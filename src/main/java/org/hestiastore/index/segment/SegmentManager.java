package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

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
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
        this.segmentCacheDataProvider = Vldtn.requireNonNull(
                segmentCacheDataProvider, "segmentCacheDataProvider");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
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
     * Allows to re-write all data in segment.
     * 
     * @return segment writer object
     */
    public SegmentFullWriterNew<K, V> createSegmentFullWriterNew() {
        return new SegmentFullWriterNew<>(segmentFiles,
                segmentPropertiesManager,
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