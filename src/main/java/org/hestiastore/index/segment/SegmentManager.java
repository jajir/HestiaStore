package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

/**
 * Class is responsible for creating new objects with complex segment
 * dependencies.
 */
public class SegmentManager<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentConf segmentConf;

    public SegmentManager(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
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