package org.hestiastore.index.segmentindex;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * Inputs required to execute a split pipeline once.
 * Now mutable with getters/setters to ease validation and future extensions.
 */
final class SegmentSplitContext<K, V> {
    private Segment<K, V> segment;
    private SegmentSplitterPlan<K, V> plan;
    private SegmentId lowerSegmentId;
    private SegmentWriterTxFactory<K, V> writerTxFactory;

    SegmentSplitContext(final Segment<K, V> segment,
            final SegmentSplitterPlan<K, V> plan,
            final SegmentId lowerSegmentId,
            final SegmentWriterTxFactory<K, V> writerTxFactory) {
        this.segment = segment;
        this.plan = plan;
        this.lowerSegmentId = lowerSegmentId;
        this.writerTxFactory = writerTxFactory;
    }

    Segment<K, V> getSegment() {
        return segment;
    }

    void setSegment(final Segment<K, V> segment) {
        this.segment = segment;
    }

    SegmentSplitterPlan<K, V> getPlan() {
        return plan;
    }

    void setPlan(final SegmentSplitterPlan<K, V> plan) {
        this.plan = plan;
    }

    SegmentId getLowerSegmentId() {
        return lowerSegmentId;
    }

    void setLowerSegmentId(final SegmentId lowerSegmentId) {
        this.lowerSegmentId = lowerSegmentId;
    }

    SegmentWriterTxFactory<K, V> getWriterTxFactory() {
        return writerTxFactory;
    }

    void setWriterTxFactory(
            final SegmentWriterTxFactory<K, V> writerTxFactory) {
        this.writerTxFactory = writerTxFactory;
    }
}
