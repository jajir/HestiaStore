package org.hestiastore.index.segmentindex;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;

/**
 * Mutable state carried across split steps.
 */
final class SegmentSplitState<K, V> {
    private SegmentId lowerSegmentId;
    private EntryIterator<K, V> iterator;
    private SegmentSplitterResult<K, V> result;

    SegmentId getLowerSegmentId() {
        return lowerSegmentId;
    }

    void setLowerSegmentId(final SegmentId lowerSegmentId) {
        this.lowerSegmentId = lowerSegmentId;
    }

    EntryIterator<K, V> getIterator() {
        return iterator;
    }

    void setIterator(final EntryIterator<K, V> iterator) {
        this.iterator = iterator;
    }

    SegmentSplitterResult<K, V> getResult() {
        return result;
    }

    void setResult(final SegmentSplitterResult<K, V> result) {
        this.result = result;
    }
}
