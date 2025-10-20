package org.hestiastore.index.segment;

import org.hestiastore.index.PairIterator;

/**
 * Mutable state carried across split steps.
 */
final class SegmentSplitState<K, V> {
    private SegmentImpl<K, V> lowerSegment;
    private PairIterator<K, V> iterator;

    SegmentImpl<K, V> getLowerSegment() {
        return lowerSegment;
    }

    void setLowerSegment(final SegmentImpl<K, V> lowerSegment) {
        this.lowerSegment = lowerSegment;
    }

    PairIterator<K, V> getIterator() {
        return iterator;
    }

    void setIterator(final PairIterator<K, V> iterator) {
        this.iterator = iterator;
    }
}
