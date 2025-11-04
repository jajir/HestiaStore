package org.hestiastore.index.segment;

import org.hestiastore.index.EntryIterator;

/**
 * Mutable state carried across split steps.
 */
final class SegmentSplitState<K, V> {
    private SegmentImpl<K, V> lowerSegment;
    private EntryIterator<K, V> iterator;

    SegmentImpl<K, V> getLowerSegment() {
        return lowerSegment;
    }

    void setLowerSegment(final SegmentImpl<K, V> lowerSegment) {
        this.lowerSegment = lowerSegment;
    }

    EntryIterator<K, V> getIterator() {
        return iterator;
    }

    void setIterator(final EntryIterator<K, V> iterator) {
        this.iterator = iterator;
    }
}
