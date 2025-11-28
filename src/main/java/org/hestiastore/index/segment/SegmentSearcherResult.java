package org.hestiastore.index.segment;

/**
 * Mutable holder for search outcome.
 */
final class SegmentSearcherResult<V> {
    private V value;

    V getValue() {
        return value;
    }

    void setValue(final V value) {
        this.value = value;
    }
}
