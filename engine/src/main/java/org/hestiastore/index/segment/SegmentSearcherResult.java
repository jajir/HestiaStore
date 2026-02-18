package org.hestiastore.index.segment;

/**
 * Mutable holder for search outcome.
 */
final class SegmentSearcherResult<V> {
    private V value;

    /**
     * Returns the resolved value, if any.
     *
     * @return value or null when not found
     */
    V getValue() {
        return value;
    }

    /**
     * Sets the resolved value for this search.
     *
     * @param value resolved value
     */
    void setValue(final V value) {
        this.value = value;
    }
}
