package org.hestiastore.index.cache;

/**
 * Base representation for values stored inside {@link CacheLru}. The element
 * keeps track of the last access counter so the LRU implementation can
 * determine ordering without peeking into the wrapped value.
 *
 * @param <V> cached value type
 */
public abstract class CacheElement<V> {

    private volatile long cx;

    CacheElement(final long initialCx) {
        cx = initialCx;
    }

    /**
     * Returns the counter that denotes the last time this element was touched.
     *
     * @return monotonic counter used by the LRU eviction logic
     */
    public final long getCx() {
        return cx;
    }

    /**
     * Updates the access counter. This is invoked whenever the element is read
     * so that it bubbles up as the most recently used entry.
     *
     * @param cx new access counter
     */
    public final void setCx(final long cx) {
        this.cx = cx;
    }

    /**
     * @return {@code true} when the element represents a cached null.
     */
    public abstract boolean isNull();

    /**
     * Returns the stored value.
     *
     * @throws IllegalStateException when called on null-only elements
     */
    public abstract V getValue();

}
