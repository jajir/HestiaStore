package org.hestiastore.index.cache;

/**
 * Marker element stored by {@link CacheLru} when a lookup resulted in a null
 * value. Keeping an explicit entry allows future reads to short-circuit without
 * issuing another segment lookup.
 *
 * @param <V> cached value type
 */
public class CacheNullElement<V> extends CacheElement<V> {

    public CacheNullElement(final long initialCx) {
        super(initialCx);
    }

    /**
     * Null marker always behaves as a null value.
     */
    @Override
    public boolean isNull() {
        return true;
    }

    /**
     * This implementation never exposes a backing value.
     *
     * @throws IllegalStateException always
     */
    @Override
    public V getValue() {
        throw new IllegalStateException("Null element does not have a value");
    }

}
