package org.hestiastore.index.cache;

import org.hestiastore.index.Vldtn;

/**
 * Holder for a real cached value. This is the counterpart to
 * {@link CacheNullElement} and simply wraps the payload while delegating all
 * access tracking to {@link CacheElement}.
 *
 * @param <V> cached value type
 */
public class CacheValueElement<V> extends CacheElement<V> {

    private final V value;

    CacheValueElement(final V value, final long initialCx) {
        super(initialCx);
        this.value = Vldtn.requireNonNull(value, "value");
    }

    /**
     * @return {@code false} because the element always represents a value.
     */
    @Override
    public boolean isNull() {
        return false;
    }

    /**
     * @return cached payload
     */
    @Override
    public V getValue() {
        return value;
    }

}
