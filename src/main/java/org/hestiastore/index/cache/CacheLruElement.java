package org.hestiastore.index.cache;

import org.hestiastore.index.Vldtn;

public class CacheLruElement<V> {

    private final V value;

    private long cx;

    CacheLruElement(final V value, long initialCx) {
        this.value = Vldtn.requireNonNull(value, "value");
        cx = initialCx;
    }

    public long getCx() {
        return cx;
    }

    public void setCx(long cx) {
        this.cx = cx;
    }

    public V getValue() {
        return value;
    }

}
