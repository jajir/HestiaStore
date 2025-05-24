package com.hestiastore.index.cache;

import java.util.Objects;

public class CacheLruElement<V> {

    private final V value;

    private long cx;

    CacheLruElement(final V value, long initialCx) {
        this.value = Objects.requireNonNull(value);
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
