package org.hestiastore.index;

import java.util.Objects;

/**
 * Key/value entry object stored in the index.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class Entry<K, V> {

    private final K key;

    private final V value;

    public Entry(final K key, final V value) {
        this.key = key;
        this.value = value;
    }

    public static <M, N> Entry<M, N> of(final M m, final N n) {
        return new Entry<M, N>(m, n);
    }

    @Override
    public String toString() {
        return String.format("Entry[key='%s',value='%s']", key, value);
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Entry<K, V> other = (Entry<K, V>) obj;
        return Objects.equals(key, other.key)
                && Objects.equals(value, other.value);
    }

}
