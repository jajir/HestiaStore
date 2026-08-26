package org.hestiastore.index.senku;

/**
 * Write-only Senku lifecycle handle.
 * <p>
 * The handle is thread-safe. Concurrent puts may mutate distinct keys in
 * parallel and may overlap while full maps are flushed. Duplicate values for
 * one key are reduced atomically without preserving their write order. It
 * deliberately has no close or abort operation; writing ends only through
 * {@link #finishWriting()}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SenkuWriting<K, V> {

    /**
     * Adds or merges one key-value pair.
     *
     * @param key   non-null key
     * @param value non-null value
     */
    void put(K key, V value);

    /**
     * Finishes ingestion and returns the exclusive ready handle.
     *
     * @return ready handle after all maintenance completes
     */
    SenkuReady<K, V> finishWriting();
}
