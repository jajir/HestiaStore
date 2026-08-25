package org.hestiastore.index.senku;

import java.util.stream.Stream;

import org.hestiastore.index.Entry;

/**
 * Exclusive read-only handle for a finalized Senku index.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SenkuReady<K, V> extends AutoCloseable {

    /**
     * Opens the only active lazy globally sorted stream.
     *
     * @return sorted entry stream
     */
    Stream<Entry<K, V>> openStream();

    /**
     * Closes the handle and any active stream.
     */
    @Override
    void close();
}
