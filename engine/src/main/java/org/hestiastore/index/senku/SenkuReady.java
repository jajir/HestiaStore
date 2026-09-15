package org.hestiastore.index.senku;

import java.util.Optional;
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
     * Returns the exact total from validated committed terminal manifests. Does
     * not read data pages or perform a full integrity scan.
     *
     * @return exact committed record count
     */
    long recordCount();

    /**
     * Returns a bounded approximate weighted natural-long distribution when
     * every nonempty terminal run contains one. Older indexes and other key
     * types may have no summary. The returned weights sum to recordCount().
     *
     * @return optional approximate distribution, never a membership index
     */
    Optional<SenkuLongKeySummary> longKeySummary();

    /**
     * Opens the only active lazy globally sorted stream.
     * Use try-with-resources to close it even after exhaustion or failure;
     * explicit stream close releases the slot for a subsequent stream. Each
     * stream has a single consumer. Coordinate consumption with stream and
     * handle close so those operations do not run concurrently.
     *
     * @return sorted entry stream
     */
    Stream<Entry<K, V>> openStream();

    /**
     * Closes the handle and any active stream, releasing the directory lock.
     * Repeated calls are harmless after a successful close. The supplied
     * directory remains owned by the caller.
     */
    @Override
    void close();
}
