package org.hestiastore.index.segmentindex;

import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.slf4j.MDC;

/**
 * Adapter that wraps a {@link SegmentIndex} and ensures that the
 * {@code index.name} MDC key is present for every operation executed against
 * the wrapped index.
 *
 * <p>
 * This allows downstream logging to consistently include the index name no
 * matter which thread initiates the call.
 * </p>
 *
 * @param <K> type of keys stored in the index
 * @param <V> type of values stored in the index
 */
public class IndexContextLoggingAdapter<K, V> extends AbstractCloseableResource
        implements SegmentIndex<K, V> {

    private final static String INDEX_NAME_MDC_KEY = "index.name";
    private final IndexConfiguration<K, V> indexConf;
    private final SegmentIndex<K, V> index;

    /**
     * Creates a new adapter that augments the provided index with MDC context
     * handling.
     *
     * @param indexConf configuration that supplies the index name
     * @param index     delegate that performs the actual index operations
     */
    IndexContextLoggingAdapter(final IndexConfiguration<K, V> indexConf,
            final SegmentIndex<K, V> index) {
        this.indexConf = Vldtn.requireNonNull(indexConf, "indexConfiguration");
        this.index = Vldtn.requireNonNull(index, "index");
    }

    /** Sets the {@code index.name} MDC key for the current thread. */
    private void setContext() {
        MDC.put(INDEX_NAME_MDC_KEY, indexConf.getIndexName());
    }

    /** Removes the {@code index.name} MDC key for the current thread. */
    private void clearContext() {
        MDC.remove(INDEX_NAME_MDC_KEY);
    }

    /**
     * Delegates to {@link SegmentIndex#put(Object, Object)} while ensuring the
     * name is present in the MDC for any resulting log statements.
     *
     * @param key   key to store
     * @param value value to associate with the key
     */
    @Override
    public void put(final K key, final V value) {
        setContext();
        try {
            index.put(key, value);
        } finally {
            clearContext();
        }
    }

    /**
     * Retrieves a value by key while populating the MDC with the index name.
     *
     * @param key key whose value should be retrieved
     * @return value associated with the key, or {@code null} if the key is
     *         absent
     */
    @Override
    public V get(final K key) {
        setContext();
        try {
            return index.get(key);
        } finally {
            clearContext();
        }
    }

    /**
     * Removes the mapping for a key while ensuring the {@code index.name} MDC
     * context is active.
     *
     * @param key key to remove
     */
    @Override
    public void delete(final K key) {
        setContext();
        try {
            index.delete(key);
        } finally {
            clearContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> putAsync(final K key, final V value) {
        setContext();
        try {
            return index.putAsync(key, value);
        } finally {
            clearContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<V> getAsync(final K key) {
        setContext();
        try {
            return index.getAsync(key);
        } finally {
            clearContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> deleteAsync(final K key) {
        setContext();
        try {
            return index.deleteAsync(key);
        } finally {
            clearContext();
        }
    }

    /**
     * Triggers compaction on the delegate index while applying the MDC context.
     */
    @Override
    public void compact() {
        setContext();
        try {
            index.compact();
        } finally {
            clearContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void compactAndWait() {
        setContext();
        try {
            index.compactAndWait();
        } finally {
            clearContext();
        }
    }

    /**
     * Flushes any pending index changes while keeping the {@code index.name} in
     * the MDC.
     */
    @Override
    public void flush() {
        setContext();
        try {
            index.flush();
        } finally {
            clearContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void flushAndWait() {
        setContext();
        try {
            index.flushAndWait();
        } finally {
            clearContext();
        }
    }

    /**
     * Returns a stream of index entries with the MDC populated so any
     * streaming-related logs include the index name.
     *
     * @param segmentWindows segment selection to stream
     * @return stream of key-value entries from the selected segments
     */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows) {
        setContext();
        try {
            return index.getStream(segmentWindows);
        } finally {
            clearContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows,
            final SegmentIteratorIsolation isolation) {
        setContext();
        try {
            return index.getStream(segmentWindows, isolation);
        } finally {
            clearContext();
        }
    }

    /**
     * Verifies and repairs index consistency while guaranteeing the index name
     * appears in log statements.
     */
    @Override
    public void checkAndRepairConsistency() {
        setContext();
        try {
            index.checkAndRepairConsistency();
        } finally {
            clearContext();
        }
    }

    /**
     * Returns the configuration of the delegate index while applying the MDC
     * context.
     *
     * @return configuration of the wrapped index
     */
    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        setContext();
        try {
            return index.getConfiguration();
        } finally {
            clearContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState getState() {
        setContext();
        try {
            return index.getState();
        } finally {
            clearContext();
        }
    }

    /**
     * Closes the wrapped index while maintaining the {@code index.name} MDC
     * context.
     */
    @Override
    protected void doClose() {
        setContext();
        try {
            index.close();
        } finally {
            clearContext();
        }
    }
}
