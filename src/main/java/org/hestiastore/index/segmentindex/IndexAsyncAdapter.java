package org.hestiastore.index.segmentindex;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentIteratorIsolation;

/**
 * Adapter that provides async operations by running synchronous calls on a
 * background thread.
 *
 * @param <K> type of keys stored in the index
 * @param <V> type of values stored in the index
 */
class IndexAsyncAdapter<K, V> extends AbstractCloseableResource
        implements SegmentIndex<K, V> {

    private final SegmentIndex<K, V> index;
    private final ThreadLocal<Boolean> inAsyncOperation = ThreadLocal
            .withInitial(() -> Boolean.FALSE);

    IndexAsyncAdapter(final SegmentIndex<K, V> index) {
        this.index = Vldtn.requireNonNull(index, "index");
    }

    /** {@inheritDoc} */
    @Override
    public void put(final K key, final V value) {
        index.put(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> putAsync(final K key, final V value) {
        return runAsyncTracked(() -> {
            put(key, value);
            return null;
        });
    }

    /** {@inheritDoc} */
    @Override
    public V get(final K key) {
        return index.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<V> getAsync(final K key) {
        return runAsyncTracked(() -> get(key));
    }

    /** {@inheritDoc} */
    @Override
    public void delete(final K key) {
        index.delete(key);
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> deleteAsync(final K key) {
        return runAsyncTracked(() -> {
            delete(key);
            return null;
        });
    }

    /** {@inheritDoc} */
    @Override
    public void compact() {
        index.compact();
    }

    /** {@inheritDoc} */
    @Override
    public void compactAndWait() {
        index.compactAndWait();
    }

    /** {@inheritDoc} */
    @Override
    public void flush() {
        index.flush();
    }

    /** {@inheritDoc} */
    @Override
    public void flushAndWait() {
        index.flushAndWait();
    }

    /** {@inheritDoc} */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows) {
        return index.getStream(segmentWindows);
    }

    /** {@inheritDoc} */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows,
            final SegmentIteratorIsolation isolation) {
        return index.getStream(segmentWindows, isolation);
    }

    /** {@inheritDoc} */
    @Override
    public void checkAndRepairConsistency() {
        index.checkAndRepairConsistency();
    }

    /** {@inheritDoc} */
    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return index.getConfiguration();
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState getState() {
        return index.getState();
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        if (Boolean.TRUE.equals(inAsyncOperation.get())) {
            throw new IllegalStateException(
                    "close() must not be called from an async index operation.");
        }
        index.close();
    }

    private <T> CompletionStage<T> runAsyncTracked(final Supplier<T> task) {
        return CompletableFuture.supplyAsync(() -> {
            final boolean previous = Boolean.TRUE
                    .equals(inAsyncOperation.get());
            inAsyncOperation.set(Boolean.TRUE);
            try {
                return task.get();
            } finally {
                inAsyncOperation.set(previous);
            }
        });
    }
}
