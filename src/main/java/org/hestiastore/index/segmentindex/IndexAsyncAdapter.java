package org.hestiastore.index.segmentindex;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;

/**
 * Adapter that provides async operations by running synchronous calls on a
 * background thread and waiting for all in-flight async operations on close.
 *
 * @param <K> type of keys stored in the index
 * @param <V> type of values stored in the index
 */
public class IndexAsyncAdapter<K, V> extends AbstractCloseableResource
        implements SegmentIndex<K, V> {

    private final SegmentIndex<K, V> index;
    private final Object asyncMonitor = new Object();
    private int asyncInFlight = 0;
    private final ThreadLocal<Boolean> inAsyncOperation = ThreadLocal
            .withInitial(() -> Boolean.FALSE);

    IndexAsyncAdapter(final SegmentIndex<K, V> index) {
        this.index = Vldtn.requireNonNull(index, "index");
    }

    @Override
    public void put(final K key, final V value) {
        index.put(key, value);
    }

    @Override
    public CompletionStage<Void> putAsync(final K key, final V value) {
        return runAsyncTracked(() -> {
            put(key, value);
            return null;
        });
    }

    @Override
    public V get(final K key) {
        return index.get(key);
    }

    @Override
    public CompletionStage<V> getAsync(final K key) {
        return runAsyncTracked(() -> get(key));
    }

    @Override
    public void delete(final K key) {
        index.delete(key);
    }

    @Override
    public CompletionStage<Void> deleteAsync(final K key) {
        return runAsyncTracked(() -> {
            delete(key);
            return null;
        });
    }

    @Override
    public void compact() {
        index.compact();
    }

    @Override
    public void compactAndWait() {
        index.compactAndWait();
    }

    @Override
    public void flush() {
        index.flush();
    }

    @Override
    public void flushAndWait() {
        index.flushAndWait();
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows) {
        return index.getStream(segmentWindows);
    }

    @Override
    public void checkAndRepairConsistency() {
        index.checkAndRepairConsistency();
    }

    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return index.getConfiguration();
    }

    @Override
    public SegmentIndexState getState() {
        return index.getState();
    }

    @Override
    protected void doClose() {
        awaitAsyncOperations();
        index.close();
    }

    private <T> CompletionStage<T> runAsyncTracked(final Supplier<T> task) {
        incrementAsync();
        try {
            return CompletableFuture.supplyAsync(() -> {
                final boolean previous = Boolean.TRUE
                        .equals(inAsyncOperation.get());
                inAsyncOperation.set(Boolean.TRUE);
                try {
                    return task.get();
                } finally {
                    inAsyncOperation.set(previous);
                    decrementAsync();
                }
            });
        } catch (final RuntimeException e) {
            decrementAsync();
            throw e;
        }
    }

    private void incrementAsync() {
        synchronized (asyncMonitor) {
            asyncInFlight++;
        }
    }

    private void decrementAsync() {
        synchronized (asyncMonitor) {
            asyncInFlight--;
            asyncMonitor.notifyAll();
        }
    }

    private void awaitAsyncOperations() {
        if (Boolean.TRUE.equals(inAsyncOperation.get())) {
            throw new IllegalStateException(
                    "close() must not be called from an async index operation.");
        }
        boolean interrupted = false;
        synchronized (asyncMonitor) {
            while (asyncInFlight > 0) {
                try {
                    asyncMonitor.wait();
                } catch (final InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
