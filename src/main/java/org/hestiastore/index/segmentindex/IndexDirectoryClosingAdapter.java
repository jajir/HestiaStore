package org.hestiastore.index.segmentindex;

import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.async.AsyncDirectory;

/**
 * Wraps a {@link SegmentIndex} and ensures the internally created
 * {@link AsyncDirectory} gets closed alongside the index.
 */
final class IndexDirectoryClosingAdapter<K, V>
        extends AbstractCloseableResource implements SegmentIndex<K, V> {

    private final SegmentIndex<K, V> index;
    private final AsyncDirectory asyncDirectory;

    IndexDirectoryClosingAdapter(final SegmentIndex<K, V> index,
            final AsyncDirectory asyncDirectory) {
        this.index = Vldtn.requireNonNull(index, "index");
        this.asyncDirectory = Vldtn.requireNonNull(asyncDirectory,
                "asyncDirectory");
    }

    @Override
    public void put(final K key, final V value) {
        index.put(key, value);
    }

    @Override
    public CompletionStage<Void> putAsync(final K key, final V value) {
        return index.putAsync(key, value);
    }

    @Override
    public V get(final K key) {
        return index.get(key);
    }

    @Override
    public CompletionStage<V> getAsync(final K key) {
        return index.getAsync(key);
    }

    @Override
    public void delete(final K key) {
        index.delete(key);
    }

    @Override
    public CompletionStage<Void> deleteAsync(final K key) {
        return index.deleteAsync(key);
    }

    @Override
    public void compact() {
        index.compact();
    }

    @Override
    public void flush() {
        index.flush();
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
    protected void doClose() {
        RuntimeException failure = null;
        try {
            index.close();
        } catch (final RuntimeException e) {
            failure = e;
        }
        try {
            asyncDirectory.close();
        } catch (final RuntimeException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
