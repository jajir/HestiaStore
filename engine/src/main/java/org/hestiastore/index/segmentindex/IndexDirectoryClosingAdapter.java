package org.hestiastore.index.segmentindex;

import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;

/**
 * Wraps a {@link SegmentIndex} and ensures the internally created
 * directory adapter gets closed alongside the index.
 */
final class IndexDirectoryClosingAdapter<K, V>
        extends AbstractCloseableResource implements SegmentIndex<K, V> {

    private final SegmentIndex<K, V> index;
    private final Directory directory;
    private final CloseableResource onClose;

    IndexDirectoryClosingAdapter(final SegmentIndex<K, V> index,
            final Directory directory) {
        this(index, directory, null);
    }

    IndexDirectoryClosingAdapter(final SegmentIndex<K, V> index,
            final Directory directory,
            final CloseableResource onClose) {
        this.index = Vldtn.requireNonNull(index, "index");
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.onClose = onClose;
    }

    /** {@inheritDoc} */
    @Override
    public void put(final K key, final V value) {
        index.put(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> putAsync(final K key, final V value) {
        return index.putAsync(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public V get(final K key) {
        return index.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<V> getAsync(final K key) {
        return index.getAsync(key);
    }

    /** {@inheritDoc} */
    @Override
    public void delete(final K key) {
        index.delete(key);
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> deleteAsync(final K key) {
        return index.deleteAsync(key);
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
    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return index.metricsSnapshot();
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        RuntimeException failure = null;
        try {
            index.close();
        } catch (final RuntimeException e) {
            failure = e;
        }
        try {
            if (directory instanceof CloseableResource closeableDirectory) {
                closeableDirectory.close();
            }
        } catch (final RuntimeException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (onClose != null) {
            try {
                onClose.close();
            } catch (final RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

}
