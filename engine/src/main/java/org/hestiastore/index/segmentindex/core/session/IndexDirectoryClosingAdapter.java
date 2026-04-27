package org.hestiastore.index.segmentindex.core.session;

import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;

/**
 * Wraps a {@link SegmentIndex} and ensures the internally created directory
 * adapter gets closed alongside the index.
 */
final class IndexDirectoryClosingAdapter<K, V>
        extends AbstractCloseableResource implements SegmentIndex<K, V> {

    private final SegmentIndex<K, V> delegate;
    private final Directory directory;
    private final CloseableResource onClose;

    IndexDirectoryClosingAdapter(final SegmentIndex<K, V> index,
            final Directory directory) {
        this(index, directory, null);
    }

    IndexDirectoryClosingAdapter(final SegmentIndex<K, V> index,
            final Directory directory, final CloseableResource onClose) {
        this.delegate = Vldtn.requireNonNull(index, "index");
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.onClose = onClose;
    }

    @Override
    public void put(final K key, final V value) {
        delegate.put(key, value);
    }

    @Override
    public void put(final Entry<K, V> entry) {
        delegate.put(entry);
    }

    @Override
    public V get(final K key) {
        return delegate.get(key);
    }

    @Override
    public void delete(final K key) {
        delegate.delete(key);
    }

    @Override
    public void compact() {
        delegate.compact();
    }

    @Override
    public void compactAndWait() {
        delegate.compactAndWait();
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public void flushAndWait() {
        delegate.flushAndWait();
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows) {
        return delegate.getStream(segmentWindows);
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows,
            final SegmentIteratorIsolation isolation) {
        return delegate.getStream(segmentWindows, isolation);
    }

    @Override
    public Stream<Entry<K, V>> getStream() {
        return delegate.getStream();
    }

    @Override
    public Stream<Entry<K, V>> getStream(
            final SegmentIteratorIsolation isolation) {
        return delegate.getStream(isolation);
    }

    @Override
    public void checkAndRepairConsistency() {
        delegate.checkAndRepairConsistency();
    }

    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return delegate.getConfiguration();
    }

    @Override
    public SegmentIndexState getState() {
        return delegate.getState();
    }

    @Override
    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return delegate.metricsSnapshot();
    }

    @Override
    public IndexControlPlane controlPlane() {
        return delegate.controlPlane();
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        final CloseFailureAccumulator closeFailures =
                new CloseFailureAccumulator();
        closeFailures.close(delegate);
        closeFailures.closeIfResource(directory);
        closeFailures.close(onClose);
        closeFailures.rethrowIfPresent();
    }

}
