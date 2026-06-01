package org.hestiastore.index.segmentindex.core.session;

import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;

/**
 * Wraps an internal index and ensures lifecycle close hooks run alongside
 * the index.
 */
public final class SegmentIndexResourceClosingAdapter<K, V>
        extends AbstractCloseableResource implements IndexInternal<K, V> {

    private final IndexInternal<K, V> delegate;

    public SegmentIndexResourceClosingAdapter(final IndexInternal<K, V> index) {
        this.delegate = Vldtn.requireNonNull(index, "index");
    }

    @Override
    public void put(final K key, final V value) {
        ensureOpen();
        delegate.put(key, value);
    }

    @Override
    public void put(final Entry<K, V> entry) {
        ensureOpen();
        delegate.put(entry);
    }

    @Override
    public V get(final K key) {
        ensureOpen();
        return delegate.get(key);
    }

    @Override
    public void delete(final K key) {
        ensureOpen();
        delegate.delete(key);
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows) {
        ensureOpen();
        return delegate.getStream(segmentWindows);
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows,
            final SegmentIteratorIsolation isolation) {
        ensureOpen();
        return delegate.getStream(segmentWindows, isolation);
    }

    @Override
    public Stream<Entry<K, V>> getStream() {
        ensureOpen();
        return delegate.getStream();
    }

    @Override
    public Stream<Entry<K, V>> getStream(
            final SegmentIteratorIsolation isolation) {
        ensureOpen();
        return delegate.getStream(isolation);
    }

    @Override
    public EntryIterator<K, V> openSegmentIterator(
            final SegmentWindow segmentWindows) {
        ensureOpen();
        return delegate.openSegmentIterator(segmentWindows);
    }

    @Override
    public RuntimeTuning runtimeTuning() {
        ensureOpen();
        return delegate.runtimeTuning();
    }

    @Override
    public IndexRuntimeMonitoring runtimeMonitoring() {
        return delegate.runtimeMonitoring();
    }

    @Override
    public SegmentIndexMaintenance maintenance() {
        ensureOpen();
        return delegate.maintenance();
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        delegate.close();
    }

    private void ensureOpen() {
        if (wasClosed()) {
            throw new IllegalStateException(
                    "Can't perform operation on closed index.");
        }
    }

}
