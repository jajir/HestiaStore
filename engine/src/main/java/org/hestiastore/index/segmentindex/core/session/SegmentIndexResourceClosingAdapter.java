package org.hestiastore.index.segmentindex.core.session;

import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfiguration;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;

/**
 * Wraps a {@link SegmentIndex} and ensures lifecycle close hooks run alongside
 * the index.
 */
public final class SegmentIndexResourceClosingAdapter<K, V>
        extends AbstractCloseableResource implements SegmentIndex<K, V> {

    private final SegmentIndex<K, V> delegate;
    private final ExecutorRegistry executorRegistry;

    public SegmentIndexResourceClosingAdapter(final SegmentIndex<K, V> index,
            final ExecutorRegistry executorRegistry) {
        this.delegate = Vldtn.requireNonNull(index, "index");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
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
    public RuntimeConfiguration runtimeConfiguration() {
        return delegate.runtimeConfiguration();
    }

    @Override
    public IndexRuntimeMonitoring runtimeMonitoring() {
        return delegate.runtimeMonitoring();
    }

    @Override
    public SegmentIndexMaintenance maintenance() {
        return delegate.maintenance();
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        final CloseFailureAccumulator closeFailures =
                new CloseFailureAccumulator();
        closeFailures.close(delegate);
        closeFailures.close(executorRegistry);
        closeFailures.rethrowIfPresent();
    }

}
