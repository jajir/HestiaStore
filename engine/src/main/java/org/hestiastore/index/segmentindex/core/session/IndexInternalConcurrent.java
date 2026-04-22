package org.hestiastore.index.segmentindex.core.session;

import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexImpl;
import org.hestiastore.index.segmentindex.core.maintenance.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.session.state.IndexState;

/**
 * Direct, caller-thread implementation of {@link SegmentIndex}.
 * <p>
 * Executes synchronous API calls on the caller thread while preserving the
 * existing {@link SegmentIndexImpl} retry behavior and iterator invalidation
 * rules.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexInternalConcurrent<K, V> extends AbstractCloseableResource
        implements IndexInternal<K, V> {

    private final SegmentIndexImpl<K, V> delegate;

    /**
     * Creates a concurrent index implementation bound to the given directory
     * and type descriptors.
     *
     * @param directoryFacade directory facade
     * @param keyTypeDescriptor key type descriptor
     * @param valueTypeDescriptor value type descriptor
     * @param conf configuration for the index
     * @param executorRegistry shared executor registry
     */
    public IndexInternalConcurrent(final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexRuntimeConfiguration<K, V> runtimeConfiguration,
            final IndexExecutorRegistry executorRegistry) {
        this.delegate = new StartedSegmentIndex<>(directoryFacade,
                keyTypeDescriptor, valueTypeDescriptor, conf,
                runtimeConfiguration, executorRegistry);
    }

    @Override
    public void put(final K key, final V value) {
        delegate.put(key, value);
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
    public void flush() {
        delegate.flush();
    }

    @Override
    public void compactAndWait() {
        delegate.compactAndWait();
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
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        return delegate.getStream(segmentWindow, isolation);
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

    public IndexState<K, V> getIndexState() {
        return delegate.getIndexState();
    }

    @Override
    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return delegate.metricsSnapshot();
    }

    @Override
    public IndexControlPlane controlPlane() {
        return delegate.controlPlane();
    }

    @Override
    public EntryIterator<K, V> openSegmentIterator(
            final SegmentWindow segmentWindows) {
        return delegate.openSegmentIterator(segmentWindows);
    }

    public EntryIterator<K, V> openSegmentIterator(
            final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        return delegate.openSegmentIterator(segmentWindow, isolation);
    }

    @Override
    protected void doClose() {
        delegate.close();
    }

    private static final class StartedSegmentIndex<K, V>
            extends SegmentIndexImpl<K, V> {

        StartedSegmentIndex(final Directory directoryFacade,
                final TypeDescriptor<K> keyTypeDescriptor,
                final TypeDescriptor<V> valueTypeDescriptor,
                final IndexConfiguration<K, V> conf,
                final IndexRuntimeConfiguration<K, V> runtimeConfiguration,
                final IndexExecutorRegistry executorRegistry) {
            super(directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf,
                    runtimeConfiguration, executorRegistry);
            completeStartup();
        }
    }
}
