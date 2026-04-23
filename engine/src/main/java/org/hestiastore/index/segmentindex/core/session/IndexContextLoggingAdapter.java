package org.hestiastore.index.segmentindex.core.session;

import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;

/**
 * Adapter that wraps a {@link SegmentIndex} and ensures that the
 * {@code index.name} MDC key is present for every operation executed against
 * the wrapped index.
 *
 * @param <K> type of keys stored in the index
 * @param <V> type of values stored in the index
 */
final class IndexContextLoggingAdapter<K, V>
        extends AbstractCloseableResource implements SegmentIndex<K, V> {
    private final SegmentIndex<K, V> delegate;
    private final IndexContextScopeRunner contextScopeRunner;
    private final IndexControlPlane controlPlane;

    IndexContextLoggingAdapter(final IndexConfiguration<K, V> indexConf,
            final SegmentIndex<K, V> index) {
        this.delegate = Vldtn.requireNonNull(index, "index");
        final IndexConfiguration<K, V> configuration = Vldtn
                .requireNonNull(indexConf, "indexConfiguration");
        this.contextScopeRunner = new IndexContextScopeRunner(
                configuration.getIndexName());
        this.controlPlane = new IndexControlPlaneContextLoggingAdapter(
                delegate.controlPlane(), contextScopeRunner);
    }

    @Override
    public void put(final K key, final V value) {
        contextScopeRunner.run(() -> delegate.put(key, value));
    }

    @Override
    public void put(final Entry<K, V> entry) {
        contextScopeRunner.run(() -> delegate.put(entry));
    }

    @Override
    public V get(final K key) {
        return contextScopeRunner.supply(() -> delegate.get(key));
    }

    @Override
    public void delete(final K key) {
        contextScopeRunner.run(() -> delegate.delete(key));
    }

    @Override
    public void compact() {
        contextScopeRunner.run(delegate::compact);
    }

    @Override
    public void compactAndWait() {
        contextScopeRunner.run(delegate::compactAndWait);
    }

    @Override
    public void flush() {
        contextScopeRunner.run(delegate::flush);
    }

    @Override
    public void flushAndWait() {
        contextScopeRunner.run(delegate::flushAndWait);
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows) {
        return contextScopeRunner.supply(() -> delegate.getStream(segmentWindows));
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows,
            final SegmentIteratorIsolation isolation) {
        return contextScopeRunner
                .supply(() -> delegate.getStream(segmentWindows, isolation));
    }

    @Override
    public Stream<Entry<K, V>> getStream() {
        return contextScopeRunner.supply(delegate::getStream);
    }

    @Override
    public Stream<Entry<K, V>> getStream(
            final SegmentIteratorIsolation isolation) {
        return contextScopeRunner.supply(() -> delegate.getStream(isolation));
    }

    @Override
    public void checkAndRepairConsistency() {
        contextScopeRunner.run(delegate::checkAndRepairConsistency);
    }

    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return contextScopeRunner.supply(delegate::getConfiguration);
    }

    @Override
    public SegmentIndexState getState() {
        return contextScopeRunner.supply(delegate::getState);
    }

    @Override
    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return contextScopeRunner.supply(delegate::metricsSnapshot);
    }

    @Override
    public IndexControlPlane controlPlane() {
        return controlPlane;
    }

    @Override
    protected void doClose() {
        contextScopeRunner.run(delegate::close);
    }
}
