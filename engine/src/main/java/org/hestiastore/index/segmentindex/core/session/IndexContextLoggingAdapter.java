package org.hestiastore.index.segmentindex.core.session;

import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.IndexMdcScopeRunner;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenanceContextLoggingAdapter;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningContextLoggingAdapter;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoringContextLoggingAdapter;

/**
 * Adapter that wraps an internal index and ensures that the
 * {@code index.name} MDC key is present for every operation executed against
 * the wrapped index.
 *
 * @param <K> type of keys stored in the index
 * @param <V> type of values stored in the index
 */
public final class IndexContextLoggingAdapter<K, V>
        extends AbstractCloseableResource implements IndexInternal<K, V> {
    private final IndexInternal<K, V> delegate;
    private final IndexMdcScopeRunner contextScopeRunner;
    private final RuntimeTuning runtimeConfiguration;
    private final IndexRuntimeMonitoring runtimeMonitoring;
    private final SegmentIndexMaintenance maintenance;

    public IndexContextLoggingAdapter(final IndexInternal<K, V> delegate,
            final IndexMdcScopeRunner contextScopeRunner) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.contextScopeRunner = Vldtn.requireNonNull(contextScopeRunner,
                "contextScopeRunner");
        this.runtimeConfiguration =
                new RuntimeTuningContextLoggingAdapter(
                        delegate.runtimeTuning(), this.contextScopeRunner);
        this.runtimeMonitoring =
                new IndexRuntimeMonitoringContextLoggingAdapter(
                        delegate.runtimeMonitoring(), this.contextScopeRunner);
        this.maintenance = new SegmentIndexMaintenanceContextLoggingAdapter(
                delegate.maintenance(), this.contextScopeRunner);
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
    public EntryIterator<K, V> openSegmentIterator(
            final SegmentWindow segmentWindows) {
        return contextScopeRunner
                .supply(() -> delegate.openSegmentIterator(segmentWindows));
    }

    @Override
    public void completeStartup() {
        contextScopeRunner.run(delegate::completeStartup);
    }

    @Override
    public RuntimeTuning runtimeTuning() {
        return runtimeConfiguration;
    }

    @Override
    public IndexRuntimeMonitoring runtimeMonitoring() {
        return runtimeMonitoring;
    }

    @Override
    public SegmentIndexMaintenance maintenance() {
        return maintenance;
    }

    @Override
    protected void doClose() {
        contextScopeRunner.run(delegate::close);
    }
}
