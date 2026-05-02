package org.hestiastore.index.segmentindex.core.session;

import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.IndexMdcScopeRunner;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfiguration;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfigurationContextLoggingAdapter;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoringContextLoggingAdapter;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenanceContextLoggingAdapter;

/**
 * Adapter that wraps a {@link SegmentIndex} and ensures that the
 * {@code index.name} MDC key is present for every operation executed against
 * the wrapped index.
 *
 * @param <K> type of keys stored in the index
 * @param <V> type of values stored in the index
 */
public final class IndexContextLoggingAdapter<K, V>
        extends AbstractCloseableResource implements SegmentIndex<K, V> {
    private final SegmentIndex<K, V> delegate;
    private final IndexMdcScopeRunner contextScopeRunner;
    private final RuntimeConfiguration runtimeConfiguration;
    private final IndexRuntimeMonitoring runtimeMonitoring;
    private final SegmentIndexMaintenance maintenance;

    public IndexContextLoggingAdapter(final IndexConfiguration<K, V> indexConf,
            final SegmentIndex<K, V> index) {
        this.delegate = Vldtn.requireNonNull(index, "index");
        final IndexConfiguration<K, V> configuration = Vldtn
                .requireNonNull(indexConf, "indexConfiguration");
        this.contextScopeRunner = new IndexMdcScopeRunner(
                configuration.identity().name());
        this.runtimeConfiguration = new RuntimeConfigurationContextLoggingAdapter(
                delegate.runtimeConfiguration(), contextScopeRunner);
        this.runtimeMonitoring = new IndexRuntimeMonitoringContextLoggingAdapter(
                delegate.runtimeMonitoring(), contextScopeRunner);
        this.maintenance = new SegmentIndexMaintenanceContextLoggingAdapter(
                delegate.maintenance(), contextScopeRunner);
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
    public RuntimeConfiguration runtimeConfiguration() {
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
