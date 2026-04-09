package org.hestiastore.index.segmentindex.core;

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexConfigurationManagement;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.control.IndexRuntimeView;
import org.hestiastore.index.control.model.ConfigurationSnapshot;
import org.hestiastore.index.control.model.IndexRuntimeSnapshot;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimePatchValidation;
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
class IndexContextLoggingAdapter<K, V> extends AbstractCloseableResource
        implements SegmentIndex<K, V> {

    private static final String DELEGATE = "delegate";

    private final String indexName;
    private final SegmentIndex<K, V> index;

    IndexContextLoggingAdapter(final IndexConfiguration<K, V> indexConf,
            final SegmentIndex<K, V> index) {
        final IndexConfiguration<K, V> configuration = Vldtn
                .requireNonNull(indexConf, "indexConfiguration");
        this.indexName = Vldtn.requireNotBlank(configuration.getIndexName(),
                "indexName");
        this.index = Vldtn.requireNonNull(index, "index");
    }

    @Override
    public void put(final K key, final V value) {
        runWithContext(() -> index.put(key, value));
    }

    @Override
    public void put(final Entry<K, V> entry) {
        runWithContext(() -> index.put(entry));
    }

    @Override
    public V get(final K key) {
        return supplyWithContext(() -> index.get(key));
    }

    @Override
    public void delete(final K key) {
        runWithContext(() -> index.delete(key));
    }

    @Override
    public void compact() {
        runWithContext(index::compact);
    }

    @Override
    public void compactAndWait() {
        runWithContext(index::compactAndWait);
    }

    @Override
    public void flush() {
        runWithContext(index::flush);
    }

    @Override
    public void flushAndWait() {
        runWithContext(index::flushAndWait);
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows) {
        return supplyWithContext(() -> index.getStream(segmentWindows));
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows,
            final SegmentIteratorIsolation isolation) {
        return supplyWithContext(
                () -> index.getStream(segmentWindows, isolation));
    }

    @Override
    public Stream<Entry<K, V>> getStream() {
        return supplyWithContext(() -> index.getStream());
    }

    @Override
    public Stream<Entry<K, V>> getStream(
            final SegmentIteratorIsolation isolation) {
        return supplyWithContext(() -> index.getStream(isolation));
    }

    @Override
    public void checkAndRepairConsistency() {
        runWithContext(index::checkAndRepairConsistency);
    }

    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return supplyWithContext(index::getConfiguration);
    }

    @Override
    public SegmentIndexState getState() {
        return supplyWithContext(index::getState);
    }

    @Override
    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return supplyWithContext(index::metricsSnapshot);
    }

    @Override
    public IndexControlPlane controlPlane() {
        return supplyWithContext(
                () -> new ContextLoggingIndexControlPlane(index.controlPlane()));
    }

    @Override
    protected void doClose() {
        runWithContext(index::close);
    }

    private void runWithContext(final Runnable action) {
        Vldtn.requireNonNull(action, "action");
        try (IndexNameMdcScope ignored = IndexNameMdcScope.open(indexName)) {
            action.run();
        }
    }

    private <T> T supplyWithContext(final Supplier<T> action) {
        Vldtn.requireNonNull(action, "action");
        try (IndexNameMdcScope ignored = IndexNameMdcScope.open(indexName)) {
            return action.get();
        }
    }

    private final class ContextLoggingIndexControlPlane
            implements IndexControlPlane {

        private final IndexControlPlane delegate;

        private ContextLoggingIndexControlPlane(
                final IndexControlPlane delegate) {
            this.delegate = Vldtn.requireNonNull(delegate, DELEGATE);
        }

        @Override
        public String indexName() {
            return supplyWithContext(delegate::indexName);
        }

        @Override
        public IndexRuntimeView runtime() {
            return supplyWithContext(
                    () -> new ContextLoggingIndexRuntimeView(delegate.runtime()));
        }

        @Override
        public IndexConfigurationManagement configuration() {
            return supplyWithContext(
                    () -> new ContextLoggingIndexConfigurationManagement(
                            delegate.configuration()));
        }
    }

    private final class ContextLoggingIndexRuntimeView
            implements IndexRuntimeView {

        private final IndexRuntimeView delegate;

        private ContextLoggingIndexRuntimeView(final IndexRuntimeView delegate) {
            this.delegate = Vldtn.requireNonNull(delegate, DELEGATE);
        }

        @Override
        public IndexRuntimeSnapshot snapshot() {
            return supplyWithContext(delegate::snapshot);
        }
    }

    private final class ContextLoggingIndexConfigurationManagement
            implements IndexConfigurationManagement {

        private final IndexConfigurationManagement delegate;

        private ContextLoggingIndexConfigurationManagement(
                final IndexConfigurationManagement delegate) {
            this.delegate = Vldtn.requireNonNull(delegate, DELEGATE);
        }

        @Override
        public ConfigurationSnapshot getConfigurationActual() {
            return supplyWithContext(delegate::getConfigurationActual);
        }

        @Override
        public ConfigurationSnapshot getConfigurationOriginal() {
            return supplyWithContext(delegate::getConfigurationOriginal);
        }

        @Override
        public RuntimePatchValidation validate(final RuntimeConfigPatch patch) {
            return supplyWithContext(() -> delegate.validate(patch));
        }

        @Override
        public RuntimePatchResult apply(final RuntimeConfigPatch patch) {
            return supplyWithContext(() -> delegate.apply(patch));
        }
    }
}
