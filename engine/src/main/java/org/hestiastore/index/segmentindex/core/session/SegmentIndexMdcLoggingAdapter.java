package org.hestiastore.index.segmentindex.core.session;

import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.MemoryEstimateReport;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningPatch;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningResult;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningSnapshot;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningValidation;
import org.hestiastore.index.segmentindex.monitoring.SegmentIndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexRuntimeSnapshot;

/**
 * Adapter that wraps an internal index and ensures that the
 * {@code index.name} MDC key is present for every operation executed against
 * the wrapped index.
 *
 * @param <K> type of keys stored in the index
 * @param <V> type of values stored in the index
 */
final class SegmentIndexMdcLoggingAdapter<K, V>
        extends AbstractCloseableResource
        implements SegmentIndex<K, V> {
    private final SegmentIndex<K, V> delegate;
    private final String indexName;
    private final RuntimeTuning runtimeConfiguration;
    private final SegmentIndexRuntimeMonitoring runtimeMonitoring;
    private final SegmentIndexMaintenance maintenance;

    SegmentIndexMdcLoggingAdapter(
            final SegmentIndex<K, V> delegate,
            final String indexName) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.indexName = Vldtn.requireNotBlank(indexName, "indexName");
        this.runtimeConfiguration = newRuntimeTuning(delegate.runtimeTuning());
        this.runtimeMonitoring = newRuntimeMonitoring(
                delegate.runtimeMonitoring());
        this.maintenance = newMaintenance(delegate.maintenance());
    }

    @Override
    public void put(final K key, final V value) {
        try (IndexMdcScope ignored = openScope()) {
            delegate.put(key, value);
        }
    }

    @Override
    public void put(final Entry<K, V> entry) {
        try (IndexMdcScope ignored = openScope()) {
            delegate.put(entry);
        }
    }

    @Override
    public V get(final K key) {
        try (IndexMdcScope ignored = openScope()) {
            return delegate.get(key);
        }
    }

    @Override
    public void delete(final K key) {
        try (IndexMdcScope ignored = openScope()) {
            delegate.delete(key);
        }
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows) {
        try (IndexMdcScope ignored = openScope()) {
            return delegate.getStream(segmentWindows);
        }
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows,
            final SegmentIteratorIsolation isolation) {
        try (IndexMdcScope ignored = openScope()) {
            return delegate.getStream(segmentWindows, isolation);
        }
    }

    @Override
    public Stream<Entry<K, V>> getStream() {
        try (IndexMdcScope ignored = openScope()) {
            return delegate.getStream();
        }
    }

    @Override
    public Stream<Entry<K, V>> getStream(
            final SegmentIteratorIsolation isolation) {
        try (IndexMdcScope ignored = openScope()) {
            return delegate.getStream(isolation);
        }
    }

    @Override
    public RuntimeTuning runtimeTuning() {
        return runtimeConfiguration;
    }

    @Override
    public SegmentIndexRuntimeMonitoring runtimeMonitoring() {
        return runtimeMonitoring;
    }

    @Override
    public MemoryEstimateReport startupMemoryEstimate() {
        return delegate.startupMemoryEstimate();
    }

    @Override
    public SegmentIndexMaintenance maintenance() {
        return maintenance;
    }

    @Override
    protected void doClose() {
        try (IndexMdcScope ignored = openScope()) {
            delegate.close();
        }
    }

    private RuntimeTuning newRuntimeTuning(final RuntimeTuning tuning) {
        final RuntimeTuning nonNullTuning = Vldtn.requireNonNull(tuning,
                "tuning");
        return new RuntimeTuning() {

            @Override
            public RuntimeTuningSnapshot current() {
                try (IndexMdcScope ignored = openScope()) {
                    return nonNullTuning.current();
                }
            }

            @Override
            public RuntimeTuningSnapshot original() {
                try (IndexMdcScope ignored = openScope()) {
                    return nonNullTuning.original();
                }
            }

            @Override
            public RuntimeTuningValidation validate(
                    final RuntimeTuningPatch patch) {
                try (IndexMdcScope ignored = openScope()) {
                    return nonNullTuning.validate(patch);
                }
            }

            @Override
            public RuntimeTuningResult apply(final RuntimeTuningPatch patch) {
                try (IndexMdcScope ignored = openScope()) {
                    return nonNullTuning.apply(patch);
                }
            }

            @Override
            public RuntimeTuningSnapshot persistCurrent() {
                try (IndexMdcScope ignored = openScope()) {
                    return nonNullTuning.persistCurrent();
                }
            }
        };
    }

    private SegmentIndexRuntimeMonitoring newRuntimeMonitoring(
            final SegmentIndexRuntimeMonitoring monitoring) {
        final SegmentIndexRuntimeMonitoring nonNullMonitoring = Vldtn.requireNonNull(
                monitoring, "monitoring");
        return new SegmentIndexRuntimeMonitoring() {

            @Override
            public SegmentIndexRuntimeSnapshot snapshot() {
                try (IndexMdcScope ignored = openScope()) {
                    return nonNullMonitoring.snapshot();
                }
            }
        };
    }

    private SegmentIndexMaintenance newMaintenance(
            final SegmentIndexMaintenance maintenanceDelegate) {
        final SegmentIndexMaintenance nonNullMaintenance = Vldtn
                .requireNonNull(maintenanceDelegate, "maintenanceDelegate");
        return new SegmentIndexMaintenance() {

            @Override
            public void compact() {
                try (IndexMdcScope ignored = openScope()) {
                    nonNullMaintenance.compact();
                }
            }

            @Override
            public void compactAndWait() {
                try (IndexMdcScope ignored = openScope()) {
                    nonNullMaintenance.compactAndWait();
                }
            }

            @Override
            public void flush() {
                try (IndexMdcScope ignored = openScope()) {
                    nonNullMaintenance.flush();
                }
            }

            @Override
            public void flushAndWait() {
                try (IndexMdcScope ignored = openScope()) {
                    nonNullMaintenance.flushAndWait();
                }
            }

            @Override
            public void checkAndRepairConsistency() {
                try (IndexMdcScope ignored = openScope()) {
                    nonNullMaintenance.checkAndRepairConsistency();
                }
            }
        };
    }

    private IndexMdcScope openScope() {
        return new IndexMdcScope(indexName);
    }
}
