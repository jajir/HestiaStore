package org.hestiastore.index.segmentindex.core.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.maintenance.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.split.SplitMetricsSnapshot;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Public metrics snapshot factory boundary exposed outside
 * {@code core.observability}.
 */
public final class SegmentIndexMetricsSnapshots {

    private SegmentIndexMetricsSnapshots() {
    }

    /**
     * Creates a supplier that snapshots the current runtime metrics view on
     * demand.
     *
     * @param conf index configuration
     * @param keyToSegmentMap runtime route map
     * @param segmentRegistry runtime segment registry
     * @param splitSnapshotSupplier split runtime snapshot supplier
     * @param executorRegistry executor runtime registry
     * @param runtimeTuningState mutable runtime tuning state
     * @param walRuntime WAL runtime
     * @param stats live operation statistics
     * @param compactRequestHighWaterMark compact request high-water mark
     * @param flushRequestHighWaterMark flush request high-water mark
     * @param lastAppliedWalLsn last applied WAL LSN
     * @param stateSupplier runtime state supplier
     * @param <K> key type
     * @param <V> value type
     * @return supplier producing immutable metrics snapshots
     */
    public static <K, V> Supplier<SegmentIndexMetricsSnapshot> create(
            final IndexConfiguration<K, V> conf,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final Supplier<SplitMetricsSnapshot> splitSnapshotSupplier,
            final IndexExecutorRegistry executorRegistry,
            final RuntimeTuningState runtimeTuningState,
            final WalRuntime<K, V> walRuntime, final Stats stats,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn,
            final Supplier<SegmentIndexState> stateSupplier) {
        return SegmentIndexMetricsCollector.create(conf, keyToSegmentMap,
                segmentRegistry, splitSnapshotSupplier, executorRegistry,
                runtimeTuningState, walRuntime, stats,
                compactRequestHighWaterMark, flushRequestHighWaterMark,
                lastAppliedWalLsn, stateSupplier)::metricsSnapshot;
    }
}
