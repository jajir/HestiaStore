package org.hestiastore.index.segmentindex.core.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.split.SplitMetricsSnapshot;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Compatibility factory for metrics snapshots.
 * <p>
 * Prefer {@link MetricService#builder()} for new code.
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
            final ExecutorRegistry executorRegistry,
            final RuntimeTuningState runtimeTuningState,
            final WalRuntime<K, V> walRuntime, final Stats stats,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn,
            final Supplier<SegmentIndexState> stateSupplier) {
        final MetricService metricService = MetricService.<K, V>builder()
                .withConf(conf)
                .withKeyToSegmentMap(keyToSegmentMap)
                .withSegmentRegistry(segmentRegistry)
                .withSplitSnapshotSupplier(splitSnapshotSupplier)
                .withExecutorRegistry(executorRegistry)
                .withRuntimeTuningState(runtimeTuningState)
                .withWalRuntime(walRuntime)
                .withStats(stats)
                .withCompactRequestHighWaterMark(compactRequestHighWaterMark)
                .withFlushRequestHighWaterMark(flushRequestHighWaterMark)
                .withLastAppliedWalLsn(lastAppliedWalLsn)
                .withStateSupplier(stateSupplier)
                .build();
        return metricService::metricsSnapshot;
    }
}
