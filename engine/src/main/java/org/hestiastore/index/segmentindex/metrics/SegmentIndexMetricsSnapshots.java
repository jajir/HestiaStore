package org.hestiastore.index.segmentindex.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstorecache.LruChunkStoreCache;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStats;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStats;
import org.hestiastore.index.segmentindex.core.split.SplitStats;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.wal.WalStats;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Compatibility factory for metrics snapshots.
 * <p>
 * Prefer {@link RuntimeMetricsCollector#builder()} for new code.
 */
final class SegmentIndexMetricsSnapshots {

    private SegmentIndexMetricsSnapshots() {
    }

    /**
     * Creates a supplier that snapshots the current runtime metrics view on
     * demand.
     *
     * @param conf index configuration
     * @param keyToSegmentMap runtime route map
     * @param segmentRegistry runtime segment registry
     * @param splitStatsSupplier split runtime stats supplier
     * @param executorRegistry executor runtime registry
     * @param runtimeTuningState mutable runtime tuning state
     * @param walStatsSupplier WAL stats supplier
     * @param indexOperationStatsSupplier point-operation stats supplier
     * @param maintenanceStatsSupplier maintenance stats supplier
     * @param compactRequestHighWaterMark compact request high-water mark
     * @param flushRequestHighWaterMark flush request high-water mark
     * @param lastAppliedWalLsn last applied WAL LSN
     * @param stateSupplier runtime state supplier
     * @param <K> key type
     * @param <V> value type
     * @return supplier producing immutable metrics snapshots
     */
    static <K, V> Supplier<SegmentIndexMetricsSnapshot> create(
            final EffectiveIndexConfiguration<K, V> conf,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final Supplier<SplitStats> splitStatsSupplier,
            final ExecutorRegistry executorRegistry,
            final RuntimeTuningState runtimeTuningState,
            final Supplier<WalStats> walStatsSupplier,
            final Supplier<IndexOperationStats> indexOperationStatsSupplier,
            final Supplier<MaintenanceStats> maintenanceStatsSupplier,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn,
            final Supplier<SegmentIndexState> stateSupplier) {
        final RuntimeMetricsCollector runtimeMetricsCollector =
                RuntimeMetricsCollector.<K, V>builder()
                .withConf(conf)
                .withKeyToSegmentMap(keyToSegmentMap)
                .withSegmentRegistry(segmentRegistry)
                .withSplitStatsSupplier(splitStatsSupplier)
                .withExecutorRegistry(executorRegistry)
                .withRuntimeTuningState(runtimeTuningState)
                .withChunkStoreCache(new LruChunkStoreCache<>(0))
                .withWalStatsSupplier(walStatsSupplier)
                .withIndexOperationStatsSupplier(indexOperationStatsSupplier)
                .withMaintenanceStatsSupplier(maintenanceStatsSupplier)
                .withCompactRequestHighWaterMark(compactRequestHighWaterMark)
                .withFlushRequestHighWaterMark(flushRequestHighWaterMark)
                .withLastAppliedWalLsn(lastAppliedWalLsn)
                .withStateSupplier(stateSupplier)
                .build();
        return runtimeMetricsCollector::metricsSnapshot;
    }
}
