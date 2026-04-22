package org.hestiastore.index.segmentindex.core.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.maintenance.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.routing.BackgroundSplitCoordinator;
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

    public static <K, V> Supplier<SegmentIndexMetricsSnapshot> create(
            final IndexConfiguration<K, V> conf,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final IndexExecutorRegistry executorRegistry,
            final RuntimeTuningState runtimeTuningState,
            final WalRuntime<K, V> walRuntime, final Stats stats,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn,
            final Supplier<SegmentIndexState> stateSupplier) {
        return SegmentIndexMetricsCollector.create(conf, keyToSegmentMap,
                segmentRegistry, backgroundSplitCoordinator, executorRegistry,
                runtimeTuningState, walRuntime, stats,
                compactRequestHighWaterMark, flushRequestHighWaterMark,
                lastAppliedWalLsn, stateSupplier)::metricsSnapshot;
    }
}
