package org.hestiastore.index.segmentindex.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStats;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStats;
import org.hestiastore.index.segmentindex.core.split.SplitStats;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Builder for {@link RuntimeMetricsCollector} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class RuntimeMetricsCollectorBuilder<K, V> {

    private EffectiveIndexConfiguration<K, V> conf;
    private KeyToSegmentMap<K> keyToSegmentMap;
    private SegmentRegistry<K, V> segmentRegistry;
    private Supplier<SplitStats> splitStatsSupplier;
    private ExecutorRegistry executorRegistry;
    private RuntimeTuningState runtimeTuningState;
    private ChunkStoreCache<K, V> chunkStoreCache;
    private WalRuntime<K, V> walRuntime;
    private Supplier<IndexOperationStats> indexOperationStatsSupplier;
    private Supplier<MaintenanceStats> maintenanceStatsSupplier;
    private AtomicLong compactRequestHighWaterMark;
    private AtomicLong flushRequestHighWaterMark;
    private AtomicLong lastAppliedWalLsn;
    private Supplier<SegmentIndexState> stateSupplier;

    RuntimeMetricsCollectorBuilder() {
    }

    public RuntimeMetricsCollectorBuilder<K, V> withConf(
            final EffectiveIndexConfiguration<K, V> conf) {
        this.conf = conf;
        return this;
    }

    public RuntimeMetricsCollectorBuilder<K, V> withKeyToSegmentMap(
            final KeyToSegmentMap<K> keyToSegmentMap) {
        this.keyToSegmentMap = keyToSegmentMap;
        return this;
    }

    public RuntimeMetricsCollectorBuilder<K, V> withSegmentRegistry(
            final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = segmentRegistry;
        return this;
    }

    public RuntimeMetricsCollectorBuilder<K, V> withSplitStatsSupplier(
            final Supplier<SplitStats> splitStatsSupplier) {
        this.splitStatsSupplier = splitStatsSupplier;
        return this;
    }

    public RuntimeMetricsCollectorBuilder<K, V> withExecutorRegistry(
            final ExecutorRegistry executorRegistry) {
        this.executorRegistry = executorRegistry;
        return this;
    }

    public RuntimeMetricsCollectorBuilder<K, V> withRuntimeTuningState(
            final RuntimeTuningState runtimeTuningState) {
        this.runtimeTuningState = runtimeTuningState;
        return this;
    }

    public RuntimeMetricsCollectorBuilder<K, V> withChunkStoreCache(
            final ChunkStoreCache<K, V> chunkStoreCache) {
        this.chunkStoreCache = chunkStoreCache;
        return this;
    }

    public RuntimeMetricsCollectorBuilder<K, V> withWalRuntime(
            final WalRuntime<K, V> walRuntime) {
        this.walRuntime = walRuntime;
        return this;
    }

    public RuntimeMetricsCollectorBuilder<K, V> withIndexOperationStatsSupplier(
            final Supplier<IndexOperationStats> indexOperationStatsSupplier) {
        this.indexOperationStatsSupplier = indexOperationStatsSupplier;
        return this;
    }

    public RuntimeMetricsCollectorBuilder<K, V> withMaintenanceStatsSupplier(
            final Supplier<MaintenanceStats> maintenanceStatsSupplier) {
        this.maintenanceStatsSupplier = maintenanceStatsSupplier;
        return this;
    }

    public RuntimeMetricsCollectorBuilder<K, V> withCompactRequestHighWaterMark(
            final AtomicLong compactRequestHighWaterMark) {
        this.compactRequestHighWaterMark = compactRequestHighWaterMark;
        return this;
    }

    public RuntimeMetricsCollectorBuilder<K, V> withFlushRequestHighWaterMark(
            final AtomicLong flushRequestHighWaterMark) {
        this.flushRequestHighWaterMark = flushRequestHighWaterMark;
        return this;
    }

    public RuntimeMetricsCollectorBuilder<K, V> withLastAppliedWalLsn(
            final AtomicLong lastAppliedWalLsn) {
        this.lastAppliedWalLsn = lastAppliedWalLsn;
        return this;
    }

    public RuntimeMetricsCollectorBuilder<K, V> withStateSupplier(
            final Supplier<SegmentIndexState> stateSupplier) {
        this.stateSupplier = stateSupplier;
        return this;
    }

    /**
     * Builds the runtime metrics collector.
     *
     * @return runtime metrics collector
     */
    public RuntimeMetricsCollector build() {
        final SegmentIndexMetricsCollector<K, V> collector =
                SegmentIndexMetricsCollector.create(
                        Vldtn.requireNonNull(conf, "conf"),
                        Vldtn.requireNonNull(keyToSegmentMap,
                                "keyToSegmentMap"),
                        Vldtn.requireNonNull(segmentRegistry,
                                "segmentRegistry"),
                        Vldtn.requireNonNull(splitStatsSupplier,
                                "splitStatsSupplier"),
                        Vldtn.requireNonNull(executorRegistry,
                                "executorRegistry"),
                        Vldtn.requireNonNull(runtimeTuningState,
                                "runtimeTuningState"),
                        Vldtn.requireNonNull(chunkStoreCache,
                                "chunkStoreCache"),
                        Vldtn.requireNonNull(walRuntime, "walRuntime"),
                        Vldtn.requireNonNull(indexOperationStatsSupplier,
                                "indexOperationStatsSupplier"),
                        Vldtn.requireNonNull(maintenanceStatsSupplier,
                                "maintenanceStatsSupplier"),
                        Vldtn.requireNonNull(compactRequestHighWaterMark,
                                "compactRequestHighWaterMark"),
                        Vldtn.requireNonNull(flushRequestHighWaterMark,
                                "flushRequestHighWaterMark"),
                        Vldtn.requireNonNull(lastAppliedWalLsn,
                                "lastAppliedWalLsn"),
                        Vldtn.requireNonNull(stateSupplier,
                                "stateSupplier"));
        return new RuntimeMetricsCollectorImpl(collector::metricsSnapshot);
    }
}
