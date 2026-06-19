package org.hestiastore.index.segmentindex.runtimemonitoring;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateView;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalMonitoringView;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Builder for {@link IndexRuntimeMonitoring} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexRuntimeMonitoringBuilder<K, V> {

    private EffectiveIndexConfiguration<K, V> conf;
    private KeyToSegmentMap<K> keyToSegmentMap;
    private SegmentRegistry<K, V> segmentRegistry;
    private SplitService<K, V> splitService;
    private ExecutorRegistry executorRegistry;
    private RuntimeTuningState runtimeTuningState;
    private ChunkStoreCache<K, V> chunkStoreCache;
    private WalMonitoringView walMonitoringView;
    private IndexOperationStatsRecorder indexOperationStatsRecorder;
    private MaintenanceStatsRecorder maintenanceStatsRecorder;
    private AtomicLong compactRequestHighWaterMark;
    private AtomicLong flushRequestHighWaterMark;
    private AtomicLong lastAppliedWalLsn;
    private SegmentIndexStateView stateView;

    IndexRuntimeMonitoringBuilder() {
    }

    /**
     * Creates a builder for runtime monitoring views.
     *
     * @param <K> key type
     * @param <V> value type
     * @return runtime monitoring builder
     */
    public static <K, V> IndexRuntimeMonitoringBuilder<K, V> builder() {
        return new IndexRuntimeMonitoringBuilder<>();
    }

    /**
     * Sets the effective index configuration.
     *
     * @param conf effective index configuration
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withConf(
            final EffectiveIndexConfiguration<K, V> conf) {
        this.conf = conf;
        return this;
    }

    /**
     * Sets the key-to-segment routing map.
     *
     * @param keyToSegmentMap key-to-segment map
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withKeyToSegmentMap(
            final KeyToSegmentMap<K> keyToSegmentMap) {
        this.keyToSegmentMap = keyToSegmentMap;
        return this;
    }

    /**
     * Sets the segment registry.
     *
     * @param segmentRegistry segment registry
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withSegmentRegistry(
            final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = segmentRegistry;
        return this;
    }

    /**
     * Sets the split runtime service used as the stats source.
     *
     * @param splitService split runtime service
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withSplitService(
            final SplitService<K, V> splitService) {
        this.splitService = splitService;
        return this;
    }

    /**
     * Sets the executor registry.
     *
     * @param executorRegistry executor registry
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withExecutorRegistry(
            final ExecutorRegistry executorRegistry) {
        this.executorRegistry = executorRegistry;
        return this;
    }

    /**
     * Sets the runtime tuning state.
     *
     * @param runtimeTuningState runtime tuning state
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withRuntimeTuningState(
            final RuntimeTuningState runtimeTuningState) {
        this.runtimeTuningState = runtimeTuningState;
        return this;
    }

    /**
     * Sets the chunk store cache.
     *
     * @param chunkStoreCache chunk store cache
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withChunkStoreCache(
            final ChunkStoreCache<K, V> chunkStoreCache) {
        this.chunkStoreCache = chunkStoreCache;
        return this;
    }

    /**
     * Sets the active WAL runtime as the WAL monitoring source.
     *
     * @param walRuntime active WAL runtime
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withWalRuntime(
            final WalRuntime<K, V> walRuntime) {
        this.walMonitoringView = Vldtn.requireNonNull(walRuntime,
                "walRuntime");
        return this;
    }

    /**
     * Sets the WAL monitoring view.
     *
     * @param walMonitoringView WAL monitoring view
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withWalMonitoringView(
            final WalMonitoringView walMonitoringView) {
        this.walMonitoringView = walMonitoringView;
        return this;
    }

    /**
     * Sets the point-operation stats recorder.
     *
     * @param indexOperationStatsRecorder point-operation stats recorder
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withIndexOperationStatsRecorder(
            final IndexOperationStatsRecorder indexOperationStatsRecorder) {
        this.indexOperationStatsRecorder = indexOperationStatsRecorder;
        return this;
    }

    /**
     * Sets the maintenance stats recorder.
     *
     * @param maintenanceStatsRecorder maintenance stats recorder
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withMaintenanceStatsRecorder(
            final MaintenanceStatsRecorder maintenanceStatsRecorder) {
        this.maintenanceStatsRecorder = maintenanceStatsRecorder;
        return this;
    }

    /**
     * Sets the compact request high-water mark.
     *
     * @param compactRequestHighWaterMark compact request high-water mark
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withCompactRequestHighWaterMark(
            final AtomicLong compactRequestHighWaterMark) {
        this.compactRequestHighWaterMark = compactRequestHighWaterMark;
        return this;
    }

    /**
     * Sets the flush request high-water mark.
     *
     * @param flushRequestHighWaterMark flush request high-water mark
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withFlushRequestHighWaterMark(
            final AtomicLong flushRequestHighWaterMark) {
        this.flushRequestHighWaterMark = flushRequestHighWaterMark;
        return this;
    }

    /**
     * Sets the last applied WAL LSN holder.
     *
     * @param lastAppliedWalLsn last applied WAL LSN holder
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withLastAppliedWalLsn(
            final AtomicLong lastAppliedWalLsn) {
        this.lastAppliedWalLsn = lastAppliedWalLsn;
        return this;
    }

    /**
     * Sets the runtime state view.
     *
     * @param stateView runtime state view
     * @return this builder
     */
    public IndexRuntimeMonitoringBuilder<K, V> withStateView(
            final SegmentIndexStateView stateView) {
        this.stateView = stateView;
        return this;
    }

    /**
     * Builds the runtime monitoring view.
     *
     * @return runtime monitoring view
     */
    public IndexRuntimeMonitoring build() {
        return IndexRuntimeSnapshotCollector.create(
                Vldtn.requireNonNull(conf, "conf"),
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                Vldtn.requireNonNull(splitService, "splitService"),
                Vldtn.requireNonNull(executorRegistry, "executorRegistry"),
                Vldtn.requireNonNull(runtimeTuningState, "runtimeTuningState"),
                Vldtn.requireNonNull(chunkStoreCache, "chunkStoreCache"),
                Vldtn.requireNonNull(walMonitoringView, "walMonitoringView"),
                Vldtn.requireNonNull(indexOperationStatsRecorder,
                        "indexOperationStatsRecorder"),
                Vldtn.requireNonNull(maintenanceStatsRecorder,
                        "maintenanceStatsRecorder"),
                Vldtn.requireNonNull(compactRequestHighWaterMark,
                        "compactRequestHighWaterMark"),
                Vldtn.requireNonNull(flushRequestHighWaterMark,
                        "flushRequestHighWaterMark"),
                Vldtn.requireNonNull(lastAppliedWalLsn, "lastAppliedWalLsn"),
                Vldtn.requireNonNull(stateView, "stateView"));
    }
}
