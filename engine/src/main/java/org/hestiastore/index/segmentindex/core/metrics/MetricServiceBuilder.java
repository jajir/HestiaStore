package org.hestiastore.index.segmentindex.core.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.split.SplitMetricsSnapshot;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Builder for {@link MetricService} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class MetricServiceBuilder<K, V> {

    private IndexConfiguration<K, V> conf;
    private KeyToSegmentMap<K> keyToSegmentMap;
    private SegmentRegistry<K, V> segmentRegistry;
    private Supplier<SplitMetricsSnapshot> splitSnapshotSupplier;
    private ExecutorRegistry executorRegistry;
    private RuntimeTuningState runtimeTuningState;
    private WalRuntime<K, V> walRuntime;
    private Stats stats;
    private AtomicLong compactRequestHighWaterMark;
    private AtomicLong flushRequestHighWaterMark;
    private AtomicLong lastAppliedWalLsn;
    private Supplier<SegmentIndexState> stateSupplier;

    MetricServiceBuilder() {
    }

    public MetricServiceBuilder<K, V> withConf(
            final IndexConfiguration<K, V> conf) {
        this.conf = conf;
        return this;
    }

    public MetricServiceBuilder<K, V> withKeyToSegmentMap(
            final KeyToSegmentMap<K> keyToSegmentMap) {
        this.keyToSegmentMap = keyToSegmentMap;
        return this;
    }

    public MetricServiceBuilder<K, V> withSegmentRegistry(
            final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = segmentRegistry;
        return this;
    }

    public MetricServiceBuilder<K, V> withSplitSnapshotSupplier(
            final Supplier<SplitMetricsSnapshot> splitSnapshotSupplier) {
        this.splitSnapshotSupplier = splitSnapshotSupplier;
        return this;
    }

    public MetricServiceBuilder<K, V> withExecutorRegistry(
            final ExecutorRegistry executorRegistry) {
        this.executorRegistry = executorRegistry;
        return this;
    }

    public MetricServiceBuilder<K, V> withRuntimeTuningState(
            final RuntimeTuningState runtimeTuningState) {
        this.runtimeTuningState = runtimeTuningState;
        return this;
    }

    public MetricServiceBuilder<K, V> withWalRuntime(
            final WalRuntime<K, V> walRuntime) {
        this.walRuntime = walRuntime;
        return this;
    }

    public MetricServiceBuilder<K, V> withStats(final Stats stats) {
        this.stats = stats;
        return this;
    }

    public MetricServiceBuilder<K, V> withCompactRequestHighWaterMark(
            final AtomicLong compactRequestHighWaterMark) {
        this.compactRequestHighWaterMark = compactRequestHighWaterMark;
        return this;
    }

    public MetricServiceBuilder<K, V> withFlushRequestHighWaterMark(
            final AtomicLong flushRequestHighWaterMark) {
        this.flushRequestHighWaterMark = flushRequestHighWaterMark;
        return this;
    }

    public MetricServiceBuilder<K, V> withLastAppliedWalLsn(
            final AtomicLong lastAppliedWalLsn) {
        this.lastAppliedWalLsn = lastAppliedWalLsn;
        return this;
    }

    public MetricServiceBuilder<K, V> withStateSupplier(
            final Supplier<SegmentIndexState> stateSupplier) {
        this.stateSupplier = stateSupplier;
        return this;
    }

    /**
     * Builds the metric service.
     *
     * @return metric service
     */
    public MetricService build() {
        final SegmentIndexMetricsCollector<K, V> collector =
                SegmentIndexMetricsCollector.create(
                        Vldtn.requireNonNull(conf, "conf"),
                        Vldtn.requireNonNull(keyToSegmentMap,
                                "keyToSegmentMap"),
                        Vldtn.requireNonNull(segmentRegistry,
                                "segmentRegistry"),
                        Vldtn.requireNonNull(splitSnapshotSupplier,
                                "splitSnapshotSupplier"),
                        Vldtn.requireNonNull(executorRegistry,
                                "executorRegistry"),
                        Vldtn.requireNonNull(runtimeTuningState,
                                "runtimeTuningState"),
                        Vldtn.requireNonNull(walRuntime, "walRuntime"),
                        Vldtn.requireNonNull(stats, "stats"),
                        Vldtn.requireNonNull(compactRequestHighWaterMark,
                                "compactRequestHighWaterMark"),
                        Vldtn.requireNonNull(flushRequestHighWaterMark,
                                "flushRequestHighWaterMark"),
                        Vldtn.requireNonNull(lastAppliedWalLsn,
                                "lastAppliedWalLsn"),
                        Vldtn.requireNonNull(stateSupplier,
                                "stateSupplier"));
        return new MetricServiceImpl(collector::metricsSnapshot);
    }
}
