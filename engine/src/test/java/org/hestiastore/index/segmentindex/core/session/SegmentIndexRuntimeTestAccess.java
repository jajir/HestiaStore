package org.hestiastore.index.segmentindex.core.session;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.mockito.Mockito.mock;

import java.util.function.Supplier;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.split.SplitStatsRecorder;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Test-only bridge exposing package-private runtime internals without keeping
 * them public in production code.
 */
public final class SegmentIndexRuntimeTestAccess {

    private SegmentIndexRuntimeTestAccess() {
    }

    public static <K, V> Object openRuntime(
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final ExecutorRegistry executorRegistry) {
        return SegmentIndexRuntime.create(directoryFacade, keyTypeDescriptor,
                valueTypeDescriptor, effective(conf),
                executorRegistry, new IndexOperationStatsRecorder(),
                new MaintenanceStatsRecorder(), new SplitStatsRecorder(),
                () -> SegmentIndexState.READY,
                failure -> {
                });
    }

    public static <K> KeyToSegmentMap<K> keyToSegmentMap(
            final Object runtime) {
        return keyToSegmentMap(castRuntime(runtime));
    }

    public static <K> KeyToSegmentMap<K> keyToSegmentMap(
            final SegmentIndexRuntime<K, ?> runtime) {
        return runtime.storage().keyToSegmentMap();
    }

    public static <K, V> SegmentRegistry<K, V> segmentRegistry(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.storage().segmentRegistry();
    }

    public static <K, V> WalRuntime<K, V> walRuntime(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.walRuntime();
    }

    public static <K, V> SegmentTopologyRuntimeAccess<K, V> topologyRuntime(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.topologyRuntime();
    }

    public static <K, V> SegmentTopologyRuntimeAccess<K, V> topologyRuntime(
            final Object runtime) {
        return topologyRuntime(castRuntime(runtime));
    }

    public static <K, V> IndexWalCoordinator<K, V> walCoordinator(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.walCoordinator();
    }

    public static <K, V> SegmentIndexOperationAccess<K, V> operationAccess(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.operationAccess();
    }

    public static <K, V> RuntimeTuningState runtimeTuningState(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.storage().runtimeTuningState();
    }

    public static <K, V> Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.metricsSnapshotSupplier();
    }

    public static <K, V> RuntimeTuning runtimeTuning(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.runtimeTuning();
    }

    public static <K, V> void closeRuntime(final SegmentIndexRuntime<K, V> runtime,
            final String indexName, final ExecutorRegistry executorRegistry) {
        final SegmentIndexStateMachine stateMachine =
                new SegmentIndexStateMachine();
        stateMachine.markReady();
        new IndexCloseCoordinator<>(indexName, stateMachine,
                mock(IndexOperationTrackingAccess.class),
                new IndexOperationStatsRecorder(), runtime,
                executorRegistry,
                new IndexDirectoryLock(new MemDirectory()))
                .close();
    }

    public static void closeRuntime(final Object runtime,
            final String indexName, final ExecutorRegistry executorRegistry) {
        closeRuntime(castRuntime(runtime), indexName, executorRegistry);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentIndexRuntime<K, V> castRuntime(
            final Object runtime) {
        return (SegmentIndexRuntime<K, V>) runtime;
    }

}
