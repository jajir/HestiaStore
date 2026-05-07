package org.hestiastore.index.segmentindex.core.session;

import static org.mockito.Mockito.mock;

import java.util.function.Supplier;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.tuning.RuntimeConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.session.state.IndexStateCoordinator;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopologyRuntime;
import org.hestiastore.index.segmentindex.metrics.Stats;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;

/**
 * Test-only bridge exposing package-private runtime internals without keeping
 * them public in production code.
 */
public final class SegmentIndexRuntimeTestAccess {

    private SegmentIndexRuntimeTestAccess() {
    }

    public static <K, V> Object openRuntime(final Logger logger,
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final ExecutorRegistry executorRegistry) {
        return SegmentIndexRuntime.create(logger, directoryFacade,
                keyTypeDescriptor, valueTypeDescriptor, conf,
                conf.resolveRuntimeConfiguration(), executorRegistry,
                new Stats(), () -> SegmentIndexState.READY, failure -> {
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

    public static <K, V> SegmentTopologyRuntime<K, V> topologyRuntime(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.topologyRuntime();
    }

    public static <K, V> SegmentTopologyRuntime<K, V> topologyRuntime(
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

    public static <K, V> RuntimeConfiguration runtimeTuning(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.runtimeTuning();
    }

    public static <K, V> void closeRuntime(final SegmentIndexRuntime<K, V> runtime,
            final String indexName) {
        new IndexCloseCoordinator<>(org.slf4j.LoggerFactory
                .getLogger(SegmentIndexRuntimeTestAccess.class), indexName,
                mockIndexStateCoordinator(),
                mock(IndexOperationTrackingAccess.class),
                new org.hestiastore.index.segmentindex.metrics.Stats(), runtime)
                .close();
    }

    public static void closeRuntime(final Object runtime,
            final String indexName) {
        closeRuntime(castRuntime(runtime), indexName);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentIndexRuntime<K, V> castRuntime(
            final Object runtime) {
        return (SegmentIndexRuntime<K, V>) runtime;
    }

    @SuppressWarnings("unchecked")
    private static <K, V> IndexStateCoordinator<K, V>
            mockIndexStateCoordinator() {
        return mock(IndexStateCoordinator.class);
    }
}
