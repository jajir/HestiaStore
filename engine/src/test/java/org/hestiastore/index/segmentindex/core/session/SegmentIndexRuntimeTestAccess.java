package org.hestiastore.index.segmentindex.core.session;

import java.util.function.Supplier;

import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.routing.SegmentIndexOperationAccess;
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

    public static <K, V> IndexControlPlane controlPlane(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.controlPlane();
    }
}
