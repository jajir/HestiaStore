package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.execution.MappedSegmentMaintenanceService;
import org.hestiastore.index.segmentindex.core.execution.PointOperationCoordinator;
import org.hestiastore.index.segmentindex.core.execution.SegmentIteratorService;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentindex.core.storage.OpenedStorageRuntime;
import org.hestiastore.index.segmentindex.monitoring.SegmentIndexRuntimeMonitoring;

/**
 * Test-only view of runtime collaborators extracted from an opened session
 * handle.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexRuntimeView<K, V> {

    private final OpenedStorageRuntime<K, V> coreStorageRuntime;
    private final SplitRuntime<K, V> splitService;
    private final SegmentIteratorService<K, V> streamingService;
    private final PointOperationCoordinator<K, V> operationAccess;
    private final MappedSegmentMaintenanceService<K, V> maintenance;
    private final SegmentIndexRuntimeMonitoring runtimeMonitoring;
    private final RuntimeTuning runtimeTuning;

    /**
     * Creates a test runtime view.
     *
     * @param coreStorageRuntime core storage runtime
     * @param splitService split service
     * @param streamingService streaming service
     * @param operationAccess operation access
     * @param maintenance maintenance service
     * @param runtimeMonitoring runtime monitoring view
     * @param runtimeTuning runtime tuning view
     */
    SegmentIndexRuntimeView(final OpenedStorageRuntime<K, V> coreStorageRuntime,
            final SplitRuntime<K, V> splitService,
            final SegmentIteratorService<K, V> streamingService,
            final PointOperationCoordinator<K, V> operationAccess,
            final MappedSegmentMaintenanceService<K, V> maintenance,
            final SegmentIndexRuntimeMonitoring runtimeMonitoring,
            final RuntimeTuning runtimeTuning) {
        this.coreStorageRuntime = Vldtn.requireNonNull(coreStorageRuntime,
                "coreStorageRuntime");
        this.splitService = Vldtn.requireNonNull(splitService,
                "splitService");
        this.streamingService = Vldtn.requireNonNull(streamingService,
                "streamingService");
        this.operationAccess = Vldtn.requireNonNull(operationAccess,
                "operationAccess");
        this.maintenance = Vldtn.requireNonNull(maintenance, "maintenance");
        this.runtimeMonitoring = Vldtn.requireNonNull(runtimeMonitoring,
                "runtimeMonitoring");
        this.runtimeTuning = Vldtn.requireNonNull(runtimeTuning,
                "runtimeTuning");
    }

    OpenedStorageRuntime<K, V> coreStorageRuntime() {
        return coreStorageRuntime;
    }

    SplitRuntime<K, V> splitService() {
        return splitService;
    }

    SegmentIteratorService<K, V> streamingService() {
        return streamingService;
    }

    PointOperationCoordinator<K, V> operationAccess() {
        return operationAccess;
    }

    MappedSegmentMaintenanceService<K, V> maintenance() {
        return maintenance;
    }

    SegmentIndexRuntimeMonitoring runtimeMonitoring() {
        return runtimeMonitoring;
    }

    RuntimeTuning runtimeTuning() {
        return runtimeTuning;
    }
}
