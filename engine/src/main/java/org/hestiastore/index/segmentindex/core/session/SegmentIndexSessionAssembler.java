package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.execution.PointOperationCoordinator;
import org.hestiastore.index.segmentindex.core.execution.MappedSegmentMaintenanceService;
import org.hestiastore.index.segmentindex.core.execution.SegmentIteratorService;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentindex.core.storage.OpenedStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageCoordinator;
import org.hestiastore.index.segmentindex.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.monitoring.SegmentIndexRuntimeMonitoring;

/**
 * Assembles the final session-facing index from initialized bootstrap
 * resources.
 */
public final class SegmentIndexSessionAssembler {

    private SegmentIndexSessionAssembler() {
    }

    /**
     * Creates the index handle returned by a successful bootstrap run.
     *
     * @param <K> key type
     * @param <V> value type
     * @param resources initialized session resources
     * @param configuration effective index configuration
     * @param keyTypeDescriptor key type descriptor
     * @param operationAccess point-operation and WAL replay access
     * @param splitService split runtime service
     * @param streamingService segment streaming service
     * @param maintenance maintenance service
     * @param runtimeTuning runtime tuning API view
     * @param runtimeMonitoring runtime monitoring API view
     * @param coreStorageRuntime core storage lifecycle owner
     * @return created session index
     */
    public static <K, V> SegmentIndex<K, V> createIndex(
            final SegmentIndexRuntimeResources<K, V> resources,
            final EffectiveIndexConfiguration<K, V> configuration,
            final TypeDescriptor<K> keyTypeDescriptor,
            final PointOperationCoordinator<K, V> operationAccess,
            final SplitRuntime<K, V> splitService,
            final SegmentIteratorService<K, V> streamingService,
            final MappedSegmentMaintenanceService<K, V> maintenance,
            final RuntimeTuning runtimeTuning,
            final SegmentIndexRuntimeMonitoring runtimeMonitoring,
            final OpenedStorageRuntime<K, V> coreStorageRuntime) {
        final SegmentIndexRuntimeResources<K, V> initializedResources =
                Vldtn.requireNonNull(resources, "resources");
        final EffectiveIndexConfiguration<K, V> initializedConfiguration =
                Vldtn.requireNonNull(configuration, "configuration");
        final SplitRuntime<K, V> initializedSplitService = Vldtn
                .requireNonNull(splitService, "splitService");
        final SegmentIteratorService<K, V> initializedStreamingService =
                Vldtn.requireNonNull(streamingService, "streamingService");
        final MappedSegmentMaintenanceService<K, V> initializedMaintenance = Vldtn
                .requireNonNull(maintenance, "maintenance");
        final OpenedStorageRuntime<K, V> initializedCoreStorageRuntime =
                Vldtn.requireNonNull(coreStorageRuntime,
                        "coreStorageRuntime");
        final SessionCloseCoordinator<K, V> closeCoordinator =
                newCloseCoordinator(initializedResources,
                        initializedConfiguration,
                        initializedSplitService, initializedMaintenance,
                        initializedCoreStorageRuntime);
        final SegmentIndexMaintenance maintenanceApi = newMaintenanceApi(
                initializedResources.stateMachine(),
                initializedResources.operationGate(), initializedSplitService,
                initializedStreamingService,
                initializedMaintenance,
                initializedCoreStorageRuntime.getStorageService());
        return new SegmentIndexSession<>(
                Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor"),
                initializedResources.operationGate(),
                Vldtn.requireNonNull(operationAccess, "operationAccess"),
                initializedStreamingService,
                initializedConfiguration,
                Vldtn.requireNonNull(runtimeTuning, "runtimeTuning"),
                Vldtn.requireNonNull(runtimeMonitoring, "runtimeMonitoring"),
                maintenanceApi,
                initializedResources.stateMachine(),
                closeCoordinator);
    }

    private static <K, V> SessionCloseCoordinator<K, V> newCloseCoordinator(
            final SegmentIndexRuntimeResources<K, V> resources,
            final EffectiveIndexConfiguration<K, V> configuration,
            final SplitRuntime<K, V> splitService,
            final MappedSegmentMaintenanceService<K, V> maintenance,
            final OpenedStorageRuntime<K, V> coreStorageRuntime) {
        return new SessionCloseCoordinator<>(
                configuration.identity().name(),
                resources.stateMachine(),
                resources.operationGate(),
                resources.operationStatsRecorder(),
                splitService,
                maintenance,
                coreStorageRuntime,
                coreStorageRuntime.getStorageService(),
                resources.executorRegistry(),
                resources.runtimeHandle(),
                resources.directoryLock());
    }

    private static <K, V> SegmentIndexMaintenance newMaintenanceApi(
            final SegmentIndexStateMachine stateMachine,
            final SessionOperationGate operationGate,
            final SplitRuntime<K, V> splitService,
            final SegmentIteratorService<K, V> streamingService,
            final MappedSegmentMaintenanceService<K, V> maintenance,
            final StorageCoordinator<K, V> storageService) {
        return new MaintenanceApiAdapter<>(
                maintenance, storageService, splitService, streamingService,
                stateMachine, operationGate);
    }
}
