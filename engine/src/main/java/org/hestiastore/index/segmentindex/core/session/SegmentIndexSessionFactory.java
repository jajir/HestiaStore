package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationCoordinator;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenanceImpl;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;

/**
 * Assembles the final session-facing index from initialized bootstrap
 * resources.
 */
public final class SegmentIndexSessionFactory {

    private SegmentIndexSessionFactory() {
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
     * @param topologyRuntime topology and iterator runtime access
     * @param maintenance maintenance service
     * @param runtimeTuning runtime tuning API view
     * @param runtimeMonitoring runtime monitoring API view
     * @param coreStorageRuntime core storage lifecycle owner
     * @return created session index
     */
    public static <K, V> SegmentIndex<K, V> createIndex(
            final SegmentIndexSessionResources<K, V> resources,
            final EffectiveIndexConfiguration<K, V> configuration,
            final TypeDescriptor<K> keyTypeDescriptor,
            final IndexOperationCoordinator<K, V> operationAccess,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final MaintenanceService<K, V> maintenance,
            final RuntimeTuning runtimeTuning,
            final IndexRuntimeMonitoring runtimeMonitoring,
            final CoreStorageRuntime<K, V> coreStorageRuntime) {
        final SegmentIndexSessionResources<K, V> initializedResources =
                Vldtn.requireNonNull(resources, "resources");
        final EffectiveIndexConfiguration<K, V> initializedConfiguration =
                Vldtn.requireNonNull(configuration, "configuration");
        final IndexOperationCoordinator<K, V> initializedOperationAccess =
                Vldtn.requireNonNull(operationAccess, "operationAccess");
        final SegmentTopologyRuntimeAccess<K, V> initializedTopologyRuntime =
                Vldtn.requireNonNull(topologyRuntime, "topologyRuntime");
        final MaintenanceService<K, V> initializedMaintenance = Vldtn
                .requireNonNull(maintenance, "maintenance");
        final RuntimeTuning initializedRuntimeTuning = Vldtn.requireNonNull(
                runtimeTuning, "runtimeTuning");
        final IndexRuntimeMonitoring initializedRuntimeMonitoring = Vldtn
                .requireNonNull(runtimeMonitoring, "runtimeMonitoring");
        final CoreStorageRuntime<K, V> initializedCoreStorageRuntime =
                Vldtn.requireNonNull(coreStorageRuntime,
                        "coreStorageRuntime");
        final IndexCloseCoordinator<K, V> closeCoordinator =
                newCloseCoordinator(initializedResources,
                        initializedConfiguration,
                        initializedTopologyRuntime, initializedMaintenance,
                        initializedCoreStorageRuntime);
        final SegmentIndexMaintenance maintenanceApi = newMaintenanceApi(
                initializedResources.trackedRunner(), initializedTopologyRuntime,
                initializedMaintenance,
                initializedCoreStorageRuntime.getStorageService());
        return new SegmentIndexImpl<>(
                Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor"),
                initializedResources.trackedRunner(),
                initializedOperationAccess,
                initializedTopologyRuntime,
                initializedConfiguration,
                initializedRuntimeTuning,
                initializedRuntimeMonitoring,
                maintenanceApi,
                initializedResources.stateMachine(),
                closeCoordinator);
    }

    private static <K, V> IndexCloseCoordinator<K, V> newCloseCoordinator(
            final SegmentIndexSessionResources<K, V> resources,
            final EffectiveIndexConfiguration<K, V> configuration,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final MaintenanceService<K, V> maintenance,
            final CoreStorageRuntime<K, V> coreStorageRuntime) {
        return new IndexCloseCoordinator<>(
                configuration.identity().name(),
                resources.stateMachine(),
                resources.operationGate(),
                resources.operationStatsRecorder(),
                topologyRuntime,
                maintenance,
                coreStorageRuntime,
                coreStorageRuntime.getStorageService(),
                resources.executorRegistry(),
                resources.directoryLock());
    }

    private static <K, V> SegmentIndexMaintenance newMaintenanceApi(
            final SegmentIndexTrackedOperationRunner trackedRunner,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final MaintenanceService<K, V> maintenance,
            final StorageService<K, V> storageService) {
        final SegmentIndexMaintenance maintenanceApi =
                new SegmentIndexMaintenanceImpl(maintenance,
                        storageService);
        return new SegmentIndexMaintenanceSessionAdapter<>(maintenanceApi,
                topologyRuntime, trackedRunner);
    }
}
