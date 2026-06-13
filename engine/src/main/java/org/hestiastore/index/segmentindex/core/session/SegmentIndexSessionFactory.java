package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.streaming.SegmentIndexEntryIteratorDecorator;
import org.hestiastore.index.segmentindex.maintenance.IndexConsistencyRepairService;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenanceImpl;

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
     * @param assemblyInput concrete runtime collaborators used to assemble the
     *            session handle
     * @return created session index
     */
    public static <K, V> SegmentIndexSessionResource<K, V> createIndex(
            final SegmentIndexSessionResources<K, V> resources,
            final EffectiveIndexConfiguration<K, V> configuration,
            final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentIndexSessionAssemblyInput<K, V> assemblyInput) {
        final SegmentIndexSessionResources<K, V> initializedResources =
                Vldtn.requireNonNull(resources, "resources");
        final EffectiveIndexConfiguration<K, V> initializedConfiguration =
                Vldtn.requireNonNull(configuration, "configuration");
        final SegmentIndexSessionAssemblyInput<K, V> input = Vldtn
                .requireNonNull(assemblyInput, "assemblyInput");
        final SegmentIndexSessionInfrastructure<K, V> infrastructure =
                initializedResources.sessionInfrastructure();
        final SegmentIndexDataAccess<K, V> dataAccess =
                newDataAccess(input);
        final SegmentIndexPointOperationFacade<K, V> pointOperationFacade =
                newPointOperationFacade(infrastructure, dataAccess);
        final SegmentIndexReadFacade<K, V> readFacade = newReadFacade(
                initializedConfiguration, infrastructure, dataAccess);
        final SegmentIndexSessionOwner<K, V> sessionOwner = newSessionOwner(
                initializedResources, initializedConfiguration, infrastructure,
                input);
        final SegmentIndexMaintenance maintenanceApi = newMaintenanceApi(
                infrastructure.trackedRunner(), input);
        return new SegmentIndexImpl<>(
                Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor"),
                pointOperationFacade, readFacade,
                input.runtimeTuning(),
                input.runtimeMonitoring(),
                maintenanceApi, sessionOwner);
    }

    private static <K, V> SegmentIndexPointOperationFacade<K, V>
            newPointOperationFacade(
                    final SegmentIndexSessionInfrastructure<K, V> infrastructure,
                    final SegmentIndexDataAccess<K, V> dataAccess) {
        return new SegmentIndexPointOperationFacade<>(
                infrastructure.trackedRunner(), dataAccess);
    }

    private static <K, V> SegmentIndexDataAccess<K, V> newDataAccess(
            final SegmentIndexSessionAssemblyInput<K, V> input) {
        return new SegmentIndexDataAccessImpl<>(input.operationAccess(),
                input.topologyRuntime());
    }

    private static <K, V> SegmentIndexReadFacade<K, V> newReadFacade(
            final EffectiveIndexConfiguration<K, V> configuration,
            final SegmentIndexSessionInfrastructure<K, V> infrastructure,
            final SegmentIndexDataAccess<K, V> dataAccess) {
        return new SegmentIndexReadFacade<>(infrastructure.trackedRunner(),
                dataAccess,
                new SegmentIndexEntryIteratorDecorator<>(configuration));
    }

    private static <K, V> SegmentIndexSessionOwner<K, V> newSessionOwner(
            final SegmentIndexSessionResources<K, V> resources,
            final EffectiveIndexConfiguration<K, V> configuration,
            final SegmentIndexSessionInfrastructure<K, V> infrastructure,
            final SegmentIndexSessionAssemblyInput<K, V> input) {
        final IndexRuntimeCloseResources<K, V> closeResources =
                new IndexRuntimeCloseResources<>(
                        input.topologyRuntime(),
                        input.maintenance(),
                        input.coreStorageRuntime(),
                        input.storageService());
        return new SegmentIndexSessionOwner<>(
                infrastructure.stateMachine(),
                new IndexCloseCoordinator<>(
                        configuration.identity().name(),
                        infrastructure.stateMachine(),
                        infrastructure.operationGate(),
                        infrastructure.operationStatsRecorder(),
                        closeResources,
                        resources.executorRegistry(),
                        resources.directoryLock()));
    }

    private static <K, V> SegmentIndexMaintenance newMaintenanceApi(
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner,
            final SegmentIndexSessionAssemblyInput<K, V> input) {
        final IndexConsistencyRepairService consistencyRepairService =
                new StorageTopologyConsistencyRepairService(
                        input.storageService(), input.topologyRuntime());
        final SegmentIndexMaintenance maintenanceApi =
                new SegmentIndexMaintenanceImpl(input.maintenance(),
                        consistencyRepairService);
        return new SegmentIndexMaintenanceSessionAdapter<>(maintenanceApi,
                input.topologyRuntime(), trackedRunner);
    }
}
