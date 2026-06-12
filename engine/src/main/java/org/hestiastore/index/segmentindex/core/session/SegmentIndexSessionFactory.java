package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.streaming.SegmentIndexEntryIteratorDecorator;
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
     * @return created session index
     */
    public static <K, V> SegmentIndexSessionResource<K, V> createIndex(
            final SegmentIndexSessionResources<K, V> resources,
            final EffectiveIndexConfiguration<K, V> configuration,
            final TypeDescriptor<K> keyTypeDescriptor) {
        final SegmentIndexSessionResources<K, V> initializedResources =
                Vldtn.requireNonNull(resources, "resources");
        final EffectiveIndexConfiguration<K, V> initializedConfiguration =
                Vldtn.requireNonNull(configuration, "configuration");
        final SegmentIndexRuntime<K, V> initializedRuntime =
                initializedResources.runtime();
        final SegmentIndexSessionInfrastructure<K, V> infrastructure =
                initializedResources.sessionInfrastructure();
        final SegmentIndexPointOperationFacade<K, V> pointOperationFacade =
                newPointOperationFacade(infrastructure, initializedRuntime);
        final SegmentIndexReadFacade<K, V> readFacade = newReadFacade(
                initializedConfiguration, infrastructure, initializedRuntime);
        final SegmentIndexSessionOwner<K, V> sessionOwner = newSessionOwner(
                initializedResources, initializedConfiguration, infrastructure,
                initializedRuntime);
        final SegmentIndexMaintenance maintenanceApi = newMaintenanceApi(
                sessionOwner, infrastructure.trackedRunner(),
                initializedRuntime.maintenance(), initializedRuntime);
        return new SegmentIndexImpl<>(
                Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor"),
                pointOperationFacade, readFacade,
                maintenanceApi, sessionOwner);
    }

    private static <K, V> SegmentIndexPointOperationFacade<K, V>
            newPointOperationFacade(
                    final SegmentIndexSessionInfrastructure<K, V> infrastructure,
                    final SegmentIndexDataAccess<K, V> dataAccess) {
        return new SegmentIndexPointOperationFacade<>(
                infrastructure.trackedRunner(), dataAccess);
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
            final SegmentIndexRuntime<K, V> runtime) {
        return new SegmentIndexSessionOwner<>(infrastructure.stateMachine(),
                runtime,
                new IndexCloseCoordinator<>(configuration.identity().name(),
                        infrastructure.stateMachine(),
                        infrastructure.operationGate(),
                        infrastructure.operationStatsRecorder(), runtime,
                        resources.executorRegistry(),
                        resources.directoryLock()));
    }

    private static <K, V> SegmentIndexMaintenance newMaintenanceApi(
            final SegmentIndexSessionOwner<K, V> sessionOwner,
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner,
            final MaintenanceService maintenance,
            final SegmentIndexRuntime<K, V> runtime) {
        final SegmentIndexMaintenance maintenanceApi =
                new SegmentIndexMaintenanceImpl(maintenance,
                        runtime.storageService(),
                        runtime::requestFullSplitScan);
        return new SegmentIndexMaintenanceSessionAdapter<>(maintenanceApi,
                sessionOwner, trackedRunner);
    }
}
