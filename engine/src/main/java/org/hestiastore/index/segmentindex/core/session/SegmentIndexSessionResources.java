package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStatsRecorder;
import org.hestiastore.index.segmentindex.core.storage.IndexConsistencyCoordinator;
import org.hestiastore.index.segmentindex.core.streaming.SegmentIndexEntryIteratorDecorator;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenanceImpl;

/**
 * Holds package-private session resources while bootstrap steps live outside
 * the session package.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexSessionResources<K, V> {

    private IndexDirectoryLock directoryLock;
    private SegmentIndexStateMachine stateMachine;
    private IndexOperationStatsRecorder operationStatsRecorder;
    private MaintenanceStatsRecorder maintenanceStatsRecorder;
    private SplitStatsRecorder splitStatsRecorder;
    private ExecutorRegistry executorRegistry;
    private SegmentIndexOperationGate operationGate;
    private SegmentIndexTrackedOperationRunner<K, V> trackedRunner;
    private SegmentIndexRuntime<K, V> runtime;

    public void acquireDirectoryLock(final Directory directory) {
        directoryLock = new IndexDirectoryLock(directory);
    }

    public void createSessionInfrastructure() {
        stateMachine = new SegmentIndexStateMachine();
        operationStatsRecorder = new IndexOperationStatsRecorder();
        maintenanceStatsRecorder = new MaintenanceStatsRecorder();
        splitStatsRecorder = new SplitStatsRecorder();
        operationGate = SegmentIndexOperationGate.create();
        trackedRunner = new SegmentIndexTrackedOperationRunner<>(stateMachine,
                operationGate);
    }

    public void createRuntime(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final EffectiveIndexConfiguration<K, V> configuration,
            final ExecutorRegistry executorRegistry) {
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        runtime = SegmentIndexRuntime.create(directory, keyTypeDescriptor,
                valueTypeDescriptor, configuration, executorRegistry,
                operationStatsRecorder(), maintenanceStatsRecorder(),
                splitStatsRecorder(),
                stateMachine()::getState, stateMachine()::markRuntimeFailure);
    }

    public void closeRuntimeAfterFailedInitialization() {
        if (runtime != null) {
            runtime.closeAfterFailedInitialization();
        }
    }

    public IndexInternal<K, V> createIndex(
            final EffectiveIndexConfiguration<K, V> configuration,
            final TypeDescriptor<K> keyTypeDescriptor) {
        final SegmentIndexRuntime<K, V> initializedRuntime = runtime();
        final IndexConsistencyCoordinator<K, V> consistencyCoordinator =
                newConsistencyCoordinator(initializedRuntime);
        final SegmentIndexPointOperationFacade<K, V> pointOperationFacade =
                newPointOperationFacade(initializedRuntime);
        final SegmentIndexReadFacade<K, V> readFacade = newReadFacade(
                configuration, initializedRuntime);
        final SegmentIndexSessionOwner<K, V> sessionOwner = newSessionOwner(
                configuration, initializedRuntime, consistencyCoordinator);
        final SegmentIndexMaintenance maintenanceApi = newMaintenanceApi(
                sessionOwner, trackedRunner(), initializedRuntime.maintenance(),
                consistencyCoordinator);
        final SegmentIndexImpl<K, V> index = new SegmentIndexImpl<>(
                keyTypeDescriptor, pointOperationFacade,
                readFacade, maintenanceApi, sessionOwner);
        return index;
    }

    private SegmentIndexPointOperationFacade<K, V> newPointOperationFacade(
            final SegmentIndexDataAccess<K, V> dataAccess) {
        return new SegmentIndexPointOperationFacade<>(trackedRunner(),
                dataAccess);
    }

    private SegmentIndexReadFacade<K, V> newReadFacade(
            final EffectiveIndexConfiguration<K, V> configuration,
            final SegmentIndexDataAccess<K, V> dataAccess) {
        return new SegmentIndexReadFacade<>(trackedRunner(), dataAccess,
                new SegmentIndexEntryIteratorDecorator<>(configuration));
    }

    private IndexConsistencyCoordinator<K, V> newConsistencyCoordinator(
            final SegmentIndexRuntime<K, V> runtime) {
        final SegmentIndexRuntime<K, V> validatedRuntime = Vldtn
                .requireNonNull(runtime, "runtime");
        return new IndexConsistencyCoordinator<>(
                validatedRuntime::validateUniqueSegmentIds,
                validatedRuntime::checkAndRepairConsistency,
                validatedRuntime::cleanupOrphanedSegmentDirectories,
                validatedRuntime::requestFullSplitScan,
                validatedRuntime::hasSegmentLockFile);
    }

    private SegmentIndexSessionOwner<K, V> newSessionOwner(
            final EffectiveIndexConfiguration<K, V> conf,
            final SegmentIndexRuntime<K, V> runtime,
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator) {
        return new SegmentIndexSessionOwner<>(stateMachine(), runtime,
                new IndexCloseCoordinator<>(conf.identity().name(),
                        stateMachine(), operationGate(),
                        operationStatsRecorder(), runtime,
                        executorRegistry(), directoryLock()),
                new SegmentIndexStartupCoordinator<>(conf.identity().name(),
                        directoryLock().wasStaleLockRecovered(), runtime,
                        stateMachine(), consistencyCoordinator));
    }

    private SegmentIndexMaintenance newMaintenanceApi(
            final SegmentIndexSessionOwner<K, V> sessionOwner,
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner,
            final MaintenanceService maintenance,
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator) {
        final SegmentIndexMaintenance maintenanceApi =
                new SegmentIndexMaintenanceImpl(maintenance,
                        consistencyCoordinator);
        return new SegmentIndexMaintenanceSessionAdapter<>(maintenanceApi,
                sessionOwner, trackedRunner);
    }

    private IndexDirectoryLock directoryLock() {
        return Vldtn.requireNonNull(directoryLock, "directoryLock");
    }

    private SegmentIndexStateMachine stateMachine() {
        return Vldtn.requireNonNull(stateMachine, "stateMachine");
    }

    private IndexOperationStatsRecorder operationStatsRecorder() {
        return Vldtn.requireNonNull(operationStatsRecorder,
                "operationStatsRecorder");
    }

    private MaintenanceStatsRecorder maintenanceStatsRecorder() {
        return Vldtn.requireNonNull(maintenanceStatsRecorder,
                "maintenanceStatsRecorder");
    }

    private ExecutorRegistry executorRegistry() {
        return Vldtn.requireNonNull(executorRegistry, "executorRegistry");
    }

    private SplitStatsRecorder splitStatsRecorder() {
        return Vldtn.requireNonNull(splitStatsRecorder, "splitStatsRecorder");
    }

    private SegmentIndexOperationGate operationGate() {
        return Vldtn.requireNonNull(operationGate, "operationGate");
    }

    private SegmentIndexTrackedOperationRunner<K, V> trackedRunner() {
        return Vldtn.requireNonNull(trackedRunner, "trackedRunner");
    }

    private SegmentIndexRuntime<K, V> runtime() {
        return Vldtn.requireNonNull(runtime, "runtime");
    }
}
