package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateView;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStatsRecorder;
import org.hestiastore.index.segmentindex.core.streaming.SegmentIndexEntryIteratorDecorator;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenanceImpl;

/**
 * TODO replace class with SegmentIndexBootstrapRequest amd
 * SegmentIndexBootstrapState builders that enforce correct resource
 * initialization order and availability
 */

/**
 * Holds package-private session resources while bootstrap steps live outside
 * the session package.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexSessionResources<K, V>
        implements SegmentIndexStateView {

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

    public boolean wasStaleLockRecovered() {
        return directoryLock().wasStaleLockRecovered();
    }

    public void recoverFromWal() {
        runtime().recoverFromWal();
    }

    public void cleanupOrphanedSegmentDirectories() {
        runtime().cleanupOrphanedSegmentDirectories();
    }

    public void markReady() {
        stateMachine().markReady();
    }

    public void runStartupConsistencyCheck() {
        runtime().runStartupConsistencyCheck();
    }

    public void requestFullSplitScan() {
        runtime().requestFullSplitScan();
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

    public void setRuntime(final SegmentIndexRuntime<K, V> runtime,
            final ExecutorRegistry executorRegistry) {
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        this.runtime = Vldtn.requireNonNull(runtime, "runtime");
    }

    /**
     * Returns the current segment-index lifecycle state.
     *
     * @return current segment-index lifecycle state
     */
    @Override
    public SegmentIndexState currentState() {
        return stateMachine().getState();
    }

    public void markRuntimeFailure(final RuntimeException failure) {
        stateMachine().markRuntimeFailure(failure);
    }

    public void closeRuntimeAfterFailedInitialization() {
        if (runtime != null) {
            runtime.closeAfterFailedInitialization();
        }
    }

    public SegmentIndexSessionHandle<K, V> createIndex(
            final EffectiveIndexConfiguration<K, V> configuration,
            final TypeDescriptor<K> keyTypeDescriptor) {
        final SegmentIndexRuntime<K, V> initializedRuntime = runtime();
        final SegmentIndexPointOperationFacade<K, V> pointOperationFacade =
                newPointOperationFacade(initializedRuntime);
        final SegmentIndexReadFacade<K, V> readFacade = newReadFacade(
                configuration, initializedRuntime);
        final SegmentIndexSessionOwner<K, V> sessionOwner = newSessionOwner(
                configuration, initializedRuntime);
        final SegmentIndexMaintenance maintenanceApi = newMaintenanceApi(
                sessionOwner, trackedRunner(), initializedRuntime.maintenance(),
                initializedRuntime);
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

    private SegmentIndexSessionOwner<K, V> newSessionOwner(
            final EffectiveIndexConfiguration<K, V> conf,
            final SegmentIndexRuntime<K, V> runtime) {
        return new SegmentIndexSessionOwner<>(stateMachine(), runtime,
                new IndexCloseCoordinator<>(conf.identity().name(),
                        stateMachine(), operationGate(),
                        operationStatsRecorder(), runtime,
                        executorRegistry(), directoryLock()));
    }

    private SegmentIndexMaintenance newMaintenanceApi(
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

    private IndexDirectoryLock directoryLock() {
        return Vldtn.requireNonNull(directoryLock, "directoryLock");
    }

    private SegmentIndexStateMachine stateMachine() {
        return Vldtn.requireNonNull(stateMachine, "stateMachine");
    }

    public IndexOperationStatsRecorder operationStatsRecorder() {
        return Vldtn.requireNonNull(operationStatsRecorder,
                "operationStatsRecorder");
    }

    public MaintenanceStatsRecorder maintenanceStatsRecorder() {
        return Vldtn.requireNonNull(maintenanceStatsRecorder,
                "maintenanceStatsRecorder");
    }

    private ExecutorRegistry executorRegistry() {
        return Vldtn.requireNonNull(executorRegistry, "executorRegistry");
    }

    public SplitStatsRecorder splitStatsRecorder() {
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
