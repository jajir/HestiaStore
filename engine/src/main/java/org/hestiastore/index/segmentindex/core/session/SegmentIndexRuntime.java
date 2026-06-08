package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;

/**
 * Aggregates long-lived collaborators created for one running index session.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexRuntime<K, V>
        implements SegmentIndexDataAccess<K, V> {

    private final StorageService<K, V> storageService;
    private final Runnable closeCoreStorageAction;
    private final SegmentTopologyRuntimeAccess<K, V> topologyRuntime;
    private final SegmentIndexRuntimeServices<K, V> services;

    /**
     * Creates a running segment-index runtime from already opened package
     * services.
     *
     * @param keyTypeDescriptor key descriptor validated during bootstrap
     * @param storageService storage package entry point
     * @param closeCoreStorageAction action that closes opened storage resources
     * @param topologyRuntime topology and streaming runtime access
     * @param services operation, maintenance, tuning, and monitoring services
     */
    public SegmentIndexRuntime(final TypeDescriptor<K> keyTypeDescriptor,
            final StorageService<K, V> storageService,
            final Runnable closeCoreStorageAction,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final SegmentIndexRuntimeServices<K, V> services) {
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
        this.closeCoreStorageAction = Vldtn.requireNonNull(
                closeCoreStorageAction, "closeCoreStorageAction");
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
        this.services = Vldtn.requireNonNull(services, "services");
    }

    void recoverFromWal() {
        storageService.recoverFromWal(services.operationAccess()::replayWalRecord);
    }

    void cleanupOrphanedSegmentDirectories() {
        storageService.cleanupOrphanedSegmentDirectories();
    }

    boolean hasSegmentLockFile(final SegmentId segmentId) {
        return storageService.hasSegmentLockFile(segmentId);
    }

    void runStartupConsistencyCheck() {
        storageService.runStartupConsistencyCheck();
        requestFullSplitScan();
    }

    void checkAndRepairConsistency() {
        storageService.checkAndRepairConsistency();
        requestFullSplitScan();
    }

    SegmentTopologyRuntimeAccess<K, V> topologyRuntime() {
        return topologyRuntime;
    }

    StorageService<K, V> storageService() {
        return storageService;
    }

    SegmentIndexOperationAccess<K, V> operationAccess() {
        return services.operationAccess();
    }

    IndexRuntimeMonitoring runtimeMonitoring() {
        return services.runtimeMonitoring();
    }

    @Override
    public void put(final K key, final V value) {
        services.operationAccess().put(key, value);
    }

    @Override
    public V get(final K key) {
        return services.operationAccess().get(key);
    }

    @Override
    public void delete(final K key) {
        services.operationAccess().delete(key);
    }

    RuntimeTuning runtimeTuning() {
        return services.runtimeTuning();
    }

    void invalidateSegmentIterators() {
        topologyRuntime.invalidateSegmentIterators();
    }

    void requestFullSplitScan() {
        topologyRuntime.requestFullSplitScan();
    }

    void closeSplitRuntime() {
        topologyRuntime.closeSplitRuntime();
    }

    void closeCoreStorage() {
        closeCoreStorageAction.run();
    }

    @Override
    public EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        return topologyRuntime.openSegmentIterator(segmentId, isolation);
    }

    @Override
    public EntryIterator<K, V> openWindowIterator(
            final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        return topologyRuntime.openWindowIterator(segmentWindow, isolation);
    }

    MaintenanceService maintenance() {
        return services.maintenance();
    }

    void flushAndWait() {
        services.maintenance().flushAndWait();
    }

    void sealAsyncMaintenanceAndWait() {
        services.maintenance().sealAsyncMaintenanceAndWait();
    }

    void closeWal() {
        storageService.closeWal();
    }

    void closeAfterFailedInitialization() {
        RuntimeException failure = null;
        failure = closeResource(this::closeSplitRuntime, failure);
        failure = closeResource(this::closeCoreStorage, failure);
        failure = closeResource(this::closeWal, failure);
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException closeResource(final Runnable closeAction,
            final RuntimeException failure) {
        try {
            closeAction.run();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            if (failure == null) {
                return cleanupFailure;
            }
            failure.addSuppressed(cleanupFailure);
            return failure;
        }
    }

}
