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
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
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

    private final SegmentIndexCoreStorage<K, V> coreStorage;
    private final SegmentTopologyRuntimeAccess<K, V> topologyRuntime;
    private final SegmentIndexRuntimeServices<K, V> services;

    public SegmentIndexRuntime(final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final SegmentIndexRuntimeServices<K, V> services) {
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.coreStorage = Vldtn.requireNonNull(coreStorage, "coreStorage");
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
        this.services = Vldtn.requireNonNull(services, "services");
    }

    void recoverFromWal() {
        coreStorage.storageService().recoverFromWal(
                services.operationAccess()::replayWalRecord);
    }

    void cleanupOrphanedSegmentDirectories() {
        coreStorage.storageService().cleanupOrphanedSegmentDirectories();
    }

    boolean hasSegmentLockFile(final SegmentId segmentId) {
        return coreStorage.storageService().hasSegmentLockFile(segmentId);
    }

    void runStartupConsistencyCheck() {
        coreStorage.storageService().runStartupConsistencyCheck();
        requestFullSplitScan();
    }

    void checkAndRepairConsistency() {
        coreStorage.storageService().checkAndRepairConsistency();
        requestFullSplitScan();
    }

    SegmentIndexCoreStorage<K, V> coreStorage() {
        return coreStorage;
    }

    SegmentTopologyRuntimeAccess<K, V> topologyRuntime() {
        return topologyRuntime;
    }

    StorageService<K, V> storageService() {
        return coreStorage.storageService();
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
        coreStorage.close();
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
        coreStorage.storageService().closeWal();
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
