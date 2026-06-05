package org.hestiastore.index.segmentindex.core.session;

import java.util.function.Predicate;
import java.util.function.Supplier;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.storage.IndexConsistencyChecker;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;

/**
 * Aggregates long-lived collaborators created for one running index session.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexRuntime<K, V>
        implements SegmentIndexDataAccess<K, V> {

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final SegmentIndexCoreStorage<K, V> coreStorage;
    private final SegmentTopologyRuntimeAccess<K, V> topologyRuntime;
    private final SegmentIndexRuntimeServices<K, V> services;

    public SegmentIndexRuntime(final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final SegmentIndexRuntimeServices<K, V> services) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.coreStorage = Vldtn.requireNonNull(coreStorage, "coreStorage");
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
        this.services = Vldtn.requireNonNull(services, "services");
    }

    void recoverFromWal() {
        services.walCoordinator().recover(
                services.operationAccess()::replayWalRecord);
    }

    void cleanupOrphanedSegmentDirectories() {
        topologyRuntime.cleanupOrphanedSegmentDirectories();
    }

    boolean hasSegmentLockFile(final SegmentId segmentId) {
        return topologyRuntime.hasSegmentLockFile(segmentId);
    }

    TypeDescriptor<K> keyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    SegmentIndexCoreStorage<K, V> coreStorage() {
        return coreStorage;
    }

    SegmentTopologyRuntimeAccess<K, V> topologyRuntime() {
        return topologyRuntime;
    }

    IndexWalCoordinator<K, V> walCoordinator() {
        return services.walCoordinator();
    }

    SegmentIndexOperationAccess<K, V> operationAccess() {
        return services.operationAccess();
    }

    Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier() {
        return services.metricsSnapshotSupplier();
    }

    SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier() {
        return services.runtimeLimitApplier();
    }

    SegmentIndexMetricsSnapshot metricsSnapshot() {
        return services.metricsSnapshotSupplier().get();
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

    void validateUniqueSegmentIds() {
        coreStorage.keyToSegmentMap().validateUniqueSegmentIds();
    }

    void checkAndRepairConsistency(
            final Predicate<SegmentId> segmentFilter) {
        new IndexConsistencyChecker<>(coreStorage.keyToSegmentMap(),
                coreStorage.segmentRegistry(), keyTypeDescriptor, segmentFilter)
                .checkAndRepairConsistency();
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

    void compact() {
        services.maintenance().compact();
    }

    void compactAndWait() {
        services.maintenance().compactAndWait();
    }

    void flush() {
        services.maintenance().flush();
    }

    void flushAndWait() {
        services.maintenance().flushAndWait();
    }

    void sealAsyncMaintenanceAndWait() {
        services.maintenance().sealAsyncMaintenanceAndWait();
    }

    void closeWalCoordinator() {
        services.walCoordinator().close();
    }

    void closeAfterFailedInitialization() {
        RuntimeException failure = null;
        failure = closeResource(this::closeSplitRuntime, failure);
        failure = closeResource(this::closeCoreStorage, failure);
        failure = closeResource(this::closeWalCoordinator, failure);
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
