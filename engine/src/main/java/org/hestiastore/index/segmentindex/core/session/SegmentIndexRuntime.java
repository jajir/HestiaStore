package org.hestiastore.index.segmentindex.core.session;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.split.SplitStatsRecorder;
import org.hestiastore.index.segmentindex.core.storage.IndexConsistencyChecker;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexRuntimeStorage;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Aggregates long-lived collaborators created for one running index session.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexRuntime<K, V>
        implements SegmentIndexDataAccess<K, V> {

    static <K, V> SegmentIndexRuntime<K, V> create(
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final EffectiveIndexConfiguration<K, V> conf,
            final ExecutorRegistry executorRegistry,
            final IndexOperationStatsRecorder operationStatsRecorder,
            final MaintenanceStatsRecorder maintenanceStatsRecorder,
            final SplitStatsRecorder splitStatsRecorder,
            final Supplier<SegmentIndexState> stateSupplier,
            final Consumer<RuntimeException> failureHandler) {
        return new SegmentIndexRuntimeFactory<>(
                new SegmentIndexRuntimeOpenContext<>(directoryFacade,
                        keyTypeDescriptor,
                        valueTypeDescriptor, conf, executorRegistry,
                        operationStatsRecorder, maintenanceStatsRecorder,
                        splitStatsRecorder,
                        new AtomicLong(),
                        new AtomicLong(),
                        new AtomicLong(),
                        stateSupplier, failureHandler))
                .open();
    }

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final SegmentIndexRuntimeStorage<K, V> storage;
    private final SegmentTopologyRuntimeAccess<K, V> topologyRuntime;
    private final SegmentIndexRuntimeServices<K, V> services;
    private final WalRuntime<K, V> walRuntime;

    SegmentIndexRuntime(final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentIndexRuntimeStorage<K, V> storage,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final WalRuntime<K, V> walRuntime,
            final SegmentIndexRuntimeServices<K, V> services) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.storage = Vldtn.requireNonNull(storage, "storage");
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
        this.services = Vldtn.requireNonNull(services, "services");
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
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

    SegmentIndexRuntimeStorage<K, V> storage() {
        return storage;
    }

    WalRuntime<K, V> walRuntime() {
        return walRuntime;
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
        storage.keyToSegmentMap().validateUniqueSegmentIds();
    }

    void checkAndRepairConsistency(
            final Predicate<SegmentId> segmentFilter) {
        new IndexConsistencyChecker<>(storage.keyToSegmentMap(),
                storage.segmentRegistry(), keyTypeDescriptor, segmentFilter)
                .checkAndRepairConsistency();
    }

    void closeSplitRuntime() {
        topologyRuntime.closeSplitRuntime();
    }

    void closeSegmentRegistry() {
        storage.segmentRegistry().close();
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

    void closeWalRuntime() {
        walRuntime.close();
    }

    void closeAfterFailedInitialization() {
        RuntimeException failure = null;
        failure = closeResource(this::closeSplitRuntime, failure);
        failure = closeResource(this::closeSegmentRegistry, failure);
        failure = closeResource(this::closeKeyToSegmentMapIfOpen, failure);
        failure = closeResource(this::closeWalRuntime, failure);
        if (failure != null) {
            throw failure;
        }
    }

    void closeKeyToSegmentMapIfOpen() {
        if (!storage.keyToSegmentMap().wasClosed()) {
            storage.keyToSegmentMap().close();
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
