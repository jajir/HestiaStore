package org.hestiastore.index.segmentindex.core.session;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfiguration;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.ResolvedIndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.control.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.metrics.Stats;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.storage.IndexConsistencyChecker;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexRuntimeStorage;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.slf4j.Logger;

/**
 * Aggregates long-lived collaborators created for one running index session.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexRuntime<K, V>
        implements SegmentIndexDataAccess<K, V> {

    public static <K, V> SegmentIndexRuntime<K, V> create(
            final Logger logger,
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final ResolvedIndexConfiguration<K, V> runtimeConfiguration,
            final ExecutorRegistry executorRegistry, final Stats stats,
            final Supplier<SegmentIndexState> stateSupplier,
            final Consumer<RuntimeException> failureHandler) {
        return new SegmentIndexRuntimeGraphBuilder<>(
                new SegmentIndexRuntimeInputs<>(logger,
                        directoryFacade, keyTypeDescriptor,
                        valueTypeDescriptor, conf, runtimeConfiguration,
                        executorRegistry, stats,
                        new java.util.concurrent.atomic.AtomicLong(),
                        new java.util.concurrent.atomic.AtomicLong(),
                        new java.util.concurrent.atomic.AtomicLong(),
                        stateSupplier, failureHandler))
                                .build();
    }

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final SegmentIndexRuntimeStorage<K, V> storage;
    private final SegmentTopologyRuntime<K, V> topologyRuntime;
    private final SegmentIndexRuntimeServices<K, V> services;
    private final WalRuntime<K, V> walRuntime;

    SegmentIndexRuntime(final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentTopologyRuntime<K, V> topologyRuntime,
            final WalRuntime<K, V> walRuntime,
            final SegmentIndexRuntimeServices<K, V> services) {
        final SegmentIndexCoreStorage<K, V> validatedCoreStorage = Vldtn
                .requireNonNull(coreStorage, "coreStorage");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.storage = new SegmentIndexRuntimeStorage<>(
                validatedCoreStorage.runtimeTuningState(),
                validatedCoreStorage.keyToSegmentMap(),
                validatedCoreStorage.segmentRegistry(),
                validatedCoreStorage.retryPolicy());
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
        this.services = Vldtn.requireNonNull(services, "services");
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
    }

    public void recoverFromWal() {
        services.walCoordinator().recover(
                services.operationAccess()::replayWalRecord);
    }

    public void cleanupOrphanedSegmentDirectories() {
        topologyRuntime.cleanupOrphanedSegmentDirectories();
    }

    public boolean hasSegmentLockFile(final SegmentId segmentId) {
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

    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return services.metricsSnapshotSupplier().get();
    }

    public IndexRuntimeMonitoring runtimeMonitoring() {
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

    public RuntimeConfiguration runtimeConfiguration() {
        return services.runtimeConfiguration();
    }

    void applyRuntimeEffectiveLimits(
            final Map<RuntimeSettingKey, Integer> effective) {
        services.runtimeLimitApplier().apply(effective);
    }

    public void invalidateSegmentIterators() {
        topologyRuntime.invalidateSegmentIterators();
    }

    public void requestFullSplitScan() {
        topologyRuntime.requestFullSplitScan();
    }

    public void validateUniqueSegmentIds() {
        storage.keyToSegmentMap().validateUniqueSegmentIds();
    }

    public void checkAndRepairConsistency(
            final Predicate<SegmentId> segmentFilter) {
        new IndexConsistencyChecker<>(storage.keyToSegmentMap(),
                storage.segmentRegistry(), keyTypeDescriptor, segmentFilter)
                        .checkAndRepairConsistency();
    }

    public void closeSplitRuntime() {
        topologyRuntime.closeSplitRuntime();
    }

    public void closeSegmentRegistry() {
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

    public void compact() {
        services.maintenance().compact();
    }

    public void compactAndWait() {
        services.maintenance().compactAndWait();
    }

    public void flush() {
        services.maintenance().flush();
    }

    public void flushAndWait() {
        services.maintenance().flushAndWait();
    }

    public void closeWalRuntime() {
        walRuntime.close();
    }

    void closeForFailedStartup(final RuntimeException failure) {
        RuntimeException cleanupFailure = closeForFailedStartup();
        if (cleanupFailure != null) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private RuntimeException closeForFailedStartup() {
        RuntimeException failure = null;
        failure = closeResource(this::closeSplitRuntime, failure);
        failure = closeResource(this::closeSegmentRegistry, failure);
        failure = closeResource(this::closeWalRuntime, failure);
        failure = closeResource(this::closeKeyToSegmentMapIfOpen, failure);
        return failure;
    }

    private void closeKeyToSegmentMapIfOpen() {
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
