package org.hestiastore.index.segmentindex.core.session;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.control.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.core.maintenance.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.SegmentIndexMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.routing.SegmentIndexDataAccess;
import org.hestiastore.index.segmentindex.core.routing.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.routing.SegmentIndexRuntimeSplits;
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
        implements SegmentIndexDataAccess<K, V>,
        SegmentIndexMaintenanceAccess<K, V> {

    public static <K, V> SegmentIndexRuntime<K, V> create(
            final Logger logger,
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexRuntimeConfiguration<K, V> runtimeConfiguration,
            final IndexExecutorRegistry executorRegistry, final Stats stats,
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
    private final SegmentIndexRuntimeSplits<K, V> splits;
    private final SegmentIndexRuntimeServices<K, V> services;
    private final WalRuntime<K, V> walRuntime;

    SegmentIndexRuntime(final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentIndexRuntimeSplits<K, V> splits,
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
        this.splits = Vldtn.requireNonNull(splits, "splits");
        this.services = Vldtn.requireNonNull(services, "services");
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
    }

    public void recoverFromWal() {
        services.walCoordinator().recover(services.operationAccess()::replayWalRecord);
    }

    public void cleanupOrphanedSegmentDirectories() {
        splits.recoveryCleanupCoordinator().cleanupOrphanedSegmentDirectories();
    }

    public boolean hasSegmentLockFile(final SegmentId segmentId) {
        return splits.recoveryCleanupCoordinator().hasSegmentLockFile(segmentId);
    }

    TypeDescriptor<K> keyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    SegmentIndexRuntimeStorage<K, V> storage() {
        return storage;
    }

    SegmentIndexRuntimeSplits<K, V> splits() {
        return splits;
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

    public IndexControlPlane controlPlane() {
        return services.controlPlane();
    }

    void applyRuntimeEffectiveLimits(
            final Map<RuntimeSettingKey, Integer> effective) {
        services.runtimeLimitApplier().apply(effective);
    }

    @Override
    public void invalidateSegmentIterators() {
        splits.stableSegmentCoordinator().invalidateIterators();
    }

    @Override
    public void awaitSplitsIdle(final long timeoutMillis) {
        splits.backgroundSplitCoordinator().awaitSplitsIdle(timeoutMillis);
    }

    public void requestSplitPlannerRescan() {
        splits.splitPlanner().requestRescan();
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

    public void awaitSplitPlannerExhausted() {
        splits.splitPlanner().awaitExhausted();
    }

    public void flushStableSegmentsWithSplitSchedulingPaused() {
        splits.backgroundSplitCoordinator().runWithSplitSchedulingPaused(
                () -> splits.stableSegmentCoordinator().flushSegments(true));
    }

    public void closeSegmentRegistry() {
        storage.segmentRegistry().close();
    }

    public void flushKeyToSegmentMap() {
        storage.keyToSegmentMap().flushIfDirty();
    }

    public void checkpointWal() {
        services.walCoordinator().checkpoint();
    }

    @Override
    public EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        return splits.stableSegmentCoordinator().openIteratorWithRetry(segmentId,
                isolation);
    }

    @Override
    public EntryIterator<K, V> openWindowIterator(
            final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        return splits.directSegmentCoordinator()
                .openWindowIterator(segmentWindow, isolation);
    }

    @Override
    public void compact() {
        services.maintenanceAccess().compact();
    }

    @Override
    public void compactAndWait() {
        services.maintenanceAccess().compactAndWait();
    }

    @Override
    public void flush() {
        services.maintenanceAccess().flush();
    }

    @Override
    public void flushAndWait() {
        services.maintenanceAccess().flushAndWait();
    }

    public void closeWalRuntime() {
        walRuntime.close();
    }
}
