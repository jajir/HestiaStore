package org.hestiastore.index.segmentindex.core.runtime;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.consistency.SegmentIndexConsistencyAccess;
import org.hestiastore.index.segmentindex.core.consistency.IndexConsistencyChecker;
import org.hestiastore.index.segmentindex.core.facade.SegmentIndexDataAccess;
import org.hestiastore.index.segmentindex.core.lifecycle.IndexCloseAccess;
import org.hestiastore.index.segmentindex.core.lifecycle.SegmentIndexStartupAccess;
import org.hestiastore.index.segmentindex.core.maintenance.SegmentIndexMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.observability.Stats;
import org.hestiastore.index.segmentindex.core.infrastructure.IndexExecutorRegistry;
import org.slf4j.Logger;

/**
 * Aggregates runtime collaborators created for a running index instance.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexRuntime<K, V>
        implements IndexCloseAccess<K, V>, SegmentIndexStartupAccess<K, V>,
        SegmentIndexConsistencyAccess<K, V>, SegmentIndexDataAccess<K, V>,
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

    private final SegmentIndexRuntimeState<K, V> state;

    SegmentIndexRuntime(final SegmentIndexRuntimeState<K, V> state) {
        this.state = Vldtn.requireNonNull(state, "state");
    }

    @Override
    public void recoverFromWal() {
        state.services().walCoordinator().recover(
                state.services().operationAccess()::replayWalRecord);
    }

    @Override
    public void cleanupOrphanedSegmentDirectories() {
        state.splits().recoveryCleanupCoordinator()
                .cleanupOrphanedSegmentDirectories();
    }

    @Override
    public boolean hasSegmentLockFile(final SegmentId segmentId) {
        return state.splits().recoveryCleanupCoordinator()
                .hasSegmentLockFile(segmentId);
    }

    SegmentIndexRuntimeState<K, V> state() {
        return state;
    }

    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return state.services().metricsSnapshotSupplier().get();
    }

    @Override
    public void put(final K key, final V value) {
        state.services().operationAccess().put(key, value);
    }

    @Override
    public V get(final K key) {
        return state.services().operationAccess().get(key);
    }

    @Override
    public void delete(final K key) {
        state.services().operationAccess().delete(key);
    }

    public IndexControlPlane controlPlane() {
        return state.services().controlPlane();
    }

    void applyRuntimeEffectiveLimits(
            final Map<RuntimeSettingKey, Integer> effective) {
        state.services().runtimeLimitApplier().apply(effective);
    }

    @Override
    public void invalidateSegmentIterators() {
        state.splits().stableSegmentCoordinator().invalidateIterators();
    }

    @Override
    public void awaitSplitsIdle(final long timeoutMillis) {
        state.splits().backgroundSplitCoordinator()
                .awaitSplitsIdle(timeoutMillis);
    }

    public void scheduleBackgroundSplitScan() {
        state.splits().backgroundSplitPolicyLoop().scheduleScan();
    }

    @Override
    public void validateUniqueSegmentIds() {
        state.storage().keyToSegmentMap().validateUniqueSegmentIds();
    }

    @Override
    public void checkAndRepairConsistency(
            final Predicate<SegmentId> segmentFilter) {
        new IndexConsistencyChecker<>(
                state.storage().keyToSegmentMap(),
                state.storage().segmentRegistry(), state.keyTypeDescriptor(),
                segmentFilter).checkAndRepairConsistency();
    }

    @Override
    public void awaitBackgroundSplitsExhausted() {
        state.splits().backgroundSplitPolicyLoop().awaitExhausted();
    }

    @Override
    public void flushStableSegmentsWithSplitSchedulingPaused() {
        state.splits().backgroundSplitCoordinator()
                .runWithSplitSchedulingPaused(() -> state.splits()
                        .stableSegmentCoordinator().flushSegments(true));
    }

    @Override
    public void closeSegmentRegistry() {
        state.storage().segmentRegistry().close();
    }

    @Override
    public void flushKeyToSegmentMap() {
        state.storage().keyToSegmentMap().flushIfDirty();
    }

    @Override
    public void checkpointWal() {
        state.services().walCoordinator().checkpoint();
    }

    @Override
    public EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        return state.splits().stableSegmentCoordinator()
                .openIteratorWithRetry(segmentId, isolation);
    }

    @Override
    public EntryIterator<K, V> openWindowIterator(
            final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        return state.splits().directSegmentCoordinator()
                .openWindowIterator(segmentWindow, isolation);
    }

    @Override
    public void compact() {
        state.services().maintenanceAccess().compact();
    }

    @Override
    public void compactAndWait() {
        state.services().maintenanceAccess().compactAndWait();
    }

    @Override
    public void flush() {
        state.services().maintenanceAccess().flush();
    }

    @Override
    public void flushAndWait() {
        state.services().maintenanceAccess().flushAndWait();
    }

    @Override
    public void closeWalRuntime() {
        state.walRuntime().close();
    }
}
