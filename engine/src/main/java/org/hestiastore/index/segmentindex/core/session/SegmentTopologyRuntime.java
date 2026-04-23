package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.maintenance.SegmentIndexMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.maintenance.SplitMaintenanceSynchronization;
import org.hestiastore.index.segmentindex.core.maintenance.StableSegmentMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.split.SplitRuntimeFactory;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.routing.DirectSegmentAccess;
import org.hestiastore.index.segmentindex.core.routing.SplitAdmissionAccess;
import org.hestiastore.index.segmentindex.core.routing.StableSegmentAccess;
import org.hestiastore.index.segmentindex.core.storage.IndexRecoveryCleanupCoordinator;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;

/**
 * Owns the segment-topology subsystem that coordinates direct access, stable
 * segment maintenance, background split policy, and recovery cleanup.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentTopologyRuntime<K, V> {

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final long indexBusyTimeoutMillis;
    private final SplitService<K, V> splitService;
    private final SplitMaintenanceSynchronization<K, V> splitSynchronization;
    private final StableSegmentMaintenanceAccess<K, V> stableSegmentMaintenance;
    private final DirectSegmentAccess<K, V> directSegmentAccess;
    private final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator;

    SegmentTopologyRuntime(final SegmentIndexRuntimeInputs<K, V> request,
            final SegmentIndexCoreStorage<K, V> coreStorage) {
        final SegmentIndexRuntimeInputs<K, V> validatedRequest = Vldtn
                .requireNonNull(request, "request");
        final SegmentIndexCoreStorage<K, V> validatedCoreStorage = Vldtn
                .requireNonNull(coreStorage, "coreStorage");
        this.keyToSegmentMap = validatedCoreStorage.keyToSegmentMap();
        this.indexBusyTimeoutMillis = validatedRequest.conf
                .getIndexBusyTimeoutMillis();
        final StableSegmentAccess<K, V> stableSegmentGateway =
                StableSegmentAccess.create(validatedCoreStorage.keyToSegmentMap(),
                        validatedCoreStorage.segmentRegistry());
        this.splitService = SplitRuntimeFactory.create(
                validatedRequest.conf,
                validatedCoreStorage.runtimeTuningState(),
                validatedRequest.keyTypeDescriptor.getComparator(),
                validatedCoreStorage.keyToSegmentMap(),
                validatedCoreStorage.segmentRegistry(),
                validatedRequest.directoryFacade,
                validatedRequest.executorRegistry.getSplitMaintenanceExecutor(),
                validatedRequest.executorRegistry.getIndexMaintenanceExecutor(),
                validatedRequest.executorRegistry.getSplitPolicyScheduler(),
                validatedRequest.stateSupplier,
                validatedRequest.failureHandler, validatedRequest.stats);
        this.splitSynchronization = SplitRuntimeFactory
                .maintenanceSynchronization(splitService);
        final SplitAdmissionAccess<K, V> splitAdmissionAccess =
                SplitRuntimeFactory
                .admissionAccess(splitService);
        this.stableSegmentMaintenance = StableSegmentMaintenanceAccess.create(
                validatedRequest.logger, validatedCoreStorage.keyToSegmentMap(),
                validatedCoreStorage.segmentRegistry(),
                splitSynchronization, stableSegmentGateway,
                validatedCoreStorage.retryPolicy(), validatedRequest.stats);
        this.directSegmentAccess = DirectSegmentAccess.create(
                validatedCoreStorage.keyToSegmentMap(),
                validatedCoreStorage.segmentRegistry(), stableSegmentGateway,
                splitAdmissionAccess, validatedCoreStorage.retryPolicy());
        this.recoveryCleanupCoordinator = new IndexRecoveryCleanupCoordinator<>(
                validatedRequest.logger, validatedRequest.directoryFacade,
                validatedCoreStorage.keyToSegmentMap(),
                validatedCoreStorage.segmentRegistry(),
                validatedCoreStorage.retryPolicy());
    }

    SegmentIndexMaintenanceAccess<K, V> maintenanceAccess(
            final IndexWalCoordinator<K, V> walCoordinator) {
        return SegmentIndexMaintenanceAccess.create(keyToSegmentMap,
                splitSynchronization, stableSegmentMaintenance,
                Vldtn.requireNonNull(walCoordinator, "walCoordinator"));
    }

    SplitService<K, V> splitService() {
        return splitService;
    }

    DirectSegmentAccess<K, V> directSegmentAccess() {
        return directSegmentAccess;
    }

    void cleanupOrphanedSegmentDirectories() {
        recoveryCleanupCoordinator.cleanupOrphanedSegmentDirectories();
    }

    boolean hasSegmentLockFile(final SegmentId segmentId) {
        return recoveryCleanupCoordinator.hasSegmentLockFile(segmentId);
    }

    void invalidateSegmentIterators() {
        stableSegmentMaintenance.invalidateIterators();
    }

    void awaitSplitsIdle(final long timeoutMillis) {
        splitService.awaitIdle(timeoutMillis);
    }

    void scheduleBackgroundSplitScan() {
        splitService.requestFullScan();
    }

    void awaitBackgroundSplitsExhausted() {
        splitService.awaitIdle(indexBusyTimeoutMillis);
    }

    void flushStableSegmentsWithSplitSchedulingPaused() {
        splitSynchronization.runWithSplitSchedulingPaused(
                () -> stableSegmentMaintenance.flushSegments(true));
    }

    EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        return stableSegmentMaintenance.openIteratorWithRetry(segmentId,
                isolation);
    }

    EntryIterator<K, V> openWindowIterator(
            final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        return directSegmentAccess.openWindowIterator(segmentWindow, isolation);
    }
}
