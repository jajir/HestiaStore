package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.maintenance.SegmentIndexMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.maintenance.StableSegmentMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.routing.DirectSegmentAccess;
import org.hestiastore.index.segmentindex.core.routing.StableSegmentAccess;
import org.hestiastore.index.segmentindex.core.storage.IndexRecoveryCleanupCoordinator;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;

/**
 * Owns the segment-topology subsystem that coordinates direct access, stable
 * segment maintenance, full split scans, and recovery cleanup.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentTopologyRuntime<K, V> {

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentTopology<K> segmentTopology;
    private final SplitService<K, V> splitService;
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
        this.segmentTopology = SegmentTopology.<K>builder()
                .snapshot(validatedCoreStorage.keyToSegmentMap().snapshot())
                .build();
        final StableSegmentAccess<K, V> stableSegmentGateway =
                StableSegmentAccess.create(
                        validatedCoreStorage.segmentRegistry());
        this.splitService = SplitService.<K, V>builder()
                .conf(validatedRequest.conf)
                .runtimeTuningState(validatedCoreStorage.runtimeTuningState())
                .keyComparator(
                        validatedRequest.keyTypeDescriptor.getComparator())
                .keyToSegmentMap(validatedCoreStorage.keyToSegmentMap())
                .segmentTopology(segmentTopology)
                .segmentRegistry(validatedCoreStorage.segmentRegistry())
                .directoryFacade(validatedRequest.directoryFacade)
                .splitExecutor(validatedRequest.executorRegistry
                        .getSplitMaintenanceExecutor())
                .workerExecutor(validatedRequest.executorRegistry
                        .getIndexMaintenanceExecutor())
                .splitPolicyScheduler(validatedRequest.executorRegistry
                        .getSplitPolicyScheduler())
                .stateSupplier(validatedRequest.stateSupplier)
                .failureHandler(validatedRequest.failureHandler)
                .stats(validatedRequest.stats)
                .build();
        this.stableSegmentMaintenance = StableSegmentMaintenanceAccess.create(
                validatedRequest.logger, validatedCoreStorage.keyToSegmentMap(),
                validatedCoreStorage.segmentRegistry(),
                stableSegmentGateway, validatedCoreStorage.retryPolicy(),
                validatedRequest.stats);
        this.directSegmentAccess = DirectSegmentAccess.create(
                validatedCoreStorage.keyToSegmentMap(),
                validatedCoreStorage.segmentRegistry(), stableSegmentGateway,
                segmentTopology, validatedCoreStorage.retryPolicy());
        this.recoveryCleanupCoordinator = new IndexRecoveryCleanupCoordinator<>(
                validatedRequest.logger, validatedRequest.directoryFacade,
                validatedCoreStorage.keyToSegmentMap(),
                validatedCoreStorage.segmentRegistry(),
                validatedCoreStorage.retryPolicy());
    }

    SegmentIndexMaintenanceAccess<K, V> maintenanceAccess(
            final IndexWalCoordinator<K, V> walCoordinator) {
        return SegmentIndexMaintenanceAccess.create(keyToSegmentMap,
                splitService, stableSegmentMaintenance,
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

    void requestFullSplitScan() {
        splitService.requestFullSplitScan();
    }

    void closeSplitRuntime() {
        splitService.close();
    }

    void flushStableSegments() {
        stableSegmentMaintenance.flushSegments(true);
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
