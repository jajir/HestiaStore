package org.hestiastore.index.segmentindex.core.routing;

import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.maintenance.StableSegmentMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexRuntimeInputs;
import org.hestiastore.index.segmentindex.core.splitplanner.SplitPlanner;
import org.hestiastore.index.segmentindex.core.splitplanner.SplitTaskDispatcher;
import org.hestiastore.index.segmentindex.core.storage.IndexRecoveryCleanupCoordinator;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;

/**
 * Builds split-related runtime collaborators on top of already opened core
 * storage.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexSplitInfrastructureFactory<K, V> {

    private final SegmentIndexRuntimeInputs<K, V> request;
    private final SegmentIndexCoreStorage<K, V> coreStorage;

    public SegmentIndexSplitInfrastructureFactory(
            final SegmentIndexRuntimeInputs<K, V> request,
            final SegmentIndexCoreStorage<K, V> coreStorage) {
        this.request = Vldtn.requireNonNull(request, "request");
        this.coreStorage = Vldtn.requireNonNull(coreStorage, "coreStorage");
    }

    public SegmentIndexRuntimeSplits<K, V> create() {
        final AtomicReference<Runnable> splitAppliedListener =
                new AtomicReference<>(() -> {
                });
        final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator =
                newBackgroundSplitCoordinator(splitAppliedListener);
        final SplitPlanner<K, V> splitPlanner = newSplitPlanner(
                backgroundSplitCoordinator);
        splitAppliedListener.set(splitPlanner::scheduleIfIdle);
        final StableSegmentAccess<K, V> stableSegmentGateway = newStableSegmentGateway();
        final StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator = newStableSegmentCoordinator(
                backgroundSplitCoordinator, stableSegmentGateway);
        final DirectSegmentAccess<K, V> directSegmentCoordinator = newDirectSegmentCoordinator(
                stableSegmentGateway, backgroundSplitCoordinator);
        final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator = newRecoveryCleanupCoordinator();
        return new SegmentIndexRuntimeSplits<>(backgroundSplitCoordinator,
                stableSegmentCoordinator, splitPlanner,
                directSegmentCoordinator,
                recoveryCleanupCoordinator);
    }

    private BackgroundSplitCoordinator<K, V> newBackgroundSplitCoordinator(
            final AtomicReference<Runnable> splitAppliedListener) {
        return BackgroundSplitCoordinator.create(
                request.conf,
                request.keyTypeDescriptor.getComparator(),
                coreStorage.keyToSegmentMap(),
                coreStorage.segmentRegistry(),
                request.directoryFacade,
                request.executorRegistry.getSplitMaintenanceExecutor(),
                request.failureHandler,
                () -> Vldtn.requireNonNull(splitAppliedListener,
                        "splitAppliedListener").get().run(),
                request.stats::recordSplitTaskStartDelayNanos,
                request.stats::recordSplitTaskRunLatencyNanos);
    }

    private SplitPlanner<K, V> newSplitPlanner(
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator) {
        return SplitPlanner.create(
                request.conf, coreStorage.runtimeTuningState(),
                backgroundSplitCoordinator,
                new SplitTaskDispatcher<>(coreStorage.keyToSegmentMap(),
                        coreStorage.segmentRegistry(),
                        backgroundSplitCoordinator, request.stats),
                request.executorRegistry.getSplitPlannerExecutor(),
                request.stateSupplier,
                () -> backgroundSplitCoordinator.awaitSplitsIdle(
                        request.conf.getIndexBusyTimeoutMillis()),
                request.failureHandler);
    }

    private StableSegmentAccess<K, V> newStableSegmentGateway() {
        return StableSegmentAccess.create(coreStorage.keyToSegmentMap(),
                coreStorage.segmentRegistry());
    }

    private StableSegmentMaintenanceAccess<K, V> newStableSegmentCoordinator(
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final StableSegmentAccess<K, V> stableSegmentGateway) {
        return StableSegmentMaintenanceAccess.create(request.logger,
                coreStorage.keyToSegmentMap(), coreStorage.segmentRegistry(),
                backgroundSplitCoordinator, stableSegmentGateway,
                coreStorage.retryPolicy(), request.stats);
    }

    private DirectSegmentAccess<K, V> newDirectSegmentCoordinator(
            final StableSegmentAccess<K, V> stableSegmentGateway,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator) {
        return DirectSegmentAccess.create(coreStorage.keyToSegmentMap(),
                coreStorage.segmentRegistry(), stableSegmentGateway,
                backgroundSplitCoordinator, coreStorage.retryPolicy());
    }

    private IndexRecoveryCleanupCoordinator<K, V> newRecoveryCleanupCoordinator() {
        return new IndexRecoveryCleanupCoordinator<>(
                request.logger, request.directoryFacade,
                coreStorage.keyToSegmentMap(),
                coreStorage.segmentRegistry(), coreStorage.retryPolicy());
    }
}
