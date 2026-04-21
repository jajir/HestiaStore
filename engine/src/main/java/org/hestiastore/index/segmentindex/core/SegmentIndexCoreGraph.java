package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.consistency.IndexConsistencyCoordinator;
import org.hestiastore.index.segmentindex.core.consistency.SegmentIndexConsistencyAccess;
import org.hestiastore.index.segmentindex.core.facade.SegmentIndexFacades;
import org.hestiastore.index.segmentindex.core.facade.SegmentIndexTrackedOperationRunner;
import org.hestiastore.index.segmentindex.core.facade.SegmentIndexMaintenanceCommands;
import org.hestiastore.index.segmentindex.core.facade.SegmentIndexMutationFacade;
import org.hestiastore.index.segmentindex.core.facade.SegmentIndexReadFacade;
import org.hestiastore.index.segmentindex.core.lifecycle.IndexCloseCoordinator;
import org.hestiastore.index.segmentindex.core.lifecycle.SegmentIndexStartupCoordinator;
import org.hestiastore.index.segmentindex.core.maintenance.SegmentIndexMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.observability.Stats;
import org.hestiastore.index.segmentindex.core.operation.IndexOperationTrackingAccess;
import org.hestiastore.index.segmentindex.core.runtime.SegmentIndexRuntime;

/**
 * Groups the core collaborators assembled for one {@link SegmentIndexImpl}.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexCoreGraph<K, V> {

    private final SegmentIndexRuntime<K, V> runtime;
    private final SegmentIndexMutationFacade<K, V> mutationFacade;
    private final SegmentIndexReadFacade<K, V> readFacade;
    private final SegmentIndexMaintenanceCommands<K, V> maintenanceCommands;
    private final SegmentIndexMaintenanceAccess<K, V> maintenanceAccess;
    private final IndexCloseCoordinator<K, V> closeCoordinator;
    private final SegmentIndexStartupCoordinator<K, V> startupCoordinator;

    private SegmentIndexCoreGraph(final SegmentIndexRuntime<K, V> runtime,
            final SegmentIndexMutationFacade<K, V> mutationFacade,
            final SegmentIndexReadFacade<K, V> readFacade,
            final SegmentIndexMaintenanceCommands<K, V> maintenanceCommands,
            final SegmentIndexMaintenanceAccess<K, V> maintenanceAccess,
            final IndexCloseCoordinator<K, V> closeCoordinator,
            final SegmentIndexStartupCoordinator<K, V> startupCoordinator) {
        this.runtime = Vldtn.requireNonNull(runtime, "runtime");
        this.mutationFacade = Vldtn.requireNonNull(mutationFacade,
                "mutationFacade");
        this.readFacade = Vldtn.requireNonNull(readFacade, "readFacade");
        this.maintenanceCommands = Vldtn.requireNonNull(maintenanceCommands,
                "maintenanceCommands");
        this.maintenanceAccess = Vldtn.requireNonNull(maintenanceAccess,
                "maintenanceAccess");
        this.closeCoordinator = Vldtn.requireNonNull(closeCoordinator,
                "closeCoordinator");
        this.startupCoordinator = Vldtn.requireNonNull(startupCoordinator,
                "startupCoordinator");
    }

    static <K, V> SegmentIndexCoreGraph<K, V> create(
            final SegmentIndexCoreInputs<K, V> request) {
        final SegmentIndexCoreInputs<K, V> validatedRequest = Vldtn
                .requireNonNull(request, "request");
        final Stats stats = new Stats();
        final IndexOperationTrackingAccess operationTracker =
                IndexOperationTrackingAccess.create();
        final SegmentIndexRuntime<K, V> runtime = createRuntime(
                validatedRequest, stats);
        final IndexConsistencyCoordinator<K, V> consistencyCoordinator =
                createConsistencyCoordinator(runtime);
        final SegmentIndexFacades<K, V> facades = createFacades(
                validatedRequest, operationTracker, runtime,
                consistencyCoordinator);
        return new SegmentIndexCoreGraph<>(runtime,
                facades.mutationFacade(), facades.readFacade(),
                facades.maintenanceCommands(), runtime,
                createCloseCoordinator(validatedRequest, operationTracker,
                        stats, runtime),
                createStartupCoordinator(validatedRequest, runtime,
                        consistencyCoordinator));
    }

    SegmentIndexRuntime<K, V> runtime() {
        return runtime;
    }

    SegmentIndexMutationFacade<K, V> mutationFacade() {
        return mutationFacade;
    }

    SegmentIndexReadFacade<K, V> readFacade() {
        return readFacade;
    }

    SegmentIndexMaintenanceCommands<K, V> maintenanceCommands() {
        return maintenanceCommands;
    }

    SegmentIndexMaintenanceAccess<K, V> maintenanceAccess() {
        return maintenanceAccess;
    }

    IndexCloseCoordinator<K, V> closeCoordinator() {
        return closeCoordinator;
    }

    SegmentIndexStartupCoordinator<K, V> startupCoordinator() {
        return startupCoordinator;
    }

    private static <K, V> IndexCloseCoordinator<K, V> createCloseCoordinator(
            final SegmentIndexCoreInputs<K, V> request,
            final IndexOperationTrackingAccess operationTracker,
            final Stats stats, final SegmentIndexRuntime<K, V> runtime) {
        return new IndexCloseCoordinator<>(request.logger,
                request.conf.getIndexName(),
                request.stateCoordinator,
                Vldtn.requireNonNull(operationTracker, "operationTracker"),
                Vldtn.requireNonNull(stats, "stats"),
                Vldtn.requireNonNull(runtime, "runtime"));
    }

    private static <K, V> SegmentIndexStartupCoordinator<K, V> createStartupCoordinator(
            final SegmentIndexCoreInputs<K, V> request,
            final SegmentIndexRuntime<K, V> runtime,
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator) {
        return new SegmentIndexStartupCoordinator<>(request.logger,
                request.conf.getIndexName(),
                request.staleLockRecovered,
                Vldtn.requireNonNull(runtime, "runtime"),
                request.stateCoordinator,
                Vldtn.requireNonNull(consistencyCoordinator,
                        "consistencyCoordinator"));
    }

    private static <K, V> SegmentIndexRuntime<K, V> createRuntime(
            final SegmentIndexCoreInputs<K, V> request,
            final Stats stats) {
        return SegmentIndexRuntime.create(request.logger,
                request.directoryFacade, request.keyTypeDescriptor,
                request.valueTypeDescriptor, request.conf,
                request.runtimeConfiguration, request.executorRegistry,
                Vldtn.requireNonNull(stats, "stats"),
                request.stateCoordinator::getState,
                request.stateCoordinator::failWithError);
    }

    private static <K, V> IndexConsistencyCoordinator<K, V> createConsistencyCoordinator(
            final SegmentIndexConsistencyAccess<K, V> runtime) {
        final SegmentIndexConsistencyAccess<K, V> validatedRuntime = Vldtn
                .requireNonNull(runtime, "runtime");
        return new IndexConsistencyCoordinator<>(
                validatedRuntime::validateUniqueSegmentIds,
                validatedRuntime::checkAndRepairConsistency,
                validatedRuntime::cleanupOrphanedSegmentDirectories,
                validatedRuntime::scheduleBackgroundSplitScan,
                validatedRuntime::hasSegmentLockFile);
    }

    private static <K, V> SegmentIndexFacades<K, V> createFacades(
            final SegmentIndexCoreInputs<K, V> request,
            final IndexOperationTrackingAccess operationTracker,
            final SegmentIndexRuntime<K, V> runtime,
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator) {
        return SegmentIndexFacades.create(request.conf,
                new SegmentIndexTrackedOperationRunner<>(
                        request.stateCoordinator::getIndexState,
                        Vldtn.requireNonNull(operationTracker,
                                "operationTracker")),
                Vldtn.requireNonNull(runtime, "runtime"), runtime,
                Vldtn.requireNonNull(consistencyCoordinator,
                        "consistencyCoordinator"));
    }
}
