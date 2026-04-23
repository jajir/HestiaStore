package org.hestiastore.index.segmentindex.core.split;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.maintenance.SplitMaintenanceSynchronization;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.routing.SplitAdmissionAccess;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Builds the default split runtime and its local routing/maintenance adapters.
 */
public final class SplitRuntimeFactory {

    private SplitRuntimeFactory() {
    }

    /**
     * Creates the default split runtime service.
     *
     * @param conf index configuration
     * @param runtimeTuningState runtime tuning state
     * @param keyComparator key comparator
     * @param keyToSegmentMap route map
     * @param segmentRegistry segment registry
     * @param directoryFacade root segment directory
     * @param splitExecutor split executor
     * @param workerExecutor split-policy worker executor
     * @param splitPolicyScheduler split-policy scheduler
     * @param stateSupplier host runtime state supplier
     * @param failureHandler fatal split failure handler
     * @param stats split telemetry source
     * @param <K> key type
     * @param <V> value type
     * @return split service
     */
    public static <K, V> SplitService<K, V> create(
            final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final Comparator<K> keyComparator,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final Directory directoryFacade,
            final Executor splitExecutor, final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final Supplier<SegmentIndexState> stateSupplier,
            final Consumer<RuntimeException> failureHandler,
            final Stats stats) {
        return BackgroundSplitRuntimeAccess.create(
                Vldtn.requireNonNull(conf, "conf"),
                Vldtn.requireNonNull(runtimeTuningState,
                        "runtimeTuningState"),
                Vldtn.requireNonNull(keyComparator, "keyComparator"),
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                Vldtn.requireNonNull(directoryFacade, "directoryFacade"),
                Vldtn.requireNonNull(splitExecutor, "splitExecutor"),
                Vldtn.requireNonNull(workerExecutor, "workerExecutor"),
                Vldtn.requireNonNull(splitPolicyScheduler,
                        "splitPolicyScheduler"),
                SplitRuntimeLifecycle.from(Vldtn.requireNonNull(stateSupplier,
                        "stateSupplier")),
                SplitFailureReporter.from(Vldtn.requireNonNull(failureHandler,
                        "failureHandler")),
                SplitRuntimeTelemetry.from(Vldtn.requireNonNull(stats,
                        "stats")));
    }

    /**
     * Creates the routing-facing split admission adapter for the split
     * service.
     *
     * @param splitService split service instance created by this factory
     * @param <K> key type
     * @param <V> value type
     * @return routing-facing split admission adapter
     */
    public static <K, V> SplitAdmissionAccess<K, V> admissionAccess(
            final SplitService<K, V> splitService) {
        final BackgroundSplitRuntimeAccess<K, V> runtimeAccess = runtimeAccess(
                Vldtn.requireNonNull(splitService, "splitService"));
        return new SplitAdmissionAccess<>() {
            @Override
            public <T> T runWithSharedSplitAdmission(final Supplier<T> action) {
                return runtimeAccess.runWithSharedSplitAdmission(action);
            }

            @Override
            public boolean isSplitBlocked(final SegmentId segmentId) {
                return runtimeAccess.isSplitBlocked(segmentId);
            }
        };
    }

    /**
     * Creates the maintenance-facing synchronization adapter for the split
     * service.
     *
     * @param splitService split service instance created by this factory
     * @param <K> key type
     * @param <V> value type
     * @return maintenance-facing synchronization adapter
     */
    public static <K, V> SplitMaintenanceSynchronization<K, V> maintenanceSynchronization(
            final SplitService<K, V> splitService) {
        final BackgroundSplitRuntimeAccess<K, V> runtimeAccess = runtimeAccess(
                Vldtn.requireNonNull(splitService, "splitService"));
        return new SplitMaintenanceSynchronization<>() {
            @Override
            public void awaitIdle(final long timeoutMillis) {
                runtimeAccess.awaitSplitsIdle(timeoutMillis);
            }

            @Override
            public int splitInFlightCount() {
                return runtimeAccess.splitInFlightCount();
            }

            @Override
            public void scheduleScanIfIdle() {
                runtimeAccess.scheduleScanIfIdle();
            }

            @Override
            public void awaitExhausted() {
                runtimeAccess.awaitExhausted();
            }

            @Override
            public <T> T runWithSplitSchedulingPaused(
                    final Supplier<T> action) {
                return runtimeAccess.runWithSplitSchedulingPaused(action);
            }

            @Override
            public void runWithSplitSchedulingPaused(final Runnable action) {
                runtimeAccess.runWithSplitSchedulingPaused(action);
            }
        };
    }

    private static <K, V> BackgroundSplitRuntimeAccess<K, V> runtimeAccess(
            final SplitService<K, V> splitService) {
        if (splitService instanceof BackgroundSplitRuntimeAccess<?, ?> access) {
            @SuppressWarnings("unchecked")
            final BackgroundSplitRuntimeAccess<K, V> typedAccess =
                    (BackgroundSplitRuntimeAccess<K, V>) access;
            return typedAccess;
        }
        throw new IllegalArgumentException(
                "Split service does not expose default runtime access.");
    }
}
