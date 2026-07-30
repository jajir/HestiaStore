package org.hestiastore.index.segmentindex.core.split;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.SegmentIndexRuntimeState;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLeaseService;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Aggregates split execution and policy scheduling inside one managed runtime
 * boundary.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SplitRuntime<K, V>
        implements AutoCloseable, Runnable {

    private final SplitTaskCoordinator<K, V> splitCoordinator;
    private final SplitPolicyScheduler<K, V> splitPolicyCoordinator;
    private final SplitStatsRecorder statsRecorder;
    private final AtomicReference<State> state = new AtomicReference<>(
            State.RUNNING);

    SplitRuntime(
            final SplitTaskCoordinator<K, V> splitCoordinator,
            final SplitPolicyScheduler<K, V> splitPolicyCoordinator,
            final SplitStatsRecorder statsRecorder) {
        this.splitCoordinator = Vldtn.requireNonNull(splitCoordinator,
                "splitCoordinator");
        this.splitPolicyCoordinator = Vldtn.requireNonNull(
                splitPolicyCoordinator, "splitPolicyCoordinator");
        this.statsRecorder = Vldtn.requireNonNull(statsRecorder,
                "statsRecorder");
    }

    /**
     * Creates the default split runtime service.
     *
     * @param conf effective index configuration
     * @param runtimeTuningState runtime tuning state
     * @param keyToSegmentMap key-to-segment map
     * @param segmentLeaseService segment lease service
     * @param segmentRegistry segment registry
     * @param directoryFacade index directory
     * @param splitExecutor split executor
     * @param workerExecutor split-policy worker executor
     * @param splitPolicyScheduler split-policy scheduler
     * @param runtimeState runtime state boundary
     * @param statsRecorder split stats recorder
     * @param <M> key type
     * @param <N> value type
     * @return split service
     */
    @SuppressWarnings("java:S107")
    public static <M, N> SplitRuntime<M, N> create(
            final EffectiveIndexConfiguration<M, N> conf,
            final RuntimeTuningState runtimeTuningState,
            final SegmentRouteMap<M> keyToSegmentMap,
            final MappedSegmentLeaseService<M, N> segmentLeaseService,
            final SegmentRegistry<M, N> segmentRegistry,
            final Directory directoryFacade,
            final Executor splitExecutor,
            final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final SegmentIndexRuntimeState runtimeState,
            final SplitStatsRecorder statsRecorder) {
        final SplitStatsRecorder validatedStatsRecorder = Vldtn.requireNonNull(
                statsRecorder, "statsRecorder");
        final EffectiveIndexConfiguration<M, N> validatedConf = Vldtn
                .requireNonNull(conf, "conf");
        final SegmentRegistry<M, N> validatedSegmentRegistry = Vldtn
                .requireNonNull(segmentRegistry, "segmentRegistry");
        final MappedSegmentLeaseService<M, N> validatedSegmentLeaseService = Vldtn
                .requireNonNull(segmentLeaseService, "segmentLeaseService");
        final SegmentRouteMap<M> validatedKeyToSegmentMap = Vldtn
                .requireNonNull(keyToSegmentMap, "keyToSegmentMap");
        final SegmentIndexRuntimeState validatedRuntimeState = Vldtn
                .requireNonNull(runtimeState, "runtimeState");
        final PreparedSegmentMaterializer<M, N> materializationService =
                new PreparedSegmentMaterializer<>(
                        Vldtn.requireNonNull(directoryFacade,
                                "directoryFacade"),
                        validatedSegmentRegistry.materialization());
        final BusyRetryPolicy retryPolicy = new BusyRetryPolicy(
                validatedConf.maintenance().busyBackoffMillis(),
                validatedConf.maintenance().busyTimeoutMillis(),
                "Split operation");
        final RouteSplitMaterializer<M, N> preparationService =
                new RouteSplitMaterializer<>(materializationService,
                        retryPolicy);
        final RouteSplitPlanner<M, N> routeSplitCoordinator =
                new RouteSplitPlanner<>(preparationService);
        final RouteSplitPublisher<M, N> routeSplitPublishCoordinator =
                new RouteSplitPublisher<>(
                        validatedKeyToSegmentMap,
                        validatedSegmentRegistry, materializationService);
        final SplitTaskCoordinator<M, N> splitCoordinator =
                new SplitTaskCoordinator<>(validatedKeyToSegmentMap,
                        validatedSegmentLeaseService,
                        routeSplitCoordinator, routeSplitPublishCoordinator,
                        Vldtn.requireNonNull(splitExecutor, "splitExecutor"),
                        validatedRuntimeState, validatedStatsRecorder,
                        System::nanoTime);
        final SplitPolicyScheduler<M, N> splitPolicyCoordinator =
                new SplitPolicyScheduler<>(validatedConf,
                        Vldtn.requireNonNull(runtimeTuningState,
                                "runtimeTuningState"),
                        validatedKeyToSegmentMap, validatedSegmentLeaseService,
                        splitCoordinator,
                        Vldtn.requireNonNull(workerExecutor, "workerExecutor"),
                        Vldtn.requireNonNull(splitPolicyScheduler,
                                "splitPolicyScheduler"),
                        validatedRuntimeState, validatedStatsRecorder,
                        new SplitWorkerState(), new SplitCandidateQueue());
        return new SplitRuntime<>(splitCoordinator, splitPolicyCoordinator,
                validatedStatsRecorder);
    }

    boolean scheduleEligibleSplit(
            final SegmentId segmentId,
            final long splitThreshold, final long observedKeyCount) {
        return splitCoordinator.scheduleEligibleSplit(segmentId,
                splitThreshold, observedKeyCount);
    }

    public boolean isSplitBlocked(final SegmentId segmentId) {
        return splitCoordinator.isSplitBlocked(segmentId);
    }

    int splitBlockedCount() {
        return splitCoordinator.splitBlockedCount();
    }

    /**
     * Hints that the provided mapped segment may now be eligible for split.
     *
     * @param segmentId mapped segment id
     */
    public void hintSplitCandidate(final SegmentId segmentId) {
        requireRunning("accept split candidate hints");
        splitPolicyCoordinator.hintSplitCandidate(segmentId);
    }

    /**
     * Closes the managed split runtime. Shared executors are owned by the
     * surrounding runtime and are not shut down by this call.
     */
    @Override
    public void close() {
        if (!beginClose()) {
            return;
        }
        try {
            splitPolicyCoordinator.awaitQuiescence();
        } finally {
            markClosed();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
        requestFullSplitScan();
    }

    /**
     * Requests a full split-policy scan regardless of current in-flight split
     * state.
     */
    public void requestFullSplitScan() {
        splitPolicyCoordinator.requestFullSplitScan();
    }

    /**
     * Waits until split-policy work and in-flight splits are quiescent.
     */
    public void awaitQuiescence() {
        splitPolicyCoordinator.awaitQuiescence();
    }

    public SplitStats statsSnapshot() {
        return statsRecorder.statsSnapshot(splitCoordinator.splitInFlightCount(),
                splitCoordinator.splitBlockedCount());
    }

    private void requireRunning(final String action) {
        final State current = state.get();
        if (current != State.RUNNING) {
            throw new IllegalStateException(String.format(
                    "Split runtime cannot %s while %s.", action, current));
        }
    }

    private boolean beginClose() {
        while (true) {
            final State current = state.get();
            if (current == State.CLOSED || current == State.CLOSING) {
                return false;
            }
            if (state.compareAndSet(State.RUNNING, State.CLOSING)) {
                return true;
            }
        }
    }

    private void markClosed() {
        while (true) {
            final State current = state.get();
            if (current == State.CLOSED) {
                return;
            }
            if (current != State.CLOSING) {
                throw new IllegalStateException(String.format(
                        "Split runtime cannot become CLOSED from %s.",
                        current));
            }
            if (state.compareAndSet(State.CLOSING, State.CLOSED)) {
                return;
            }
        }
    }

    private enum State {
        RUNNING,
        CLOSING,
        CLOSED
    }

}
