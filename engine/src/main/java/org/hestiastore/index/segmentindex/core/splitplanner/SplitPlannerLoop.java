package org.hestiastore.index.segmentindex.core.splitplanner;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.routing.BackgroundSplitCoordinator;

/**
 * Dedicated planner event loop that owns split hints and periodic
 * reconciliation.
 */
@SuppressWarnings("java:S107")
final class SplitPlannerLoop<K, V> implements SplitPlanner<K, V> {

    private static final long RECONCILIATION_INTERVAL_MILLIS = 250L;
    private static final long SETTLE_POLL_INTERVAL_MILLIS = 10L;

    private final IndexConfiguration<K, V> conf;
    private final RuntimeTuningState runtimeTuningState;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final SplitTaskDispatcher<K, V> splitTaskDispatcher;
    private final Executor plannerExecutor;
    private final Supplier<SegmentIndexState> stateSupplier;
    private final Runnable awaitSplitsIdleAction;
    private final Consumer<RuntimeException> failureHandler;
    private final SplitPlannerState plannerState;

    SplitPlannerLoop(final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final SplitTaskDispatcher<K, V> splitTaskDispatcher,
            final Executor plannerExecutor,
            final Supplier<SegmentIndexState> stateSupplier,
            final Runnable awaitSplitsIdleAction,
            final Consumer<RuntimeException> failureHandler,
            final SplitPlannerState plannerState) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
        this.splitTaskDispatcher = Vldtn.requireNonNull(splitTaskDispatcher,
                "splitTaskDispatcher");
        this.plannerExecutor = Vldtn.requireNonNull(plannerExecutor,
                "plannerExecutor");
        this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                "stateSupplier");
        this.awaitSplitsIdleAction = Vldtn.requireNonNull(
                awaitSplitsIdleAction, "awaitSplitsIdleAction");
        this.failureHandler = Vldtn.requireNonNull(failureHandler,
                "failureHandler");
        this.plannerState = Vldtn.requireNonNull(plannerState,
                "plannerState");
        startPlannerLoop();
    }

    @Override
    public void hintSegment(final SegmentId segmentId) {
        if (!canHintSegment(segmentId)) {
            return;
        }
        plannerState.hintSegment(segmentId);
    }

    @Override
    public void requestRescan() {
        if (!isPlannerEnabled()) {
            return;
        }
        plannerState.requestRescan();
    }

    @Override
    public void scheduleIfIdle() {
        if (!isPlannerEnabled()
                || backgroundSplitCoordinator.splitInFlightCount() > 0) {
            return;
        }
        plannerState.requestRescan();
    }

    @Override
    public void awaitExhausted() {
        awaitSettled();
        if (!isPlannerEnabled()) {
            return;
        }
        plannerState.requestForceRetry();
        awaitSettled();
    }

    private void startPlannerLoop() {
        try {
            plannerExecutor.execute(this::runPlannerLoop);
        } catch (final RuntimeException e) {
            if (!isClosedOrClosingState()) {
                throw e;
            }
        }
    }

    private void runPlannerLoop() {
        while (true) {
            final SplitPlannerState.PlannerCycle cycle = plannerState
                    .awaitNextCycle(RECONCILIATION_INTERVAL_MILLIS,
                            this::isClosedOrClosingState);
            if (cycle.isShutdownCycle()) {
                return;
            }
            try {
                if (!isPlannerEnabled()) {
                    plannerState.clearPendingWork();
                    continue;
                }
                dispatchHintedCandidates(cycle);
                if (shouldRunFullRescan(cycle)) {
                    splitTaskDispatcher.dispatchAllMappedCandidates(
                            splitThreshold(), cycle.forceRetryRequested());
                }
            } catch (final RuntimeException e) {
                if (!isClosedOrClosingState()) {
                    failureHandler.accept(e);
                }
                return;
            } finally {
                plannerState.finishCycle();
            }
        }
    }

    private void dispatchHintedCandidates(
            final SplitPlannerState.PlannerCycle cycle) {
        final int splitThreshold = splitThreshold();
        if (!isEnabled(splitThreshold)) {
            return;
        }
        if (cycle.hintedSegments().isEmpty()) {
            return;
        }
        splitTaskDispatcher.dispatchCandidates(cycle.hintedSegments(),
                splitThreshold, cycle.forceRetryRequested());
    }

    private boolean shouldRunFullRescan(
            final SplitPlannerState.PlannerCycle cycle) {
        if (!isEnabled(splitThreshold())) {
            return false;
        }
        if (cycle.forceRetryRequested() || cycle.rescanRequested()) {
            return true;
        }
        return cycle.periodicTick()
                && backgroundSplitCoordinator.splitInFlightCount() == 0;
    }

    private void awaitSettled() {
        final long timeoutMillis = conf.getIndexBusyTimeoutMillis();
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            awaitSplitsIdleAction.run();
            if (isSettled()) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                throw new IndexException(String.format(
                        "Split planner completion timed out after %d ms.",
                        timeoutMillis));
            }
            LockSupport.parkNanos(
                    TimeUnit.MILLISECONDS.toNanos(SETTLE_POLL_INTERVAL_MILLIS));
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                throw new IndexException(
                        "Interrupted while waiting for split planner completion.");
            }
        }
    }

    private boolean isSettled() {
        if (!isPlannerEnabled()) {
            plannerState.clearPendingWork();
            return plannerState.isSettled()
                    && backgroundSplitCoordinator.splitInFlightCount() == 0;
        }
        return plannerState.isSettled()
                && backgroundSplitCoordinator.splitInFlightCount() == 0;
    }

    private boolean canHintSegment(final SegmentId segmentId) {
        return segmentId != null && isPlannerEnabled()
                && splitTaskDispatcher.isCandidateMapped(segmentId);
    }

    private int splitThreshold() {
        return runtimeTuningState.effectiveValue(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT);
    }

    private boolean isEnabled(final int threshold) {
        return threshold >= 1;
    }

    private boolean isPlannerEnabled() {
        return Boolean.TRUE.equals(conf.isBackgroundMaintenanceAutoEnabled())
                && !isClosedOrClosingState();
    }

    private boolean isClosedOrClosingState() {
        final SegmentIndexState state = stateSupplier.get();
        return state == SegmentIndexState.CLOSING
                || state == SegmentIndexState.CLOSED
                || state == SegmentIndexState.ERROR;
    }
}
