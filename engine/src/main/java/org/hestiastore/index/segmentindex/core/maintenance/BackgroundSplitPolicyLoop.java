package org.hestiastore.index.segmentindex.core.maintenance;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.Optional;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.observability.Stats;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.core.split.BackgroundSplitCoordinator;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Owns the autonomous background split-policy scan loop and its coordination
 * state.
 */
@SuppressWarnings("java:S107")
final class BackgroundSplitPolicyLoop<K, V>
        implements BackgroundSplitPolicyAccess<K, V> {

    private static final long AUTONOMOUS_SCHEDULER_INTERVAL_MILLIS = 250L;
    private static final long SETTLE_POLL_INTERVAL_MILLIS = 10L;

    private final IndexConfiguration<K, V> conf;
    private final RuntimeTuningState runtimeTuningState;
    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final Stats stats;
    private final Supplier<SegmentIndexState> stateSupplier;
    private final BackgroundSplitPolicyWorkState workState;
    private final Executor workerExecutor;
    private final ScheduledExecutorService splitPolicyScheduler;
    private final Consumer<RuntimeException> failureHandler;
    private final Runnable awaitSplitsIdleAction;

    static <K, V> BackgroundSplitPolicyLoop<K, V> create(
            final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final Stats stats,
            final Supplier<SegmentIndexState> stateSupplier,
            final Runnable awaitSplitsIdleAction,
            final Consumer<RuntimeException> failureHandler) {
        return new BackgroundSplitPolicyLoop<>(conf, runtimeTuningState,
                keyToSegmentMap, segmentRegistry, backgroundSplitCoordinator,
                workerExecutor, splitPolicyScheduler, stats, stateSupplier,
                awaitSplitsIdleAction, failureHandler,
                new BackgroundSplitPolicyWorkState());
    }

    BackgroundSplitPolicyLoop(
            final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final Stats stats,
            final Supplier<SegmentIndexState> stateSupplier,
            final Runnable awaitSplitsIdleAction,
            final Consumer<RuntimeException> failureHandler,
            final BackgroundSplitPolicyWorkState workState) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                "stateSupplier");
        this.workState = Vldtn.requireNonNull(workState, "workState");
        this.workerExecutor = Vldtn.requireNonNull(workerExecutor,
                "workerExecutor");
        this.splitPolicyScheduler = Vldtn.requireNonNull(splitPolicyScheduler,
                "splitPolicyScheduler");
        this.failureHandler = Vldtn.requireNonNull(failureHandler,
                "failureHandler");
        this.awaitSplitsIdleAction = Vldtn.requireNonNull(
                awaitSplitsIdleAction, "awaitSplitsIdleAction");
    }

    private void requestFullScan() {
        workState.markScanRequested();
        scheduleWorker();
    }

    @Override
    public void scheduleScan() {
        if (!isPolicyEnabled()) {
            return;
        }
        ensureAutonomousSchedulerScheduled();
        requestFullScan();
    }

    @Override
    public void scheduleScanIfIdle() {
        if (!isPolicyEnabled()) {
            return;
        }
        ensureAutonomousSchedulerScheduled();
        if (!isAutonomousSchedulerIdle()) {
            return;
        }
        requestFullScan();
    }

    void scheduleHint(final SegmentId segmentId) {
        if (!canScheduleHint(segmentId)) {
            return;
        }
        ensureAutonomousSchedulerScheduled();
        workState.addHint(segmentId);
        scheduleWorker();
    }

    @Override
    public void awaitExhausted() {
        awaitSettled();
        if (isPolicyEnabled() && forceRetryEligibleSplitCandidates()) {
            awaitSettled();
        }
    }

    private void scheduleWorker() {
        if (!workState.tryMarkScanScheduled()) {
            return;
        }
        try {
            workerExecutor.execute(this::runWorkerLoop);
        } catch (final RuntimeException e) {
            workState.clearScanScheduled();
            if (!isClosedOrClosingState()) {
                throw e;
            }
        }
    }

    private void runWorkerLoop() {
        try {
            if (!isPolicyEnabled()) {
                return;
            }
            do {
                final boolean fullScanRequested = workState
                        .consumeScanRequested();
                scheduleHintedSplitCandidates();
                if (fullScanRequested) {
                    scheduleCurrentSplitCandidates();
                }
            } while (workState.hasPendingWork());
        } catch (final RuntimeException e) {
            if (!isClosedOrClosingState()) {
                failureHandler.accept(e);
                throw e;
            }
        } finally {
            workState.clearScanScheduled();
            if (workState.hasPendingWork() && isPolicyEnabled()) {
                scheduleWorker();
            }
        }
    }

    private void ensureAutonomousSchedulerScheduled() {
        if (!isPolicyEnabled()) {
            return;
        }
        if (!workState.tryMarkTickScheduled()) {
            return;
        }
        try {
            splitPolicyScheduler.schedule(this::runAutonomousTick,
                    AUTONOMOUS_SCHEDULER_INTERVAL_MILLIS,
                    java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (final RuntimeException e) {
            workState.clearTickScheduled();
            if (isClosedOrClosingState()) {
                return;
            }
            throw e;
        }
    }

    private void runAutonomousTick() {
        workState.clearTickScheduled();
        try {
            if (!isPolicyEnabled()) {
                return;
            }
            if (isAutonomousSchedulerIdle()) {
                requestFullScan();
            }
        } catch (final RuntimeException e) {
            if (!isClosedOrClosingState()) {
                failureHandler.accept(e);
                throw e;
            }
        } finally {
            if (isPolicyEnabled()) {
                ensureAutonomousSchedulerScheduled();
            }
        }
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
                        "Background split policy completion timed out after %d ms.",
                        timeoutMillis));
            }
            LockSupport.parkNanos(
                    TimeUnit.MILLISECONDS.toNanos(SETTLE_POLL_INTERVAL_MILLIS));
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                throw new IndexException(
                        "Interrupted while waiting for background split policy completion.");
            }
        }
    }

    private boolean isSettled() {
        if (!isPolicyEnabled()) {
            workState.clearPendingWork();
            return !workState.isScanScheduled()
                    && backgroundSplitCoordinator.splitInFlightCount() == 0;
        }
        return !workState.isScanRequested()
                && !workState.isScanScheduled()
                && !workState.hasPendingHints()
                && backgroundSplitCoordinator.splitInFlightCount() == 0;
    }

    private boolean isPolicyEnabled() {
        return Boolean.TRUE.equals(conf.isBackgroundMaintenanceAutoEnabled())
                && !isClosedOrClosingState();
    }

    private boolean isClosedOrClosingState() {
        final SegmentIndexState state = stateSupplier.get();
        return state == SegmentIndexState.CLOSING
                || state == SegmentIndexState.CLOSED
                || state == SegmentIndexState.ERROR;
    }

    private boolean isAutonomousSchedulerIdle() {
        return backgroundSplitCoordinator.splitInFlightCount() == 0;
    }

    private boolean canScheduleHint(final SegmentId segmentId) {
        return segmentId != null && isPolicyEnabled()
                && keyToSegmentMap.getSegmentIds().contains(segmentId);
    }

    private void scheduleCurrentSplitCandidates() {
        scheduleSplitCandidates(keyToSegmentMap.getSegmentIds(), false);
    }

    private void scheduleHintedSplitCandidates() {
        scheduleHintedSplitCandidates(false);
    }

    private boolean forceRetryEligibleSplitCandidates() {
        return scheduleSplitCandidates(keyToSegmentMap.getSegmentIds(), true);
    }

    private boolean scheduleHintedSplitCandidates(final boolean forceRetry) {
        final int threshold = splitThreshold();
        if (!isEnabled(threshold)) {
            workState.clearHintedSegments();
            return false;
        }
        return scheduleSplitCandidates(workState.consumeHintedSegmentIds(),
                threshold, forceRetry);
    }

    private boolean scheduleSplitCandidates(final java.util.List<SegmentId> segmentIds,
            final boolean forceRetry) {
        final int threshold = splitThreshold();
        if (!isEnabled(threshold)) {
            return false;
        }
        return scheduleSplitCandidates(segmentIds, threshold, forceRetry);
    }

    private boolean scheduleSplitCandidates(
            final java.util.List<SegmentId> segmentIds, final int threshold,
            final boolean forceRetry) {
        boolean scheduledAny = false;
        for (final SegmentId segmentId : segmentIds) {
            scheduledAny |= scheduleSplitCandidateIfEligible(segmentId,
                    threshold, forceRetry);
        }
        return scheduledAny;
    }

    private boolean scheduleSplitCandidateIfEligible(
            final SegmentId segmentId, final int threshold,
            final boolean forceRetry) {
        if (!isSegmentStillMapped(segmentId)) {
            return false;
        }
        final SegmentHandle<K, V> segmentHandle = tryLoadSplitCandidate(
                segmentId);
        if (segmentHandle == null) {
            return false;
        }
        final boolean scheduled = backgroundSplitCoordinator
                .handleSplitCandidate(segmentHandle, threshold, forceRetry);
        if (scheduled) {
            stats.recordSplitScheduled();
        }
        return scheduled;
    }

    private SegmentHandle<K, V> tryLoadSplitCandidate(final SegmentId segmentId) {
        try {
            final Optional<SegmentHandle<K, V>> loaded = segmentRegistry
                    .tryGetSegment(segmentId);
            if (loaded.isPresent()) {
                return loaded.get();
            }
            return null;
        } catch (final IndexException e) {
            if (!isSegmentStillMapped(segmentId)) {
                return null;
            }
            throw e;
        }
    }

    private int splitThreshold() {
        return runtimeTuningState.effectiveValue(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT);
    }

    private boolean isEnabled(final int threshold) {
        return threshold >= 1;
    }

    private boolean isSegmentStillMapped(final SegmentId segmentId) {
        return keyToSegmentMap.getSegmentIds().contains(segmentId);
    }
}
