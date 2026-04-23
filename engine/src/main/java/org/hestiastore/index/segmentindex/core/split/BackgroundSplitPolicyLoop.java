package org.hestiastore.index.segmentindex.core.split;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Owns autonomous background split-policy scans inside the split runtime
 * boundary.
 *
 * @param <K> key type
 * @param <V> value type
 */
@SuppressWarnings("java:S107")
final class BackgroundSplitPolicyLoop<K, V> implements SplitRuntimeEvents {

    private static final long AUTONOMOUS_SCHEDULER_INTERVAL_MILLIS = 250L;
    private static final long SETTLE_POLL_INTERVAL_MILLIS = 10L;

    private final IndexConfiguration<K, V> conf;
    private final RuntimeTuningState runtimeTuningState;
    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final SplitRuntimeLifecycle lifecycle;
    private final BackgroundSplitPolicyWorkState workState;
    private final Executor workerExecutor;
    private final ScheduledExecutorService splitPolicyScheduler;
    private final SplitFailureReporter failureReporter;
    private final SplitRuntimeTelemetry telemetry;

    BackgroundSplitPolicyLoop(final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final SplitRuntimeLifecycle lifecycle,
            final SplitFailureReporter failureReporter,
            final SplitRuntimeTelemetry telemetry,
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
        this.lifecycle = Vldtn.requireNonNull(lifecycle, "lifecycle");
        this.workState = Vldtn.requireNonNull(workState, "workState");
        this.workerExecutor = Vldtn.requireNonNull(workerExecutor,
                "workerExecutor");
        this.splitPolicyScheduler = Vldtn.requireNonNull(splitPolicyScheduler,
                "splitPolicyScheduler");
        this.failureReporter = Vldtn.requireNonNull(failureReporter,
                "failureReporter");
        this.telemetry = Vldtn.requireNonNull(telemetry, "telemetry");
    }

    void scheduleScan() {
        if (!isPolicyEnabled()) {
            return;
        }
        ensureAutonomousSchedulerScheduled();
        requestFullScan();
    }

    void scheduleScanIfIdle() {
        if (!isPolicyEnabled()) {
            return;
        }
        ensureAutonomousSchedulerScheduled();
        if (!isAutonomousSchedulerIdle()) {
            return;
        }
        requestFullScan();
    }

    void awaitExhausted() {
        awaitExhausted(conf.getIndexBusyTimeoutMillis());
    }

    void awaitExhausted(final long timeoutMillis) {
        awaitSettled();
        if (isPolicyEnabled() && forceRetryEligibleSplitCandidates()) {
            awaitSettled(timeoutMillis);
        }
    }

    void requestSegmentCheck(final SegmentId segmentId) {
        if (!isPolicyEnabled() || segmentId == null) {
            return;
        }
        ensureAutonomousSchedulerScheduled();
        workState.addHint(segmentId);
        scheduleWorker();
    }

    private void requestFullScan() {
        workState.markScanRequested();
        scheduleWorker();
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
                failureReporter.reportFailure(e);
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
                    TimeUnit.MILLISECONDS);
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
                failureReporter.reportFailure(e);
                throw e;
            }
        } finally {
            if (isPolicyEnabled()) {
                ensureAutonomousSchedulerScheduled();
            }
        }
    }

    private void awaitSettled() {
        awaitSettled(conf.getIndexBusyTimeoutMillis());
    }

    private void awaitSettled(final long timeoutMillis) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            backgroundSplitCoordinator.awaitSplitsIdle(timeoutMillis);
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
        return lifecycle.rejectsBackgroundWork();
    }

    private boolean isAutonomousSchedulerIdle() {
        return backgroundSplitCoordinator.splitInFlightCount() == 0;
    }

    private void scheduleCurrentSplitCandidates() {
        scheduleSplitCandidates(keyToSegmentMap.getSegmentIds(), false);
    }

    private void scheduleHintedSplitCandidates() {
        final int threshold = splitThreshold();
        if (!isEnabled(threshold)) {
            workState.clearHintedSegments();
            return;
        }
        scheduleSplitCandidates(workState.consumeHintedSegmentIds(), threshold,
                false);
    }

    private boolean forceRetryEligibleSplitCandidates() {
        return scheduleSplitCandidates(keyToSegmentMap.getSegmentIds(), true);
    }

    private boolean scheduleSplitCandidates(final List<SegmentId> segmentIds,
            final boolean forceRetry) {
        final int threshold = splitThreshold();
        if (!isEnabled(threshold)) {
            return false;
        }
        return scheduleSplitCandidates(segmentIds, threshold, forceRetry);
    }

    private boolean scheduleSplitCandidates(final List<SegmentId> segmentIds,
            final int threshold, final boolean forceRetry) {
        boolean scheduledAny = false;
        for (final SegmentId segmentId : segmentIds) {
            scheduledAny |= scheduleSplitCandidateIfEligible(segmentId,
                    threshold, forceRetry);
        }
        return scheduledAny;
    }

    private boolean scheduleSplitCandidateIfEligible(final SegmentId segmentId,
            final int threshold, final boolean forceRetry) {
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
            telemetry.recordSplitScheduled();
        }
        return scheduled;
    }

    private SegmentHandle<K, V> tryLoadSplitCandidate(
            final SegmentId segmentId) {
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
        return segmentId != null
                && keyToSegmentMap.getSegmentIds().contains(segmentId);
    }

    @Override
    public void onSplitApplied() {
        scheduleScanIfIdle();
    }
}
