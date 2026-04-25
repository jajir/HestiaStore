package org.hestiastore.index.segmentindex.core.split;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Owns periodic full split scans, candidate deduplication, and policy workers
 * inside the managed split runtime.
 *
 * @param <K> key type
 * @param <V> value type
 */
@SuppressWarnings("java:S107")
final class SplitPolicyCoordinator<K, V> {

    private static final long AUTONOMOUS_SCHEDULER_INTERVAL_MILLIS = 250L;
    private static final long SETTLE_POLL_INTERVAL_MILLIS = 10L;

    private final IndexConfiguration<K, V> conf;
    private final RuntimeTuningState runtimeTuningState;
    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SplitExecutionCoordinator<K, V> splitExecutionCoordinator;
    private final Supplier<SegmentIndexState> stateSupplier;
    private final SplitPolicyState policyState;
    private final SplitCandidateRegistry candidateRegistry;
    private final Executor workerExecutor;
    private final ScheduledExecutorService splitPolicyScheduler;
    private final SplitFailureReporter failureReporter;
    private final SplitTelemetry telemetry;

    SplitPolicyCoordinator(final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SplitExecutionCoordinator<K, V> splitExecutionCoordinator,
            final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final Supplier<SegmentIndexState> stateSupplier,
            final SplitFailureReporter failureReporter,
            final SplitTelemetry telemetry,
            final SplitPolicyState policyState) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.splitExecutionCoordinator = Vldtn.requireNonNull(
                splitExecutionCoordinator, "splitExecutionCoordinator");
        this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                "stateSupplier");
        this.policyState = Vldtn.requireNonNull(policyState, "policyState");
        this.candidateRegistry = new SplitCandidateRegistry();
        this.workerExecutor = Vldtn.requireNonNull(workerExecutor,
                "workerExecutor");
        this.splitPolicyScheduler = Vldtn.requireNonNull(splitPolicyScheduler,
                "splitPolicyScheduler");
        this.failureReporter = Vldtn.requireNonNull(failureReporter,
                "failureReporter");
        this.telemetry = Vldtn.requireNonNull(telemetry, "telemetry");
    }

    void requestFullSplitScan() {
        if (!isPolicyEnabled()) {
            return;
        }
        ensureAutonomousSchedulerScheduled();
        requestFullSplitScanWork();
    }

    void awaitQuiescence() {
        awaitQuiescence(conf.getIndexBusyTimeoutMillis());
    }

    void awaitQuiescence(final long timeoutMillis) {
        awaitSettled(timeoutMillis);
    }

    void hintSplitCandidate(final SegmentId segmentId) {
        if (!isPolicyEnabled() || segmentId == null) {
            return;
        }
        ensureAutonomousSchedulerScheduled();
        if (candidateRegistry.offer(segmentId)) {
            scheduleWorkers();
        }
    }

    private void requestFullSplitScanWork() {
        policyState.markFullScanRequested();
        scheduleWorkers();
    }

    private void scheduleWorkers() {
        if (!isPolicyEnabled()) {
            return;
        }
        final int workersToSchedule = policyState
                .reserveWorkers(workerParallelism());
        for (int i = 0; i < workersToSchedule; i++) {
            try {
                workerExecutor.execute(this::runWorkerLoop);
            } catch (final RuntimeException e) {
                policyState.markWorkerFinished();
                if (!isClosedOrClosingState()) {
                    throw e;
                }
                return;
            }
        }
    }

    private void runWorkerLoop() {
        try {
            runWorkerUntilIdle();
        } catch (final RuntimeException e) {
            if (!isClosedOrClosingState()) {
                failureReporter.reportFailure(e);
                throw e;
            }
        } finally {
            policyState.markWorkerFinished();
            if ((policyState.hasPendingWork()
                    || candidateRegistry.hasPendingCandidates())
                    && isPolicyEnabled()) {
                scheduleWorkers();
            }
        }
    }

    private void runWorkerUntilIdle() {
        while (isPolicyEnabled()) {
            if (policyState.consumeFullScanRequested()) {
                enqueueCurrentSplitCandidates();
            }
            final Optional<SegmentId> nextCandidate = waitForNextCandidate();
            if (nextCandidate.isEmpty()) {
                if (!policyState.hasPendingWork()) {
                    return;
                }
                continue;
            }
            processClaimedCandidate(nextCandidate.get());
        }
    }

    private void ensureAutonomousSchedulerScheduled() {
        if (!isPolicyEnabled()) {
            return;
        }
        if (!policyState.tryMarkTickScheduled()) {
            return;
        }
        try {
            splitPolicyScheduler.schedule(this::runAutonomousTick,
                    AUTONOMOUS_SCHEDULER_INTERVAL_MILLIS,
                    TimeUnit.MILLISECONDS);
        } catch (final RuntimeException e) {
            policyState.clearTickScheduled();
            if (isClosedOrClosingState()) {
                return;
            }
            throw e;
        }
    }

    private void runAutonomousTick() {
        policyState.clearTickScheduled();
        try {
            if (!isPolicyEnabled()) {
                return;
            }
            if (hasNoSplitInFlight()) {
                requestFullSplitScanWork();
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

    private void awaitSettled(final long timeoutMillis) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            splitExecutionCoordinator.awaitSplitsIdle(timeoutMillis);
            if (isSettled()) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                throw new IndexException(String.format(
                        "Split policy completion timed out after %d ms.",
                        timeoutMillis));
            }
            LockSupport.parkNanos(
                    TimeUnit.MILLISECONDS.toNanos(SETTLE_POLL_INTERVAL_MILLIS));
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                throw new IndexException(
                        "Interrupted while waiting for split policy completion.");
            }
        }
    }

    private boolean isSettled() {
        if (!isPolicyEnabled()) {
            policyState.clearPendingWork();
            candidateRegistry.clear();
            return !policyState.isWorkerActive()
                    && splitExecutionCoordinator.splitInFlightCount() == 0;
        }
        return !policyState.isFullScanRequested()
                && !policyState.isWorkerActive()
                && !candidateRegistry.hasPendingCandidates()
                && splitExecutionCoordinator.splitInFlightCount() == 0;
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

    private boolean hasNoSplitInFlight() {
        return splitExecutionCoordinator.splitInFlightCount() == 0;
    }

    private void enqueueCurrentSplitCandidates() {
        final int threshold = splitThreshold();
        if (!isEnabled(threshold)) {
            return;
        }
        keyToSegmentMap.getSegmentIds().forEach(
                segmentId -> offerScanCandidateIfEligible(segmentId,
                        threshold));
    }

    private Optional<SegmentId> waitForNextCandidate() {
        final int threshold = splitThreshold();
        if (!isEnabled(threshold)) {
            candidateRegistry.clear();
            return Optional.empty();
        }
        try {
            return candidateRegistry.claimNextCandidate(
                    workerKeepAliveMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IndexException(
                    "Interrupted while waiting for split candidate.",
                    e);
        }
    }

    private void processClaimedCandidate(final SegmentId segmentId) {
        final int threshold = splitThreshold();
        if (!isEnabled(threshold)) {
            candidateRegistry.clear();
            return;
        }
        try {
            scheduleSplitCandidateIfEligible(segmentId, threshold);
        } finally {
            candidateRegistry.markFinished(segmentId);
        }
    }

    private boolean scheduleSplitCandidateIfEligible(final SegmentId segmentId,
            final int threshold) {
        if (!isSegmentStillMapped(segmentId)) {
            return false;
        }
        final BlockingSegment<K, V> segmentHandle = tryLoadSplitCandidate(
                segmentId);
        if (segmentHandle == null) {
            return false;
        }
        final long observedKeyCount = observedKeyCount(segmentHandle);
        if (!exceedsSplitThreshold(observedKeyCount, threshold)) {
            return false;
        }
        final boolean scheduled = splitExecutionCoordinator
                .scheduleEligibleSplit(segmentHandle, threshold,
                        observedKeyCount);
        if (scheduled) {
            telemetry.recordSplitScheduled();
        }
        return scheduled;
    }

    private void offerScanCandidateIfEligible(final SegmentId segmentId,
            final int threshold) {
        if (!isSegmentStillMapped(segmentId)) {
            return;
        }
        final BlockingSegment<K, V> segmentHandle = tryLoadSplitCandidate(
                segmentId);
        if (segmentHandle == null) {
            return;
        }
        if (!exceedsSplitThreshold(observedKeyCount(segmentHandle), threshold)) {
            return;
        }
        candidateRegistry.offer(segmentId);
    }

    private BlockingSegment<K, V> tryLoadSplitCandidate(
            final SegmentId segmentId) {
        try {
            final Optional<BlockingSegment<K, V>> loaded = segmentRegistry
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

    private long observedKeyCount(final BlockingSegment<K, V> segmentHandle) {
        return segmentHandle.getRuntime().getNumberOfKeysInCache();
    }

    private boolean exceedsSplitThreshold(final long observedKeyCount,
            final int threshold) {
        return observedKeyCount > threshold;
    }

    private int workerParallelism() {
        final Integer configured = conf.getNumberOfIndexMaintenanceThreads();
        if (configured == null || configured < 1) {
            return 1;
        }
        return configured;
    }

    private long workerKeepAliveMillis() {
        final Integer configured = conf.getIndexBusyBackoffMillis();
        if (configured == null || configured < 1) {
            return 1L;
        }
        return configured.longValue();
    }

    private boolean isEnabled(final int threshold) {
        return threshold >= 1;
    }

    private boolean isSegmentStillMapped(final SegmentId segmentId) {
        return segmentId != null
                && keyToSegmentMap.getSegmentIds().contains(segmentId);
    }
}
