package org.hestiastore.index.segmentindex.core.split;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.SegmentIndexRuntimeState;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLease;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLeaseService;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.BlockingSegment;

/**
 * Owns periodic full split scans, candidate deduplication, and policy workers
 * inside the managed split runtime.
 *
 * @param <K> key type
 * @param <V> value type
 */
@SuppressWarnings("java:S107")
final class SplitPolicyScheduler<K, V> {

    private static final long AUTONOMOUS_SCHEDULER_INTERVAL_MILLIS = 250L;
    private static final long SETTLE_POLL_INTERVAL_MILLIS = 10L;
    private static final long INELIGIBLE_SEGMENT_KEY_COUNT = -1L;

    private final EffectiveIndexConfiguration<K, V> conf;
    private final RuntimeTuningState runtimeTuningState;
    private final SegmentRouteMap<K> keyToSegmentMap;
    private final MappedSegmentLeaseService<K, V> segmentLeaseService;
    private final SplitTaskCoordinator<K, V> splitExecutionCoordinator;
    private final SegmentIndexRuntimeState runtimeState;
    private final SplitWorkerState policyState;
    private final SplitCandidateQueue candidateRegistry;
    private final Executor workerExecutor;
    private final ScheduledExecutorService splitPolicyScheduler;
    private final SplitStatsRecorder statsRecorder;

    SplitPolicyScheduler(final EffectiveIndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final SegmentRouteMap<K> keyToSegmentMap,
            final MappedSegmentLeaseService<K, V> segmentLeaseService,
            final SplitTaskCoordinator<K, V> splitExecutionCoordinator,
            final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final SegmentIndexRuntimeState runtimeState,
            final SplitStatsRecorder statsRecorder,
            final SplitWorkerState policyState,
            final SplitCandidateQueue candidateRegistry) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentLeaseService = Vldtn.requireNonNull(segmentLeaseService,
                "segmentLeaseService");
        this.splitExecutionCoordinator = Vldtn.requireNonNull(
                splitExecutionCoordinator, "splitExecutionCoordinator");
        this.runtimeState = Vldtn.requireNonNull(runtimeState, "runtimeState");
        this.policyState = Vldtn.requireNonNull(policyState, "policyState");
        this.candidateRegistry = Vldtn.requireNonNull(candidateRegistry,
                "candidateRegistry");
        this.workerExecutor = Vldtn.requireNonNull(workerExecutor,
                "workerExecutor");
        this.splitPolicyScheduler = Vldtn.requireNonNull(splitPolicyScheduler,
                "splitPolicyScheduler");
        this.statsRecorder = Vldtn.requireNonNull(statsRecorder,
                "statsRecorder");
    }

    void requestFullSplitScan() {
        if (!isPolicyEnabled()) {
            return;
        }
        ensureAutonomousSchedulerScheduled();
        requestFullSplitScanWork();
    }

    void awaitQuiescence() {
        awaitSettled(conf.maintenance().busyTimeoutMillis());
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
            boolean submitted = false;
            try {
                workerExecutor.execute(this::runWorkerLoop);
                submitted = true;
            } catch (final RuntimeException e) {
                if (!isClosedOrClosingState()) {
                    throw e;
                }
                return;
            } finally {
                if (!submitted) {
                    releaseReservedWorkers(workersToSchedule - i);
                }
            }
        }
    }

    private void releaseReservedWorkers(final int workerCount) {
        for (int i = 0; i < workerCount; i++) {
            policyState.markWorkerFinished();
        }
    }

    private void runWorkerLoop() {
        try {
            runWorkerUntilIdle();
        } catch (final RuntimeException e) {
            if (!isClosedOrClosingState()) {
                runtimeState.markRuntimeFailure(e);
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
            if (splitExecutionCoordinator.splitInFlightCount() == 0) {
                requestFullSplitScanWork();
            }
        } catch (final RuntimeException e) {
            if (!isClosedOrClosingState()) {
                runtimeState.markRuntimeFailure(e);
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
            final long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0L) {
                throw new IndexException(String.format(
                        "Split policy completion timed out after %d ms.",
                        timeoutMillis));
            }
            splitExecutionCoordinator.awaitSplitsIdle(Math.max(1L,
                    TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
            if (isSettled()) {
                return;
            }
            LockSupport.parkNanos(Math.min(remainingNanos,
                    TimeUnit.MILLISECONDS.toNanos(
                            SETTLE_POLL_INTERVAL_MILLIS)));
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
        return conf.maintenance().backgroundAutoEnabled()
                && !isClosedOrClosingState();
    }

    private boolean isClosedOrClosingState() {
        final SegmentIndexState state = runtimeState.currentState();
        return state == SegmentIndexState.CLOSING
                || state == SegmentIndexState.CLOSED
                || state == SegmentIndexState.ERROR;
    }

    private void enqueueCurrentSplitCandidates() {
        final int threshold = splitThreshold();
        if (threshold < 1) {
            return;
        }
        segmentLeaseService.getLoadedMappedSegmentIds().forEach(
                segmentId -> offerScanCandidateIfEligible(segmentId,
                        threshold));
    }

    private Optional<SegmentId> waitForNextCandidate() {
        final int threshold = splitThreshold();
        if (threshold < 1) {
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
        if (threshold < 1) {
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
        final long observedKeyCount = eligibleKeyCount(segmentId, threshold);
        if (observedKeyCount == INELIGIBLE_SEGMENT_KEY_COUNT) {
            return false;
        }
        final boolean scheduled = splitExecutionCoordinator.scheduleEligibleSplit(
                segmentId, threshold, observedKeyCount);
        if (scheduled) {
            statsRecorder.recordSplitScheduled();
        }
        return scheduled;
    }

    private void offerScanCandidateIfEligible(final SegmentId segmentId,
            final int threshold) {
        if (eligibleKeyCount(segmentId, threshold)
                != INELIGIBLE_SEGMENT_KEY_COUNT) {
            candidateRegistry.offer(segmentId);
        }
    }

    private long eligibleKeyCount(final SegmentId segmentId,
            final int threshold) {
        final Optional<MappedSegmentLease<K, V>> leaseResult = tryAcquireOpenMappedSegment(
                segmentId);
        if (leaseResult.isEmpty()) {
            return INELIGIBLE_SEGMENT_KEY_COUNT;
        }
        try (MappedSegmentLease<K, V> lease = leaseResult.get()) {
            final long observedKeyCount = observedKeyCount(lease.segment());
            return exceedsSplitThreshold(observedKeyCount, threshold)
                    ? observedKeyCount
                    : INELIGIBLE_SEGMENT_KEY_COUNT;
        }
    }

    private Optional<MappedSegmentLease<K, V>> tryAcquireOpenMappedSegment(
            final SegmentId segmentId) {
        if (!isSegmentStillMapped(segmentId)) {
            return Optional.empty();
        }
        final Optional<MappedSegmentLease<K, V>> leaseResult = tryAcquireSplitCandidate(
                segmentId);
        if (leaseResult.isEmpty()) {
            return Optional.empty();
        }
        final MappedSegmentLease<K, V> lease = leaseResult.get();
        try {
            if (isClosedCandidate(lease.segment())) {
                lease.close();
                return Optional.empty();
            }
            return Optional.of(lease);
        } catch (final RuntimeException e) {
            lease.close();
            throw e;
        }
    }

    private Optional<MappedSegmentLease<K, V>> tryAcquireSplitCandidate(
            final SegmentId segmentId) {
        try {
            return segmentLeaseService.tryAcquireLoadedMappedSegment(segmentId);
        } catch (final IndexException e) {
            if (!isSegmentStillMapped(segmentId)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private int splitThreshold() {
        return runtimeTuningState.segmentSplitKeyThreshold();
    }

    private long observedKeyCount(final BlockingSegment<K, V> segmentHandle) {
        return segmentHandle.getRuntime().getNumberOfKeysInCache();
    }

    private boolean isClosedCandidate(final BlockingSegment<K, V> segmentHandle) {
        return segmentHandle.getRuntime().getState() == SegmentState.CLOSED;
    }

    private boolean exceedsSplitThreshold(final long observedKeyCount,
            final int threshold) {
        return observedKeyCount > threshold;
    }

    private int workerParallelism() {
        return conf.maintenance().indexThreads();
    }

    private long workerKeepAliveMillis() {
        return conf.maintenance().busyBackoffMillis();
    }

    private boolean isSegmentStillMapped(final SegmentId segmentId) {
        return segmentId != null
                && keyToSegmentMap.getSegmentIds().contains(segmentId);
    }
}
