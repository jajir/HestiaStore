package org.hestiastore.index.segmentindex.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.partition.PartitionRuntimeSnapshot;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;

/**
 * Owns the autonomous background split-policy scan loop and its coordination
 * state.
 */
final class BackgroundSplitPolicyLoop<K, V> {

    private static final long BACKGROUND_SPLIT_POLICY_INTERVAL_MILLIS = 250L;

    private final IndexConfiguration<K, V> conf;
    private final RuntimeTuningState runtimeTuningState;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final PartitionRuntime<K, V> partitionRuntime;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final Executor workerExecutor;
    private final ScheduledExecutorService splitPolicyScheduler;
    private final Stats stats;
    private final Supplier<SegmentIndexState> stateSupplier;
    private final Runnable awaitSplitsIdleAction;
    private final Consumer<RuntimeException> failureHandler;
    private final AtomicBoolean backgroundSplitScanScheduled = new AtomicBoolean(
            false);
    private final AtomicBoolean backgroundSplitScanRequested = new AtomicBoolean(
            false);
    private final AtomicBoolean backgroundSplitPolicyTickScheduled = new AtomicBoolean(
            false);
    private final ConcurrentHashMap<SegmentId, Boolean> backgroundSplitHintedSegments = new ConcurrentHashMap<>();

    BackgroundSplitPolicyLoop(final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final PartitionRuntime<K, V> partitionRuntime,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final Stats stats,
            final Supplier<SegmentIndexState> stateSupplier,
            final Runnable awaitSplitsIdleAction,
            final Consumer<RuntimeException> failureHandler) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.partitionRuntime = Vldtn.requireNonNull(partitionRuntime,
                "partitionRuntime");
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
        this.workerExecutor = Vldtn.requireNonNull(workerExecutor,
                "workerExecutor");
        this.splitPolicyScheduler = Vldtn.requireNonNull(splitPolicyScheduler,
                "splitPolicyScheduler");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                "stateSupplier");
        this.awaitSplitsIdleAction = Vldtn.requireNonNull(awaitSplitsIdleAction,
                "awaitSplitsIdleAction");
        this.failureHandler = Vldtn.requireNonNull(failureHandler,
                "failureHandler");
    }

    void scheduleScan() {
        if (!isBackgroundSplitPolicyEnabled()) {
            return;
        }
        ensureAutonomousBackgroundSplitPolicyLoop();
        backgroundSplitScanRequested.set(true);
        scheduleBackgroundSplitPolicyWorker();
    }

    void scheduleScanIfIdle() {
        if (!isBackgroundSplitPolicyEnabled()) {
            return;
        }
        ensureAutonomousBackgroundSplitPolicyLoop();
        if (!isAutonomousSplitPolicyIdle()) {
            return;
        }
        scheduleScan();
    }

    void scheduleHint(final SegmentId segmentId) {
        if (!isBackgroundSplitPolicyEnabled()) {
            return;
        }
        if (segmentId == null || !isSegmentStillMapped(segmentId)) {
            return;
        }
        backgroundSplitHintedSegments.put(segmentId, Boolean.TRUE);
        ensureAutonomousBackgroundSplitPolicyLoop();
        scheduleBackgroundSplitPolicyWorker();
    }

    void awaitExhausted() {
        awaitSettled();
        if (forceRetryEligibleSplitCandidates()) {
            awaitSettled();
        }
    }

    private void scheduleBackgroundSplitPolicyWorker() {
        if (!backgroundSplitScanScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            workerExecutor.execute(this::runBackgroundSplitPolicyLoop);
        } catch (final RuntimeException e) {
            backgroundSplitScanScheduled.set(false);
            if (isClosedOrClosingState()) {
                return;
            }
            throw e;
        }
    }

    private void runBackgroundSplitPolicyLoop() {
        try {
            if (!isBackgroundSplitPolicyEnabled()) {
                return;
            }
            do {
                final boolean fullScanRequested = backgroundSplitScanRequested
                        .getAndSet(false);
                scanHintedSplitCandidates();
                if (fullScanRequested) {
                    scanCurrentSplitCandidates();
                }
            } while (backgroundSplitScanRequested.get()
                    || hasPendingSplitHints());
        } catch (final RuntimeException e) {
            if (!isClosedOrClosingState()) {
                failureHandler.accept(e);
                throw e;
            }
        } finally {
            backgroundSplitScanScheduled.set(false);
            if ((backgroundSplitScanRequested.get()
                    || hasPendingSplitHints())
                    && isBackgroundSplitPolicyEnabled()) {
                scheduleBackgroundSplitPolicyWorker();
            }
        }
    }

    private void ensureAutonomousBackgroundSplitPolicyLoop() {
        if (!isBackgroundSplitPolicyEnabled()) {
            return;
        }
        if (!backgroundSplitPolicyTickScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            splitPolicyScheduler.schedule(
                    this::runAutonomousBackgroundSplitPolicyTick,
                    BACKGROUND_SPLIT_POLICY_INTERVAL_MILLIS,
                    TimeUnit.MILLISECONDS);
        } catch (final RuntimeException e) {
            backgroundSplitPolicyTickScheduled.set(false);
            if (isClosedOrClosingState()) {
                return;
            }
            throw e;
        }
    }

    private void runAutonomousBackgroundSplitPolicyTick() {
        backgroundSplitPolicyTickScheduled.set(false);
        try {
            if (!isBackgroundSplitPolicyEnabled()) {
                return;
            }
            if (isAutonomousSplitPolicyIdle()) {
                scheduleScan();
            }
        } catch (final RuntimeException e) {
            if (!isClosedOrClosingState()) {
                failureHandler.accept(e);
                throw e;
            }
        } finally {
            if (isBackgroundSplitPolicyEnabled()) {
                ensureAutonomousBackgroundSplitPolicyLoop();
            }
        }
    }

    private boolean isAutonomousSplitPolicyIdle() {
        final PartitionRuntimeSnapshot snapshot = partitionRuntime.snapshot();
        return snapshot.getBufferedKeyCount() == 0
                && snapshot.getActivePartitionCount() == 0
                && snapshot.getImmutableRunCount() == 0
                && snapshot.getDrainInFlightCount() == 0
                && snapshot.getDrainingPartitionCount() == 0
                && backgroundSplitCoordinator.splitInFlightCount() == 0;
    }

    private void scanCurrentSplitCandidates() {
        final int threshold = runtimeTuningState.effectiveValue(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT);
        if (threshold < 1) {
            return;
        }
        for (final SegmentId segmentId : keyToSegmentMap.getSegmentIds()) {
            scheduleSplitCandidateIfEligible(segmentId, threshold, false);
        }
    }

    private void scanHintedSplitCandidates() {
        final int threshold = runtimeTuningState.effectiveValue(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT);
        if (threshold < 1) {
            backgroundSplitHintedSegments.clear();
            return;
        }
        final List<SegmentId> hintedSegmentIds = new ArrayList<>(
                backgroundSplitHintedSegments.keySet());
        for (final SegmentId segmentId : hintedSegmentIds) {
            backgroundSplitHintedSegments.remove(segmentId);
            scheduleSplitCandidateIfEligible(segmentId, threshold, false);
        }
    }

    private boolean scheduleSplitCandidateIfEligible(final SegmentId segmentId,
            final int threshold, final boolean forceRetry) {
        if (!isSegmentStillMapped(segmentId)) {
            return false;
        }
        final Segment<K, V> segment = tryLoadSplitCandidate(segmentId);
        if (segment == null) {
            return false;
        }
        final boolean scheduled = forceRetry
                ? backgroundSplitCoordinator.forceHandleSplitCandidate(segment,
                        threshold)
                : backgroundSplitCoordinator.handleSplitCandidate(segment,
                        threshold);
        if (scheduled) {
            stats.incSplitScheduleCx();
        }
        return scheduled;
    }

    private boolean hasPendingSplitHints() {
        return !backgroundSplitHintedSegments.isEmpty();
    }

    private Segment<K, V> tryLoadSplitCandidate(final SegmentId segmentId) {
        final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry
                .getSegment(segmentId);
        if (loaded.getStatus() == SegmentRegistryResultStatus.OK) {
            return loaded.getValue();
        }
        if (loaded.getStatus() == SegmentRegistryResultStatus.BUSY
                || loaded.getStatus() == SegmentRegistryResultStatus.CLOSED) {
            return null;
        }
        if (!isSegmentStillMapped(segmentId)) {
            return null;
        }
        throw new IndexException(String.format(
                "Segment '%s' failed to load for split scheduling: %s",
                segmentId, loaded.getStatus()));
    }

    private void awaitSettled() {
        final long timeoutMillis = conf.getIndexBusyTimeoutMillis();
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            awaitSplitsIdleAction.run();
            if (!backgroundSplitScanRequested.get()
                    && !backgroundSplitScanScheduled.get()
                    && !hasPendingSplitHints()
                    && backgroundSplitCoordinator.splitInFlightCount() == 0) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                throw new IndexException(String.format(
                        "Background split policy completion timed out after %d ms.",
                        timeoutMillis));
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                throw new IndexException(
                        "Interrupted while waiting for background split policy completion.");
            }
        }
    }

    private boolean forceRetryEligibleSplitCandidates() {
        final int threshold = runtimeTuningState.effectiveValue(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT);
        if (threshold < 1) {
            return false;
        }
        boolean scheduledAny = false;
        for (final SegmentId segmentId : keyToSegmentMap.getSegmentIds()) {
            scheduledAny |= scheduleSplitCandidateIfEligible(segmentId,
                    threshold, true);
        }
        return scheduledAny;
    }

    private boolean isBackgroundSplitPolicyEnabled() {
        return Boolean.TRUE.equals(conf.isBackgroundMaintenanceAutoEnabled())
                && !isClosedOrClosingState();
    }

    private boolean isClosedOrClosingState() {
        final SegmentIndexState state = stateSupplier.get();
        return state == SegmentIndexState.CLOSED
                || state == SegmentIndexState.ERROR;
    }

    private boolean isSegmentStillMapped(final SegmentId segmentId) {
        return keyToSegmentMap.getSegmentIds().contains(segmentId);
    }
}
