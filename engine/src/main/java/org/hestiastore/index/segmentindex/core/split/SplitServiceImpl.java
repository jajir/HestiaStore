package org.hestiastore.index.segmentindex.core.split;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Aggregates split execution and split-policy scheduling behind one owned
 * boundary.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SplitServiceImpl<K, V>
        implements BackgroundSplitRuntimeAccess<K, V> {

    private final BackgroundSplitCoordinator<K, V> splitCoordinator;
    private final BackgroundSplitPolicyLoop<K, V> splitPolicyLoop;

    SplitServiceImpl(
            final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final Comparator<K> keyComparator,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final Directory directoryFacade,
            final Executor splitExecutor,
            final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final SplitRuntimeLifecycle lifecycle,
            final SplitFailureReporter failureReporter,
            final SplitRuntimeTelemetry telemetry,
            final BackgroundSplitPolicyWorkState workState) {
        final IndexConfiguration<K, V> validatedConf = Vldtn
                .requireNonNull(conf, "conf");
        final RuntimeTuningState validatedRuntimeTuningState = Vldtn
                .requireNonNull(runtimeTuningState, "runtimeTuningState");
        final KeyToSegmentMap<K> validatedKeyToSegmentMap = Vldtn
                .requireNonNull(keyToSegmentMap, "keyToSegmentMap");
        final SegmentRegistry<K, V> validatedSegmentRegistry = Vldtn
                .requireNonNull(segmentRegistry, "segmentRegistry");
        final SplitFailureReporter validatedFailureReporter = Vldtn
                .requireNonNull(failureReporter, "failureReporter");
        final SplitRuntimeTelemetry validatedTelemetry = Vldtn
                .requireNonNull(telemetry, "telemetry");
        final SplitRuntimeEventRouter runtimeEvents =
                new SplitRuntimeEventRouter();
        this.splitCoordinator = BackgroundSplitCoordinator.create(
                validatedConf,
                Vldtn.requireNonNull(keyComparator, "keyComparator"),
                validatedKeyToSegmentMap, validatedSegmentRegistry,
                Vldtn.requireNonNull(directoryFacade, "directoryFacade"),
                Vldtn.requireNonNull(splitExecutor, "splitExecutor"),
                validatedFailureReporter, runtimeEvents, validatedTelemetry);
        this.splitPolicyLoop = new BackgroundSplitPolicyLoop<>(
                validatedConf, validatedRuntimeTuningState,
                validatedKeyToSegmentMap, validatedSegmentRegistry,
                this.splitCoordinator,
                Vldtn.requireNonNull(workerExecutor, "workerExecutor"),
                Vldtn.requireNonNull(splitPolicyScheduler,
                        "splitPolicyScheduler"),
                Vldtn.requireNonNull(lifecycle, "lifecycle"),
                validatedFailureReporter, validatedTelemetry,
                Vldtn.requireNonNull(workState, "workState"));
        runtimeEvents.attach(splitPolicyLoop);
    }

    SplitServiceImpl(final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final BackgroundSplitCoordinator<K, V> splitCoordinator,
            final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final SplitRuntimeLifecycle lifecycle,
            final SplitFailureReporter failureReporter,
            final SplitRuntimeTelemetry telemetry,
            final BackgroundSplitPolicyWorkState workState) {
        this.splitCoordinator = Vldtn.requireNonNull(splitCoordinator,
                "splitCoordinator");
        this.splitPolicyLoop = new BackgroundSplitPolicyLoop<>(
                Vldtn.requireNonNull(conf, "conf"),
                Vldtn.requireNonNull(runtimeTuningState,
                        "runtimeTuningState"),
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                this.splitCoordinator,
                Vldtn.requireNonNull(workerExecutor, "workerExecutor"),
                Vldtn.requireNonNull(splitPolicyScheduler,
                        "splitPolicyScheduler"),
                Vldtn.requireNonNull(lifecycle, "lifecycle"),
                Vldtn.requireNonNull(failureReporter, "failureReporter"),
                Vldtn.requireNonNull(telemetry, "telemetry"),
                Vldtn.requireNonNull(workState, "workState"));
    }

    @Override
    public boolean handleSplitCandidate(
            final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold, final boolean ignoreCooldown) {
        return splitCoordinator.handleSplitCandidate(segmentHandle,
                splitThreshold, ignoreCooldown);
    }

    @Override
    public void awaitSplitsIdle(final long timeoutMillis) {
        splitCoordinator.awaitSplitsIdle(timeoutMillis);
    }

    @Override
    public int splitInFlightCount() {
        return splitCoordinator.splitInFlightCount();
    }

    @Override
    public boolean isSplitBlocked(final SegmentId segmentId) {
        return splitCoordinator.isSplitBlocked(segmentId);
    }

    @Override
    public int splitBlockedCount() {
        return splitCoordinator.splitBlockedCount();
    }

    @Override
    public <T> T runWithSplitSchedulingPaused(final Supplier<T> action) {
        return splitCoordinator.runWithSplitSchedulingPaused(action);
    }

    @Override
    public void runWithSplitSchedulingPaused(final Runnable action) {
        splitCoordinator.runWithSplitSchedulingPaused(action);
    }

    @Override
    public <T> T runWithSharedSplitAdmission(final Supplier<T> action) {
        return splitCoordinator.runWithSharedSplitAdmission(action);
    }

    @Override
    public void requestSegmentCheck(final SegmentId segmentId) {
        splitPolicyLoop.requestSegmentCheck(segmentId);
    }

    @Override
    public void requestFullScan() {
        scheduleScan();
    }

    @Override
    public void awaitIdle(final long timeoutMillis) {
        splitPolicyLoop.awaitExhausted(timeoutMillis);
    }

    @Override
    public SplitRuntimeSnapshot runtimeSnapshot() {
        return new SplitRuntimeSnapshot(splitCoordinator.splitInFlightCount(),
                splitCoordinator.splitBlockedCount());
    }

    @Override
    public void scheduleScan() {
        splitPolicyLoop.scheduleScan();
    }

    @Override
    public void scheduleScanIfIdle() {
        splitPolicyLoop.scheduleScanIfIdle();
    }

    @Override
    public void awaitExhausted() {
        splitPolicyLoop.awaitExhausted();
    }
}
