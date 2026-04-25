package org.hestiastore.index.segmentindex.core.split;

import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.maintenance.SplitMaintenanceSynchronization;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Aggregates split execution and policy scheduling inside one managed runtime
 * boundary.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SplitServiceImpl<K, V>
        implements SplitService<K, V>, SplitMaintenanceSynchronization<K, V>,
        SplitMetricsView {

    private final SplitExecutionCoordinator<K, V> splitCoordinator;
    private final SplitPolicyCoordinator<K, V> splitPolicyCoordinator;
    private final ManagedSplitRuntimeState managedState;

    SplitServiceImpl(
            final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final Comparator<K> keyComparator,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentTopology<K> segmentTopology,
            final SegmentRegistry<K, V> segmentRegistry,
            final Directory directoryFacade,
            final Executor splitExecutor,
            final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final Supplier<SegmentIndexState> stateSupplier,
            final SplitFailureReporter failureReporter,
            final SplitTelemetry telemetry,
            final SplitPolicyState policyState) {
        final IndexConfiguration<K, V> validatedConf = Vldtn
                .requireNonNull(conf, "conf");
        final RuntimeTuningState validatedRuntimeTuningState = Vldtn
                .requireNonNull(runtimeTuningState, "runtimeTuningState");
        final KeyToSegmentMap<K> validatedKeyToSegmentMap = Vldtn
                .requireNonNull(keyToSegmentMap, "keyToSegmentMap");
        final SegmentTopology<K> validatedSegmentTopology = Vldtn
                .requireNonNull(segmentTopology, "segmentTopology");
        final SegmentRegistry<K, V> validatedSegmentRegistry = Vldtn
                .requireNonNull(segmentRegistry, "segmentRegistry");
        final SplitFailureReporter validatedFailureReporter = Vldtn
                .requireNonNull(failureReporter, "failureReporter");
        final SplitTelemetry validatedTelemetry = Vldtn
                .requireNonNull(telemetry, "telemetry");
        final AtomicReference<Runnable> onSplitAppliedRef = new AtomicReference<>(() -> {
        });
        this.splitCoordinator = SplitExecutionCoordinator.create(
                validatedConf,
                Vldtn.requireNonNull(keyComparator, "keyComparator"),
                validatedKeyToSegmentMap, validatedSegmentRegistry,
                validatedSegmentTopology,
                Vldtn.requireNonNull(directoryFacade, "directoryFacade"),
                Vldtn.requireNonNull(splitExecutor, "splitExecutor"),
                validatedFailureReporter,
                () -> onSplitAppliedRef.get().run(), validatedTelemetry);
        this.splitPolicyCoordinator = new SplitPolicyCoordinator<>(
                validatedConf, validatedRuntimeTuningState,
                validatedKeyToSegmentMap, validatedSegmentRegistry,
                this.splitCoordinator,
                Vldtn.requireNonNull(workerExecutor, "workerExecutor"),
                Vldtn.requireNonNull(splitPolicyScheduler,
                        "splitPolicyScheduler"),
                Vldtn.requireNonNull(stateSupplier, "stateSupplier"),
                validatedFailureReporter, validatedTelemetry,
                Vldtn.requireNonNull(policyState, "policyState"));
        this.managedState = initializeManagedState();
        onSplitAppliedRef.set(splitPolicyCoordinator::onSplitApplied);
    }

    SplitServiceImpl(final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SplitExecutionCoordinator<K, V> splitCoordinator,
            final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final Supplier<SegmentIndexState> stateSupplier,
            final SplitFailureReporter failureReporter,
            final SplitTelemetry telemetry,
            final SplitPolicyState policyState) {
        this.splitCoordinator = Vldtn.requireNonNull(splitCoordinator,
                "splitCoordinator");
        this.splitPolicyCoordinator = new SplitPolicyCoordinator<>(
                Vldtn.requireNonNull(conf, "conf"),
                Vldtn.requireNonNull(runtimeTuningState,
                        "runtimeTuningState"),
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                this.splitCoordinator,
                Vldtn.requireNonNull(workerExecutor, "workerExecutor"),
                Vldtn.requireNonNull(splitPolicyScheduler,
                        "splitPolicyScheduler"),
                Vldtn.requireNonNull(stateSupplier, "stateSupplier"),
                Vldtn.requireNonNull(failureReporter, "failureReporter"),
                Vldtn.requireNonNull(telemetry, "telemetry"),
                Vldtn.requireNonNull(policyState, "policyState"));
        this.managedState = initializeManagedState();
    }

    boolean scheduleEligibleSplit(
            final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold, final long observedKeyCount) {
        return splitCoordinator.scheduleEligibleSplit(segmentHandle,
                splitThreshold, observedKeyCount);
    }

    void awaitSplitsIdle(final long timeoutMillis) {
        splitCoordinator.awaitSplitsIdle(timeoutMillis);
    }

    @Override
    public void awaitIdle(final long timeoutMillis) {
        awaitSplitsIdle(timeoutMillis);
    }

    @Override
    public int splitInFlightCount() {
        return splitCoordinator.splitInFlightCount();
    }

    public boolean isSplitBlocked(final SegmentId segmentId) {
        return splitCoordinator.isSplitBlocked(segmentId);
    }

    int splitBlockedCount() {
        return splitCoordinator.splitBlockedCount();
    }

    @Override
    public void runWithSplitSchedulingPaused(final Runnable action) {
        splitCoordinator.runWithSplitSchedulingPaused(action);
    }

    @Override
    public void hintSplitCandidate(final SegmentId segmentId) {
        managedState.requireRunning("accept split candidate hints");
        splitPolicyCoordinator.hintSplitCandidate(segmentId);
    }

    @Override
    public void awaitQuiescence(final Duration timeout) {
        managedState.requireRunning("await quiescence");
        final Duration validatedTimeout = Vldtn.requireNonNull(timeout,
                "timeout");
        splitPolicyCoordinator.awaitQuiescence(validatedTimeout.toMillis());
    }

    @Override
    public SplitMaintenanceSynchronization<K, V> splitMaintenance() {
        return this;
    }

    @Override
    public SplitMetricsView splitMetricsView() {
        return this;
    }

    @Override
    public void close() {
        if (!managedState.beginClose()) {
            return;
        }
        try {
            splitPolicyCoordinator.awaitQuiescence();
        } finally {
            managedState.markClosed();
        }
    }

    @Override
    public void requestReconciliation() {
        splitPolicyCoordinator.requestReconciliation();
    }

    @Override
    public void requestReconciliationIfIdle() {
        splitPolicyCoordinator.requestReconciliationIfIdle();
    }

    @Override
    public void awaitQuiescence() {
        splitPolicyCoordinator.awaitQuiescence();
    }

    @Override
    public SplitMetricsSnapshot metricsSnapshot() {
        return new SplitMetricsSnapshot(splitCoordinator.splitInFlightCount(),
                splitCoordinator.splitBlockedCount());
    }

    private ManagedSplitRuntimeState initializeManagedState() {
        final ManagedSplitRuntimeState state = new ManagedSplitRuntimeState();
        state.markRunning();
        return state;
    }
}
