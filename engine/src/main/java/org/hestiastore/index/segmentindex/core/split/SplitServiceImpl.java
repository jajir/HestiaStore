package org.hestiastore.index.segmentindex.core.split;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Aggregates split execution and policy scheduling inside one managed runtime
 * boundary.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SplitServiceImpl<K, V>
        implements SplitService, SplitMetricsView {

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
        this.splitCoordinator = SplitExecutionCoordinator.create(
                validatedConf,
                Vldtn.requireNonNull(keyComparator, "keyComparator"),
                validatedKeyToSegmentMap, validatedSegmentRegistry,
                validatedSegmentTopology,
                Vldtn.requireNonNull(directoryFacade, "directoryFacade"),
                Vldtn.requireNonNull(splitExecutor, "splitExecutor"),
                validatedFailureReporter,
                validatedTelemetry);
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
            final BlockingSegment<K, V> segmentHandle,
            final long splitThreshold, final long observedKeyCount) {
        return splitCoordinator.scheduleEligibleSplit(segmentHandle,
                splitThreshold, observedKeyCount);
    }

    public boolean isSplitBlocked(final SegmentId segmentId) {
        return splitCoordinator.isSplitBlocked(segmentId);
    }

    int splitBlockedCount() {
        return splitCoordinator.splitBlockedCount();
    }

    @Override
    public void hintSplitCandidate(final SegmentId segmentId) {
        managedState.requireRunning("accept split candidate hints");
        splitPolicyCoordinator.hintSplitCandidate(segmentId);
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
    public void requestFullSplitScan() {
        splitPolicyCoordinator.requestFullSplitScan();
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
