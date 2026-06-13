package org.hestiastore.index.segmentindex.core.split;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateView;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Builder for the default split runtime service.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SplitServiceBuilder<K, V> {

    private EffectiveIndexConfiguration<K, V> conf;
    private RuntimeTuningState runtimeTuningState;
    private KeyToSegmentMap<K> keyToSegmentMap;
    private SegmentLeaseService<K, V> segmentLeaseService;
    private SegmentRegistry<K, V> segmentRegistry;
    private Directory directoryFacade;
    private Executor splitExecutor;
    private Executor workerExecutor;
    private ScheduledExecutorService splitPolicyScheduler;
    private SegmentIndexStateView stateView;
    private SplitFailureReporter failureReporter;
    private SplitStatsRecorder statsRecorder;

    SplitServiceBuilder() {
    }

    /**
     * Sets the effective index configuration used by split policy decisions.
     *
     * @param conf effective index configuration
     * @return this builder
     */
    public SplitServiceBuilder<K, V> conf(
            final EffectiveIndexConfiguration<K, V> conf) {
        this.conf = conf;
        return this;
    }

    /**
     * Sets runtime tuning state used by the autonomous split policy.
     *
     * @param runtimeTuningState runtime tuning state
     * @return this builder
     */
    public SplitServiceBuilder<K, V> runtimeTuningState(
            final RuntimeTuningState runtimeTuningState) {
        this.runtimeTuningState = runtimeTuningState;
        return this;
    }

    /**
     * Sets the route map updated when prepared splits are published.
     *
     * @param keyToSegmentMap key-to-segment map
     * @return this builder
     */
    public SplitServiceBuilder<K, V> keyToSegmentMap(
            final KeyToSegmentMap<K> keyToSegmentMap) {
        this.keyToSegmentMap = keyToSegmentMap;
        return this;
    }

    /**
     * Sets the lease service used to acquire and drain split candidates.
     *
     * @param segmentLeaseService segment lease service
     * @return this builder
     */
    public SplitServiceBuilder<K, V> segmentLeaseService(
            final SegmentLeaseService<K, V> segmentLeaseService) {
        this.segmentLeaseService = segmentLeaseService;
        return this;
    }

    /**
     * Sets the segment registry used to load and publish split segments.
     *
     * @param segmentRegistry segment registry
     * @return this builder
     */
    public SplitServiceBuilder<K, V> segmentRegistry(
            final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = segmentRegistry;
        return this;
    }

    /**
     * Sets the root directory used for prepared segment materialization.
     *
     * @param directoryFacade directory facade
     * @return this builder
     */
    public SplitServiceBuilder<K, V> directoryFacade(
            final Directory directoryFacade) {
        this.directoryFacade = directoryFacade;
        return this;
    }

    /**
     * Sets the executor used for split work execution.
     *
     * @param splitExecutor split executor
     * @return this builder
     */
    public SplitServiceBuilder<K, V> splitExecutor(
            final Executor splitExecutor) {
        this.splitExecutor = splitExecutor;
        return this;
    }

    /**
     * Sets the executor used for autonomous split policy workers.
     *
     * @param workerExecutor worker executor
     * @return this builder
     */
    public SplitServiceBuilder<K, V> workerExecutor(
            final Executor workerExecutor) {
        this.workerExecutor = workerExecutor;
        return this;
    }

    /**
     * Sets the scheduler used for autonomous split policy ticks.
     *
     * @param splitPolicyScheduler split policy scheduler
     * @return this builder
     */
    public SplitServiceBuilder<K, V> splitPolicyScheduler(
            final ScheduledExecutorService splitPolicyScheduler) {
        this.splitPolicyScheduler = splitPolicyScheduler;
        return this;
    }

    /**
     * Sets the runtime state view used before scheduling split work.
     *
     * @param stateView runtime state view
     * @return this builder
     */
    public SplitServiceBuilder<K, V> stateView(
            final SegmentIndexStateView stateView) {
        this.stateView = stateView;
        return this;
    }

    /**
     * Sets the failure reporter used when split workers fail.
     *
     * @param failureReporter split failure reporter
     * @return this builder
     */
    public SplitServiceBuilder<K, V> failureReporter(
            final SplitFailureReporter failureReporter) {
        this.failureReporter = failureReporter;
        return this;
    }

    /**
     * Sets the split telemetry recorder.
     *
     * @param statsRecorder stats recorder
     * @return this builder
     */
    public SplitServiceBuilder<K, V> statsRecorder(
            final SplitStatsRecorder statsRecorder) {
        this.statsRecorder = statsRecorder;
        return this;
    }

    /**
     * Builds the split service and creates package-local retry policy objects
     * from the configured maintenance retry values.
     *
     * @return split service
     */
    public SplitService build() {
        final SplitFailureReporter validatedFailureReporter = Vldtn
                .requireNonNull(failureReporter, "failureReporter");
        final SplitStatsRecorder validatedStatsRecorder = Vldtn.requireNonNull(
                statsRecorder, "statsRecorder");
        final EffectiveIndexConfiguration<K, V> validatedConf = Vldtn
                .requireNonNull(conf, "conf");
        final SegmentRegistry<K, V> validatedSegmentRegistry = Vldtn
                .requireNonNull(segmentRegistry, "segmentRegistry");
        final SegmentLeaseService<K, V> validatedSegmentLeaseService = Vldtn
                .requireNonNull(segmentLeaseService, "segmentLeaseService");
        final DefaultSegmentMaterializationService<K, V> materializationService =
                new DefaultSegmentMaterializationService<>(
                        Vldtn.requireNonNull(directoryFacade,
                                "directoryFacade"),
                        validatedSegmentRegistry.materialization());
        final SplitRetryPolicy retryPolicy = new SplitRetryPolicy(
                validatedConf.maintenance().busyBackoffMillis(),
                validatedConf.maintenance().busyTimeoutMillis());
        final RouteSplitPreparationService<K, V> preparationService =
                new RouteSplitPreparationService<>(materializationService,
                        retryPolicy);
        final RouteSplitCoordinator<K, V> routeSplitCoordinator =
                new RouteSplitCoordinator<>(
                        new SegmentIndexSplitPolicyThreshold<>(),
                        preparationService);
        final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator =
                new RouteSplitPublishCoordinator<>(
                        Vldtn.requireNonNull(keyToSegmentMap,
                                "keyToSegmentMap"),
                        validatedSegmentRegistry, materializationService);
        final SplitExecutionCoordinator<K, V> splitCoordinator =
                new SplitExecutionCoordinatorImpl<>(keyToSegmentMap,
                        validatedSegmentLeaseService,
                        routeSplitCoordinator, routeSplitPublishCoordinator,
                        Vldtn.requireNonNull(splitExecutor, "splitExecutor"),
                        validatedFailureReporter, validatedStatsRecorder);
        final SplitPolicyCoordinator<K, V> splitPolicyCoordinator =
                new SplitPolicyCoordinator<>(validatedConf,
                        Vldtn.requireNonNull(runtimeTuningState,
                                "runtimeTuningState"),
                        keyToSegmentMap, validatedSegmentLeaseService,
                        splitCoordinator,
                        Vldtn.requireNonNull(workerExecutor, "workerExecutor"),
                        Vldtn.requireNonNull(splitPolicyScheduler,
                                "splitPolicyScheduler"),
                        Vldtn.requireNonNull(stateView, "stateView"),
                        validatedFailureReporter, validatedStatsRecorder,
                        new SplitPolicyState(), new SplitCandidateRegistry());
        final ManagedSplitRuntimeState managedState =
                new ManagedSplitRuntimeState();
        managedState.markRunning();
        return new SplitServiceImpl<>(splitCoordinator, splitPolicyCoordinator,
                managedState, validatedStatsRecorder);
    }
}
