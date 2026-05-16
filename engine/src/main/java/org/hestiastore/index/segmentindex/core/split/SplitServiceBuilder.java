package org.hestiastore.index.segmentindex.core.split;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
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
    private Supplier<SegmentIndexState> stateSupplier;
    private Consumer<RuntimeException> failureHandler;
    private SplitStatsRecorder statsRecorder;

    SplitServiceBuilder() {
    }

    public SplitServiceBuilder<K, V> conf(
            final EffectiveIndexConfiguration<K, V> conf) {
        this.conf = conf;
        return this;
    }

    public SplitServiceBuilder<K, V> runtimeTuningState(
            final RuntimeTuningState runtimeTuningState) {
        this.runtimeTuningState = runtimeTuningState;
        return this;
    }

    public SplitServiceBuilder<K, V> keyToSegmentMap(
            final KeyToSegmentMap<K> keyToSegmentMap) {
        this.keyToSegmentMap = keyToSegmentMap;
        return this;
    }

    public SplitServiceBuilder<K, V> segmentLeaseService(
            final SegmentLeaseService<K, V> segmentLeaseService) {
        this.segmentLeaseService = segmentLeaseService;
        return this;
    }

    public SplitServiceBuilder<K, V> segmentRegistry(
            final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = segmentRegistry;
        return this;
    }

    public SplitServiceBuilder<K, V> directoryFacade(
            final Directory directoryFacade) {
        this.directoryFacade = directoryFacade;
        return this;
    }

    public SplitServiceBuilder<K, V> splitExecutor(
            final Executor splitExecutor) {
        this.splitExecutor = splitExecutor;
        return this;
    }

    public SplitServiceBuilder<K, V> workerExecutor(
            final Executor workerExecutor) {
        this.workerExecutor = workerExecutor;
        return this;
    }

    public SplitServiceBuilder<K, V> splitPolicyScheduler(
            final ScheduledExecutorService splitPolicyScheduler) {
        this.splitPolicyScheduler = splitPolicyScheduler;
        return this;
    }

    public SplitServiceBuilder<K, V> stateSupplier(
            final Supplier<SegmentIndexState> stateSupplier) {
        this.stateSupplier = stateSupplier;
        return this;
    }

    public SplitServiceBuilder<K, V> failureHandler(
            final Consumer<RuntimeException> failureHandler) {
        this.failureHandler = failureHandler;
        return this;
    }

    public SplitServiceBuilder<K, V> statsRecorder(
            final SplitStatsRecorder statsRecorder) {
        this.statsRecorder = statsRecorder;
        return this;
    }

    public SplitService build() {
        final SplitFailureReporter failureReporter = SplitFailureReporter
                .from(Vldtn.requireNonNull(failureHandler, "failureHandler"));
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
        final IndexRetryPolicy retryPolicy = new IndexRetryPolicy(
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
                        failureReporter, validatedStatsRecorder);
        final SplitPolicyCoordinator<K, V> splitPolicyCoordinator =
                new SplitPolicyCoordinator<>(validatedConf,
                        Vldtn.requireNonNull(runtimeTuningState,
                                "runtimeTuningState"),
                        keyToSegmentMap, validatedSegmentLeaseService,
                        splitCoordinator,
                        Vldtn.requireNonNull(workerExecutor, "workerExecutor"),
                        Vldtn.requireNonNull(splitPolicyScheduler,
                                "splitPolicyScheduler"),
                        Vldtn.requireNonNull(stateSupplier, "stateSupplier"),
                        failureReporter, validatedStatsRecorder,
                        new SplitPolicyState(), new SplitCandidateRegistry());
        final ManagedSplitRuntimeState managedState =
                new ManagedSplitRuntimeState();
        managedState.markRunning();
        return new SplitServiceImpl<>(splitCoordinator, splitPolicyCoordinator,
                managedState, validatedStatsRecorder);
    }
}
