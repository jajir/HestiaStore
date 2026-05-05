package org.hestiastore.index.segmentindex.core.split;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.metrics.Stats;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.LoggerFactory;

/**
 * Builder for the default split runtime service.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SplitServiceBuilder<K, V> {

    private IndexConfiguration<K, V> conf;
    private RuntimeTuningState runtimeTuningState;
    private KeyToSegmentMap<K> keyToSegmentMap;
    private SegmentTopology<K> segmentTopology;
    private SegmentRegistry<K, V> segmentRegistry;
    private Directory directoryFacade;
    private Executor splitExecutor;
    private Executor workerExecutor;
    private ScheduledExecutorService splitPolicyScheduler;
    private Supplier<SegmentIndexState> stateSupplier;
    private Consumer<RuntimeException> failureHandler;
    private Stats stats;

    SplitServiceBuilder() {
    }

    public SplitServiceBuilder<K, V> conf(
            final IndexConfiguration<K, V> conf) {
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

    public SplitServiceBuilder<K, V> segmentTopology(
            final SegmentTopology<K> segmentTopology) {
        this.segmentTopology = segmentTopology;
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

    public SplitServiceBuilder<K, V> stats(final Stats stats) {
        this.stats = stats;
        return this;
    }

    public SplitService build() {
        final SplitFailureReporter failureReporter = SplitFailureReporter
                .from(Vldtn.requireNonNull(failureHandler, "failureHandler"));
        final SplitTelemetry telemetry = SplitTelemetry.from(
                Vldtn.requireNonNull(stats, "stats"));
        final IndexConfiguration<K, V> validatedConf = Vldtn
                .requireNonNull(conf, "conf");
        final SegmentRegistry<K, V> validatedSegmentRegistry = Vldtn
                .requireNonNull(segmentRegistry, "segmentRegistry");
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
                        retryPolicy,
                        LoggerFactory.getLogger(RouteSplitCoordinator.class));
        final RouteSplitCoordinator<K, V> routeSplitCoordinator =
                new RouteSplitCoordinator<>(validatedSegmentRegistry,
                        new SegmentIndexSplitPolicyThreshold<>(),
                        preparationService);
        final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator =
                new RouteSplitPublishCoordinator<>(
                        Vldtn.requireNonNull(keyToSegmentMap,
                                "keyToSegmentMap"),
                        validatedSegmentRegistry, materializationService);
        final SplitExecutionCoordinator<K, V> splitCoordinator =
                new SplitExecutionCoordinatorImpl<>(keyToSegmentMap,
                        Vldtn.requireNonNull(segmentTopology,
                                "segmentTopology"),
                        routeSplitCoordinator, routeSplitPublishCoordinator,
                        Vldtn.requireNonNull(splitExecutor, "splitExecutor"),
                        failureReporter, telemetry);
        final SplitPolicyCoordinator<K, V> splitPolicyCoordinator =
                new SplitPolicyCoordinator<>(validatedConf,
                        Vldtn.requireNonNull(runtimeTuningState,
                                "runtimeTuningState"),
                        keyToSegmentMap, validatedSegmentRegistry,
                        splitCoordinator,
                        Vldtn.requireNonNull(workerExecutor, "workerExecutor"),
                        Vldtn.requireNonNull(splitPolicyScheduler,
                                "splitPolicyScheduler"),
                        Vldtn.requireNonNull(stateSupplier, "stateSupplier"),
                        failureReporter, telemetry, new SplitPolicyState(),
                        new SplitCandidateRegistry());
        final ManagedSplitRuntimeState managedState =
                new ManagedSplitRuntimeState();
        managedState.markRunning();
        return new SplitServiceImpl<>(splitCoordinator, splitPolicyCoordinator,
                managedState);
    }
}
