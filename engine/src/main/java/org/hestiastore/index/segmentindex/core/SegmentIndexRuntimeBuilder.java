package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.split.RouteSplitCoordinator;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.slf4j.Logger;

/**
 * Builds the runtime collaborator graph used by {@link SegmentIndexRuntime}.
 *
 * @param <K> key type
 * @param <V> value type
 */
@SuppressWarnings({ "java:S6206", "java:S6539" })
final class SegmentIndexRuntimeBuilder<K, V> {

    interface BuildObserver<K, V> {

        default void onKeyToSegmentMapCreated(
                final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap) {
        }

        default void onSegmentRegistryCreated(
                final SegmentRegistry<K, V> segmentRegistry) {
        }

        default void onWalRuntimeCreated(final WalRuntime<K, V> walRuntime) {
        }
    }

    static <K, V> BuildObserver<K, V> noOpBuildObserver() {
        return new BuildObserver<>() {
        };
    }

    /**
     * Callback bundle used while assembling runtime collaborators.
     */
    static final class Callbacks {

        private final Supplier<SegmentIndexState> stateSupplier;
        private final Runnable awaitSplitsIdle;
        private final Consumer<RuntimeException> failureHandler;
        private final Runnable onBackgroundSplitApplied;

        Callbacks(final Supplier<SegmentIndexState> stateSupplier,
                final Runnable awaitSplitsIdle,
                final Consumer<RuntimeException> failureHandler,
                final Runnable onBackgroundSplitApplied) {
            this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                    "stateSupplier");
            this.awaitSplitsIdle = Vldtn.requireNonNull(awaitSplitsIdle,
                    "awaitSplitsIdle");
            this.failureHandler = Vldtn.requireNonNull(failureHandler,
                    "failureHandler");
            this.onBackgroundSplitApplied = Vldtn.requireNonNull(
                    onBackgroundSplitApplied, "onBackgroundSplitApplied");
        }

        Supplier<SegmentIndexState> stateSupplier() {
            return stateSupplier;
        }

        Runnable awaitSplitsIdle() {
            return awaitSplitsIdle;
        }

        Consumer<RuntimeException> failureHandler() {
            return failureHandler;
        }

        Runnable onBackgroundSplitApplied() {
            return onBackgroundSplitApplied;
        }
    }

    private final Logger logger;
    private final Directory directoryFacade;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final IndexConfiguration<K, V> conf;
    private final IndexRuntimeConfiguration<K, V> runtimeConfiguration;
    private final IndexExecutorRegistry executorRegistry;
    private final Stats stats;
    private final AtomicLong compactRequestHighWaterMark;
    private final AtomicLong flushRequestHighWaterMark;
    private final AtomicLong lastAppliedWalLsn;
    private final Callbacks callbacks;
    private final BuildObserver<K, V> buildObserver;

    @SuppressWarnings("java:S107")
    SegmentIndexRuntimeBuilder(final Logger logger,
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexRuntimeConfiguration<K, V> runtimeConfiguration,
            final IndexExecutorRegistry executorRegistry, final Stats stats,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn, final Callbacks callbacks,
            final BuildObserver<K, V> buildObserver) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.runtimeConfiguration = Vldtn.requireNonNull(runtimeConfiguration,
                "runtimeConfiguration");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.compactRequestHighWaterMark = Vldtn.requireNonNull(
                compactRequestHighWaterMark, "compactRequestHighWaterMark");
        this.flushRequestHighWaterMark = Vldtn.requireNonNull(
                flushRequestHighWaterMark, "flushRequestHighWaterMark");
        this.lastAppliedWalLsn = Vldtn.requireNonNull(lastAppliedWalLsn,
                "lastAppliedWalLsn");
        this.callbacks = Vldtn.requireNonNull(callbacks, "callbacks");
        this.buildObserver = Vldtn.requireNonNull(buildObserver,
                "buildObserver");
    }

    SegmentIndexRuntime<K, V> build() {
        KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap = null;
        SegmentRegistry<K, V> segmentRegistry = null;
        WalRuntime<K, V> walRuntime = null;
        try {
            final RuntimeTuningState runtimeTuningState = RuntimeTuningState
                    .fromConfiguration(conf);
            final KeyToSegmentMap<K> keyToSegmentMapDelegate = new KeyToSegmentMap<>(
                    directoryFacade, keyTypeDescriptor);
            keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                    keyToSegmentMapDelegate);
            buildObserver.onKeyToSegmentMapCreated(keyToSegmentMap);
            final SegmentFactory<K, V> segmentFactory = newSegmentFactory();
            segmentRegistry = newSegmentRegistry();
            buildObserver.onSegmentRegistryCreated(segmentRegistry);
            final RouteSplitCoordinator<K, V> routeSplitCoordinator = new RouteSplitCoordinator<>(
                    conf, keyTypeDescriptor.getComparator(), keyToSegmentMap,
                    segmentRegistry);
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator = new BackgroundSplitCoordinator<>(
                    keyToSegmentMap, routeSplitCoordinator,
                    executorRegistry.getSplitMaintenanceExecutor(),
                    callbacks.failureHandler(),
                    callbacks.onBackgroundSplitApplied(), stats);
            final StableSegmentGateway<K, V> stableSegmentGateway = new StableSegmentGateway<>(
                    keyToSegmentMap, segmentRegistry);
            final IndexRetryPolicy retryPolicy = newRetryPolicy();
            final StableSegmentCoordinator<K, V> stableSegmentCoordinator = new StableSegmentCoordinator<>(
                    logger, keyToSegmentMap, segmentRegistry,
                    backgroundSplitCoordinator, stableSegmentGateway,
                    retryPolicy, stats);
            final BackgroundSplitPolicyLoop<K, V> backgroundSplitPolicyLoop = new BackgroundSplitPolicyLoop<>(
                    conf, runtimeTuningState, keyToSegmentMap, segmentRegistry,
                    backgroundSplitCoordinator,
                    executorRegistry.getIndexMaintenanceExecutor(),
                    executorRegistry.getSplitPolicyScheduler(),
                    stats, callbacks.stateSupplier(),
                    callbacks.awaitSplitsIdle(),
                    callbacks.failureHandler());
            final DirectSegmentWriteCoordinator<K, V> directSegmentWriteCoordinator = new DirectSegmentWriteCoordinator<>(
                    keyToSegmentMap, stableSegmentGateway,
                    backgroundSplitCoordinator);
            final DirectSegmentReadCoordinator<K, V> directSegmentReadCoordinator = new DirectSegmentReadCoordinator<>(
                    keyToSegmentMap, segmentRegistry, stableSegmentGateway,
                    backgroundSplitCoordinator, retryPolicy);
            final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator = new IndexRecoveryCleanupCoordinator<>(
                    logger, directoryFacade, keyToSegmentMap, segmentRegistry,
                    retryPolicy);
            walRuntime = WalRuntime.open(directoryFacade, conf.getWal(),
                    keyTypeDescriptor, valueTypeDescriptor);
            buildObserver.onWalRuntimeCreated(walRuntime);
            final KeyToSegmentMapSynchronizedAdapter<K> builtKeyToSegmentMap = keyToSegmentMap;
            final IndexWalCoordinator<K, V> walCoordinator = new IndexWalCoordinator<>(
                    logger, conf, walRuntime, retryPolicy,
                    () -> {
                    },
                    () -> {
                        stableSegmentCoordinator.flushSegments(true);
                        builtKeyToSegmentMap.optionalyFlush();
                    }, callbacks.stateSupplier(), callbacks.failureHandler(),
                    lastAppliedWalLsn);
            final IndexOperationCoordinator<K, V> operationCoordinator = new IndexOperationCoordinator<>(
                    valueTypeDescriptor, stats,
                    directSegmentWriteCoordinator, directSegmentReadCoordinator,
                    walCoordinator, retryPolicy);
            final IndexMaintenanceCoordinator<K, V> maintenanceCoordinator = new IndexMaintenanceCoordinator<>(
                    keyToSegmentMap, backgroundSplitCoordinator,
                    backgroundSplitPolicyLoop, stableSegmentCoordinator,
                    walCoordinator);
            final SegmentIndexMetricsCollector<K, V> metricsCollector = newMetricsCollector(
                    keyToSegmentMap, segmentRegistry, backgroundSplitCoordinator,
                    runtimeTuningState, walRuntime);
            final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier = new SegmentRuntimeLimitApplier<>(
                    segmentRegistry, segmentFactory);
            return new SegmentIndexRuntime<>(runtimeTuningState,
                    keyToSegmentMap, segmentRegistry, backgroundSplitCoordinator,
                    backgroundSplitPolicyLoop, stableSegmentCoordinator,
                    directSegmentReadCoordinator, maintenanceCoordinator,
                    recoveryCleanupCoordinator, retryPolicy, walRuntime,
                    metricsCollector, walCoordinator, operationCoordinator,
                    new IndexRuntimeControlPlane(conf, runtimeTuningState,
                            callbacks.stateSupplier(),
                            metricsCollector::metricsSnapshot,
                            runtimeLimitApplier::apply,
                            backgroundSplitPolicyLoop::scheduleScan),
                    runtimeLimitApplier);
        } catch (final RuntimeException failure) {
            throw cleanupFailedBuild(failure, walRuntime, segmentRegistry,
                    keyToSegmentMap);
        }
    }

    private SegmentFactory<K, V> newSegmentFactory() {
        return SegmentFactory.withRuntimeConfiguration(directoryFacade,
                keyTypeDescriptor, valueTypeDescriptor, conf,
                runtimeConfiguration,
                executorRegistry.getStableSegmentMaintenanceExecutor());
    }

    private SegmentRegistry<K, V> newSegmentRegistry() {
        return SegmentRegistry.<K, V>builder()
                .withDirectoryFacade(directoryFacade)
                .withKeyTypeDescriptor(keyTypeDescriptor)
                .withValueTypeDescriptor(valueTypeDescriptor)
                .withConfiguration(conf)
                .withRuntimeConfiguration(runtimeConfiguration)
                .withSegmentMaintenanceExecutor(
                        executorRegistry.getStableSegmentMaintenanceExecutor())
                .withRegistryMaintenanceExecutor(
                        executorRegistry.getRegistryMaintenanceExecutor())
                .build();
    }

    private IndexRetryPolicy newRetryPolicy() {
        return new IndexRetryPolicy(conf.getIndexBusyBackoffMillis(),
                conf.getIndexBusyTimeoutMillis());
    }

    private SegmentIndexMetricsCollector<K, V> newMetricsCollector(
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final RuntimeTuningState runtimeTuningState,
            final WalRuntime<K, V> walRuntime) {
        return new SegmentIndexMetricsCollector<>(conf, keyToSegmentMap,
                segmentRegistry, backgroundSplitCoordinator,
                executorRegistry, runtimeTuningState, walRuntime, stats,
                compactRequestHighWaterMark, flushRequestHighWaterMark,
                lastAppliedWalLsn,
                callbacks.stateSupplier());
    }

    private RuntimeException cleanupFailedBuild(final RuntimeException failure,
            final WalRuntime<K, V> walRuntime,
            final SegmentRegistry<K, V> segmentRegistry,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap) {
        closeWalRuntime(walRuntime, failure);
        closeSegmentRegistry(segmentRegistry, failure);
        closeKeyToSegmentMap(keyToSegmentMap, failure);
        return failure;
    }

    private void closeWalRuntime(final WalRuntime<K, V> walRuntime,
            final RuntimeException failure) {
        if (walRuntime == null) {
            return;
        }
        try {
            walRuntime.close();
        } catch (final RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private void closeSegmentRegistry(final SegmentRegistry<K, V> segmentRegistry,
            final RuntimeException failure) {
        if (segmentRegistry == null) {
            return;
        }
        try {
            final SegmentRegistryResult<Void> closeResult = segmentRegistry
                    .close();
            if (closeResult == null) {
                failure.addSuppressed(new IllegalStateException(
                        "Segment registry close returned null during runtime cleanup."));
                return;
            }
            final SegmentRegistryResultStatus status = closeResult.getStatus();
            if (status != SegmentRegistryResultStatus.OK
                    && status != SegmentRegistryResultStatus.CLOSED) {
                failure.addSuppressed(new IllegalStateException(String.format(
                        "Segment registry close failed during runtime cleanup: %s",
                        status)));
            }
        } catch (final RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private void closeKeyToSegmentMap(
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final RuntimeException failure) {
        if (keyToSegmentMap == null || keyToSegmentMap.wasClosed()) {
            return;
        }
        try {
            keyToSegmentMap.close();
        } catch (final RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }
}
