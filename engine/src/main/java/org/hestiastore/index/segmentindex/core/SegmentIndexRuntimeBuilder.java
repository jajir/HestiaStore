package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.split.PartitionStableSplitCoordinator;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;

/**
 * Builds the runtime collaborator graph used by {@link SegmentIndexRuntime}.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexRuntimeBuilder<K, V> {

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
    private final IndexExecutorRegistry executorRegistry;
    private final Stats stats;
    private final AtomicLong compactRequestHighWaterMark;
    private final AtomicLong flushRequestHighWaterMark;
    private final AtomicLong lastAppliedWalLsn;
    private final Callbacks callbacks;

    SegmentIndexRuntimeBuilder(final Logger logger,
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexExecutorRegistry executorRegistry, final Stats stats,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn, final Callbacks callbacks) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
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
    }

    SegmentIndexRuntime<K, V> build() {
        final RuntimeTuningState runtimeTuningState = RuntimeTuningState
                .fromConfiguration(conf);
        final KeyToSegmentMap<K> keyToSegmentMapDelegate = new KeyToSegmentMap<>(
                directoryFacade, keyTypeDescriptor);
        final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMapDelegate);
        final SegmentFactory<K, V> segmentFactory = new SegmentFactory<>(
                directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf,
                executorRegistry.getStableSegmentMaintenanceExecutor());
        final SegmentRegistry<K, V> segmentRegistry = SegmentRegistry.<K, V>builder()
                .withDirectoryFacade(directoryFacade)
                .withKeyTypeDescriptor(keyTypeDescriptor)
                .withValueTypeDescriptor(valueTypeDescriptor)
                .withConfiguration(conf)
                .withSegmentMaintenanceExecutor(
                        executorRegistry.getStableSegmentMaintenanceExecutor())
                .withRegistryMaintenanceExecutor(
                        executorRegistry.getRegistryMaintenanceExecutor())
                .build();
        final PartitionRuntime<K, V> partitionRuntime = new PartitionRuntime<>(
                keyTypeDescriptor.getComparator());
        final PartitionStableSplitCoordinator<K, V> splitCoordinator = new PartitionStableSplitCoordinator<>(
                conf, keyTypeDescriptor.getComparator(), keyToSegmentMap,
                segmentRegistry, partitionRuntime);
        final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator = new BackgroundSplitCoordinator<>(
                keyToSegmentMap, partitionRuntime, splitCoordinator,
                executorRegistry.getSplitMaintenanceExecutor(),
                callbacks.failureHandler(),
                callbacks.onBackgroundSplitApplied());
        final SegmentIndexCore<K, V> core = new SegmentIndexCore<>(
                keyToSegmentMap, segmentRegistry);
        final Executor drainExecutor = executorRegistry
                .getIndexMaintenanceExecutor();
        final IndexRetryPolicy retryPolicy = new IndexRetryPolicy(
                conf.getIndexBusyBackoffMillis(),
                conf.getIndexBusyTimeoutMillis());
        final StableSegmentCoordinator<K, V> stableSegmentCoordinator = new StableSegmentCoordinator<>(
                logger, keyToSegmentMap, segmentRegistry,
                backgroundSplitCoordinator, core, retryPolicy, stats);
        final BackgroundSplitPolicyLoop<K, V> backgroundSplitPolicyLoop = new BackgroundSplitPolicyLoop<>(
                conf, runtimeTuningState, keyToSegmentMap, segmentRegistry,
                partitionRuntime, backgroundSplitCoordinator, drainExecutor,
                executorRegistry.getSplitPolicyScheduler(), stats,
                callbacks.stateSupplier(), callbacks.awaitSplitsIdle(),
                callbacks.failureHandler());
        final PartitionDrainCoordinator<K, V> partitionDrainCoordinator = new PartitionDrainCoordinator<>(
                partitionRuntime, keyToSegmentMap, drainExecutor, retryPolicy,
                stableSegmentCoordinator, stats,
                backgroundSplitPolicyLoop::scheduleHint,
                callbacks.failureHandler());
        final PartitionWriteCoordinator<K, V> partitionWriteCoordinator = new PartitionWriteCoordinator<>(
                keyToSegmentMap, partitionRuntime, runtimeTuningState,
                backgroundSplitCoordinator,
                partitionDrainCoordinator::scheduleDrain);
        final PartitionReadCoordinator<K, V> partitionReadCoordinator = new PartitionReadCoordinator<>(
                keyToSegmentMap, partitionRuntime, segmentRegistry, core,
                backgroundSplitCoordinator, keyTypeDescriptor,
                valueTypeDescriptor, retryPolicy);
        final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator = new IndexRecoveryCleanupCoordinator<>(
                logger, directoryFacade, keyToSegmentMap, segmentRegistry,
                retryPolicy);
        final WalRuntime<K, V> walRuntime = WalRuntime.open(directoryFacade,
                conf.getWal(), keyTypeDescriptor, valueTypeDescriptor);
        final IndexWalCoordinator<K, V> walCoordinator = new IndexWalCoordinator<>(
                logger, conf, walRuntime, retryPolicy,
                () -> partitionDrainCoordinator.drainPartitions(true), () -> {
                    stableSegmentCoordinator.flushSegments(true);
                    keyToSegmentMap.optionalyFlush();
                }, callbacks.stateSupplier(), callbacks.failureHandler(),
                lastAppliedWalLsn);
        final IndexOperationCoordinator<K, V> operationCoordinator = new IndexOperationCoordinator<>(
                valueTypeDescriptor, stats, partitionWriteCoordinator,
                partitionReadCoordinator, walCoordinator, retryPolicy);
        final IndexMaintenanceCoordinator<K, V> maintenanceCoordinator = new IndexMaintenanceCoordinator<>(
                keyToSegmentMap, partitionRuntime, partitionDrainCoordinator,
                backgroundSplitCoordinator, backgroundSplitPolicyLoop,
                stableSegmentCoordinator, walCoordinator);
        final SegmentIndexMetricsCollector<K, V> metricsCollector = new SegmentIndexMetricsCollector<>(
                conf, keyToSegmentMap, segmentRegistry, partitionRuntime,
                runtimeTuningState, walRuntime, stats,
                compactRequestHighWaterMark, flushRequestHighWaterMark,
                lastAppliedWalLsn, callbacks.stateSupplier());
        final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier = new SegmentRuntimeLimitApplier<>(
                segmentRegistry, segmentFactory);
        final IndexControlPlane controlPlane = new IndexRuntimeControlPlane(
                conf, runtimeTuningState, callbacks.stateSupplier(),
                metricsCollector::metricsSnapshot, runtimeLimitApplier::apply,
                backgroundSplitPolicyLoop::scheduleScan);
        return new SegmentIndexRuntime<>(runtimeTuningState, keyToSegmentMap,
                segmentFactory, segmentRegistry, backgroundSplitCoordinator,
                backgroundSplitPolicyLoop, core, stableSegmentCoordinator,
                partitionDrainCoordinator, partitionWriteCoordinator,
                partitionReadCoordinator, maintenanceCoordinator,
                recoveryCleanupCoordinator, retryPolicy, walRuntime,
                metricsCollector, walCoordinator, operationCoordinator,
                controlPlane, runtimeLimitApplier);
    }
}
