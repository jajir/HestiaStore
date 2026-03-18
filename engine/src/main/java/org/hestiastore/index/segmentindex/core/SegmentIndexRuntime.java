package org.hestiastore.index.segmentindex.core;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.control.model.RuntimeSettingKey;
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
 * Aggregates runtime collaborators and centralizes their creation.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexRuntime<K, V> {

    private final RuntimeTuningState runtimeTuningState;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentFactory<K, V> segmentFactory;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final BackgroundSplitPolicyLoop<K, V> backgroundSplitPolicyLoop;
    private final SegmentIndexCore<K, V> core;
    private final StableSegmentCoordinator<K, V> stableSegmentCoordinator;
    private final PartitionDrainCoordinator<K, V> partitionDrainCoordinator;
    private final PartitionWriteCoordinator<K, V> partitionWriteCoordinator;
    private final PartitionReadCoordinator<K, V> partitionReadCoordinator;
    private final IndexMaintenanceCoordinator<K, V> maintenanceCoordinator;
    private final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator;
    private final IndexRetryPolicy retryPolicy;
    private final WalRuntime<K, V> walRuntime;
    private final SegmentIndexMetricsCollector<K, V> metricsCollector;
    private final IndexWalCoordinator<K, V> walCoordinator;
    private final IndexControlPlane controlPlane;
    private final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier;

    private SegmentIndexRuntime(
            final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentFactory<K, V> segmentFactory,
            final SegmentRegistry<K, V> segmentRegistry,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final BackgroundSplitPolicyLoop<K, V> backgroundSplitPolicyLoop,
            final SegmentIndexCore<K, V> core,
            final StableSegmentCoordinator<K, V> stableSegmentCoordinator,
            final PartitionDrainCoordinator<K, V> partitionDrainCoordinator,
            final PartitionWriteCoordinator<K, V> partitionWriteCoordinator,
            final PartitionReadCoordinator<K, V> partitionReadCoordinator,
            final IndexMaintenanceCoordinator<K, V> maintenanceCoordinator,
            final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator,
            final IndexRetryPolicy retryPolicy,
            final WalRuntime<K, V> walRuntime,
            final SegmentIndexMetricsCollector<K, V> metricsCollector,
            final IndexWalCoordinator<K, V> walCoordinator,
            final IndexControlPlane controlPlane,
            final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier) {
        this.runtimeTuningState = runtimeTuningState;
        this.keyToSegmentMap = keyToSegmentMap;
        this.segmentFactory = segmentFactory;
        this.segmentRegistry = segmentRegistry;
        this.backgroundSplitCoordinator = backgroundSplitCoordinator;
        this.backgroundSplitPolicyLoop = backgroundSplitPolicyLoop;
        this.core = core;
        this.stableSegmentCoordinator = stableSegmentCoordinator;
        this.partitionDrainCoordinator = partitionDrainCoordinator;
        this.partitionWriteCoordinator = partitionWriteCoordinator;
        this.partitionReadCoordinator = partitionReadCoordinator;
        this.maintenanceCoordinator = maintenanceCoordinator;
        this.recoveryCleanupCoordinator = recoveryCleanupCoordinator;
        this.retryPolicy = retryPolicy;
        this.walRuntime = walRuntime;
        this.metricsCollector = metricsCollector;
        this.walCoordinator = walCoordinator;
        this.controlPlane = controlPlane;
        this.runtimeLimitApplier = runtimeLimitApplier;
    }

    static <K, V> SegmentIndexRuntime<K, V> open(final Logger logger,
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexExecutorRegistry executorRegistry, final Stats stats,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn,
            final Supplier<SegmentIndexState> stateSupplier,
            final Runnable awaitSplitsIdle,
            final Consumer<RuntimeException> failureHandler,
            final Runnable onBackgroundSplitApplied) {
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
                executorRegistry.getSplitMaintenanceExecutor(), failureHandler,
                onBackgroundSplitApplied);
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
                stateSupplier, awaitSplitsIdle, failureHandler);
        final PartitionDrainCoordinator<K, V> partitionDrainCoordinator = new PartitionDrainCoordinator<>(
                partitionRuntime, keyToSegmentMap, drainExecutor, retryPolicy,
                stableSegmentCoordinator, stats,
                backgroundSplitPolicyLoop::scheduleHint, failureHandler);
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
                }, stateSupplier, failureHandler, lastAppliedWalLsn);
        final IndexMaintenanceCoordinator<K, V> maintenanceCoordinator = new IndexMaintenanceCoordinator<>(
                keyToSegmentMap, partitionRuntime, partitionDrainCoordinator,
                backgroundSplitCoordinator, backgroundSplitPolicyLoop,
                stableSegmentCoordinator, walCoordinator);
        final SegmentIndexMetricsCollector<K, V> metricsCollector = new SegmentIndexMetricsCollector<>(
                conf, keyToSegmentMap, segmentRegistry, partitionRuntime,
                runtimeTuningState, walRuntime, stats,
                compactRequestHighWaterMark, flushRequestHighWaterMark,
                lastAppliedWalLsn, stateSupplier);
        final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier = new SegmentRuntimeLimitApplier<>(
                segmentRegistry, segmentFactory);
        final IndexControlPlane controlPlane = new IndexRuntimeControlPlane(
                conf, runtimeTuningState, stateSupplier,
                metricsCollector::metricsSnapshot, runtimeLimitApplier::apply,
                backgroundSplitPolicyLoop::scheduleScan);
        return new SegmentIndexRuntime<>(runtimeTuningState, keyToSegmentMap,
                segmentFactory, segmentRegistry, backgroundSplitCoordinator,
                backgroundSplitPolicyLoop, core, stableSegmentCoordinator,
                partitionDrainCoordinator, partitionWriteCoordinator,
                partitionReadCoordinator, maintenanceCoordinator,
                recoveryCleanupCoordinator, retryPolicy, walRuntime,
                metricsCollector, walCoordinator, controlPlane,
                runtimeLimitApplier);
    }

    RuntimeTuningState runtimeTuningState() {
        return runtimeTuningState;
    }

    KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap() {
        return keyToSegmentMap;
    }

    SegmentRegistry<K, V> segmentRegistry() {
        return segmentRegistry;
    }

    BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator() {
        return backgroundSplitCoordinator;
    }

    BackgroundSplitPolicyLoop<K, V> backgroundSplitPolicyLoop() {
        return backgroundSplitPolicyLoop;
    }

    StableSegmentCoordinator<K, V> stableSegmentCoordinator() {
        return stableSegmentCoordinator;
    }

    PartitionWriteCoordinator<K, V> partitionWriteCoordinator() {
        return partitionWriteCoordinator;
    }

    PartitionReadCoordinator<K, V> partitionReadCoordinator() {
        return partitionReadCoordinator;
    }

    IndexMaintenanceCoordinator<K, V> maintenanceCoordinator() {
        return maintenanceCoordinator;
    }

    IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator() {
        return recoveryCleanupCoordinator;
    }

    IndexRetryPolicy retryPolicy() {
        return retryPolicy;
    }

    WalRuntime<K, V> walRuntime() {
        return walRuntime;
    }

    SegmentIndexMetricsCollector<K, V> metricsCollector() {
        return metricsCollector;
    }

    IndexWalCoordinator<K, V> walCoordinator() {
        return walCoordinator;
    }

    IndexControlPlane controlPlane() {
        return controlPlane;
    }

    void applyRuntimeEffectiveLimits(
            final Map<RuntimeSettingKey, Integer> effective) {
        runtimeLimitApplier.apply(effective);
    }

    void invalidateSegmentIterators() {
        stableSegmentCoordinator.invalidateIterators();
    }

    void awaitSplitsIdle(final long timeoutMillis) {
        backgroundSplitCoordinator.awaitSplitsIdle(timeoutMillis);
    }

    void recover(final WalRuntime.ReplayConsumer<K, V> replayConsumer) {
        walCoordinator.recover(replayConsumer);
    }

    IndexCloseCoordinator newCloseCoordinator(final Logger logger,
            final String indexName, final Runnable beginCloseTransition,
            final Runnable awaitAsyncOperations, final Runnable markClosed,
            final LongSupplier getReadCount, final LongSupplier getWriteCount,
            final LongSupplier getDeleteCount,
            final Runnable finishCloseTransition) {
        return new IndexCloseCoordinator(logger, indexName, beginCloseTransition,
                awaitAsyncOperations,
                () -> partitionDrainCoordinator.drainPartitions(true),
                backgroundSplitPolicyLoop::awaitExhausted, markClosed,
                () -> backgroundSplitCoordinator
                        .runWithSplitSchedulingPaused(() -> stableSegmentCoordinator
                                .flushSegments(true)),
                segmentRegistry::close, keyToSegmentMap::optionalyFlush,
                walCoordinator::checkpoint, getReadCount, getWriteCount,
                getDeleteCount, finishCloseTransition, walRuntime::close);
    }
}
