package org.hestiastore.index.segmentindex.core;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;

/**
 * Aggregates runtime collaborators created for a running index instance.
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
    private final StableSegmentGateway<K, V> stableSegmentGateway;
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
    private final IndexOperationCoordinator<K, V> operationCoordinator;
    private final IndexControlPlane controlPlane;
    private final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier;

    SegmentIndexRuntime(
            final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentFactory<K, V> segmentFactory,
            final SegmentRegistry<K, V> segmentRegistry,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final BackgroundSplitPolicyLoop<K, V> backgroundSplitPolicyLoop,
            final StableSegmentGateway<K, V> stableSegmentGateway,
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
            final IndexOperationCoordinator<K, V> operationCoordinator,
            final IndexControlPlane controlPlane,
            final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier) {
        this.runtimeTuningState = runtimeTuningState;
        this.keyToSegmentMap = keyToSegmentMap;
        this.segmentFactory = segmentFactory;
        this.segmentRegistry = segmentRegistry;
        this.backgroundSplitCoordinator = backgroundSplitCoordinator;
        this.backgroundSplitPolicyLoop = backgroundSplitPolicyLoop;
        this.stableSegmentGateway = stableSegmentGateway;
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
        this.operationCoordinator = operationCoordinator;
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
        return new SegmentIndexRuntimeBuilder<>(logger, directoryFacade,
                keyTypeDescriptor, valueTypeDescriptor, conf, executorRegistry,
                stats, compactRequestHighWaterMark,
                flushRequestHighWaterMark, lastAppliedWalLsn,
                new SegmentIndexRuntimeBuilder.Callbacks(stateSupplier,
                        awaitSplitsIdle, failureHandler,
                        onBackgroundSplitApplied))
                                .build();
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

    IndexOperationCoordinator<K, V> operationCoordinator() {
        return operationCoordinator;
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
