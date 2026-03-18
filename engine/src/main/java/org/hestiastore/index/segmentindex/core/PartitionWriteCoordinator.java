package org.hestiastore.index.segmentindex.core;

import java.util.function.Consumer;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.partition.PartitionRuntimeLimits;
import org.hestiastore.index.segmentindex.partition.PartitionWriteResult;
import org.hestiastore.index.segmentindex.partition.PartitionWriteResultStatus;

/**
 * Owns routed writes into the partition runtime and applies current runtime
 * limits.
 */
final class PartitionWriteCoordinator<K, V> {

    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final PartitionRuntime<K, V> partitionRuntime;
    private final RuntimeTuningState runtimeTuningState;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final Consumer<SegmentId> drainScheduler;

    PartitionWriteCoordinator(
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final PartitionRuntime<K, V> partitionRuntime,
            final RuntimeTuningState runtimeTuningState,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final Consumer<SegmentId> drainScheduler) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.partitionRuntime = Vldtn.requireNonNull(partitionRuntime,
                "partitionRuntime");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
        this.drainScheduler = Vldtn.requireNonNull(drainScheduler,
                "drainScheduler");
    }

    IndexResult<Void> putBuffered(final K key, final V value) {
        return backgroundSplitCoordinator.runWithStableWriteAdmission(() -> {
            final IndexResult<SegmentId> routeResult = resolveWriteSegmentId(
                    key);
            if (routeResult.getStatus() != IndexResultStatus.OK
                    || routeResult.getValue() == null) {
                return toVoidResult(routeResult.getStatus());
            }
            final SegmentId segmentId = routeResult.getValue();
            partitionRuntime.ensurePartition(segmentId);
            final PartitionWriteResult writeResult = partitionRuntime.write(
                    segmentId, key, value, currentPartitionRuntimeLimits());
            if (writeResult.getStatus() == PartitionWriteResultStatus.BUSY) {
                return IndexResult.busy();
            }
            if (writeResult.isDrainRecommended()) {
                drainScheduler.accept(segmentId);
            }
            return IndexResult.ok();
        });
    }

    private IndexResult<SegmentId> resolveWriteSegmentId(final K key) {
        final KeyToSegmentMap.Snapshot<K> snapshot = keyToSegmentMap.snapshot();
        if (snapshot.findSegmentId(key) == null
                && !keyToSegmentMap.tryExtendMaxKey(key, snapshot)) {
            return IndexResult.busy();
        }
        final KeyToSegmentMap.Snapshot<K> stableSnapshot = keyToSegmentMap
                .snapshot();
        final SegmentId segmentId = stableSnapshot.findSegmentId(key);
        if (segmentId == null) {
            return IndexResult.busy();
        }
        return IndexResult.ok(segmentId);
    }

    private PartitionRuntimeLimits currentPartitionRuntimeLimits() {
        final int maxActive = Math.max(1, runtimeTuningState.effectiveValue(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION));
        final int maxImmutableRuns = Math.max(1, runtimeTuningState
                .effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION));
        final int maxPartitionBuffer = Math.max(maxActive + 1,
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER));
        final int maxIndexBuffer = Math.max(maxPartitionBuffer, runtimeTuningState
                .effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER));
        return new PartitionRuntimeLimits(maxActive, maxImmutableRuns,
                maxPartitionBuffer, maxIndexBuffer);
    }

    private static IndexResult<Void> toVoidResult(
            final IndexResultStatus status) {
        if (status == IndexResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (status == IndexResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (status == IndexResultStatus.OK) {
            return IndexResult.ok();
        }
        return IndexResult.error();
    }
}
