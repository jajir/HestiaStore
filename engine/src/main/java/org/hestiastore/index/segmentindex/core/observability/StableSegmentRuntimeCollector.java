package org.hestiastore.index.segmentindex.core.observability;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Collects runtime metrics for stable segments that are still mapped by the
 * index routing table.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class StableSegmentRuntimeCollector<K, V> {

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;

    StableSegmentRuntimeCollector(final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
    }

    StableSegmentRuntimeMetrics collect() {
        final StableSegmentRuntimeMetrics metrics =
                new StableSegmentRuntimeMetrics();
        final List<SegmentId> mappedSegmentIds = mappedSegmentIds();
        metrics.setTotalMappedStableSegmentCount(mappedSegmentIds.size());
        if (mappedSegmentIds.isEmpty()) {
            return metrics;
        }
        final Set<SegmentId> mappedSegmentIdSet = new HashSet<>(
                mappedSegmentIds);
        final int accountedMappedSegmentCount = collectLoadedMappedSegments(
                mappedSegmentIdSet, metrics);
        metrics.setUnloadedMappedStableSegmentCount(
                metrics.getTotalMappedStableSegmentCount()
                        - accountedMappedSegmentCount);
        return metrics;
    }

    private List<SegmentId> mappedSegmentIds() {
        return keyToSegmentMap.getSegmentIds();
    }

    private int collectLoadedMappedSegments(final Set<SegmentId> mappedSegmentIdSet,
            final StableSegmentRuntimeMetrics metrics) {
        int accountedMappedSegmentCount = 0;
        for (final SegmentHandle<K, V> segmentHandle : loadedSegmentsSnapshot()) {
            if (!isMappedLoadedSegment(segmentHandle, mappedSegmentIdSet)) {
                continue;
            }
            accountedMappedSegmentCount++;
            accumulateMappedSegmentMetrics(metrics,
                    runtimeSnapshot(segmentHandle));
        }
        return accountedMappedSegmentCount;
    }

    private List<SegmentHandle<K, V>> loadedSegmentsSnapshot() {
        return segmentRegistry.runtime().loadedSegmentsSnapshot();
    }

    private boolean isMappedLoadedSegment(final SegmentHandle<K, V> segmentHandle,
            final Set<SegmentId> mappedSegmentIdSet) {
        if (segmentHandle == null) {
            return false;
        }
        return mappedSegmentIdSet
                .contains(runtimeSnapshot(segmentHandle).getSegmentId());
    }

    private SegmentRuntimeSnapshot runtimeSnapshot(
            final SegmentHandle<K, V> segmentHandle) {
        return segmentHandle.getRuntime().getRuntimeSnapshot();
    }

    private void accumulateMappedSegmentMetrics(
            final StableSegmentRuntimeMetrics metrics,
            final SegmentRuntimeSnapshot segmentRuntime) {
        accumulateStateMetrics(metrics, segmentRuntime.getState());
        metrics.addSegmentRuntimeSnapshot(segmentRuntime);
    }

    private void accumulateStateMetrics(
            final StableSegmentRuntimeMetrics metrics,
            final SegmentState state) {
        if (state == SegmentState.READY) {
            metrics.incrementReadyStableSegmentCount();
            return;
        }
        if (isMaintenanceState(state)) {
            metrics.incrementStableSegmentsInMaintenanceStateCount();
            return;
        }
        if (state == SegmentState.ERROR) {
            metrics.incrementErrorStableSegmentCount();
            return;
        }
        if (state == SegmentState.CLOSED) {
            metrics.incrementClosedStableSegmentCount();
        }
    }

    private boolean isMaintenanceState(final SegmentState state) {
        return state == SegmentState.MAINTENANCE_RUNNING
                || state == SegmentState.FREEZE;
    }
}
