package org.hestiastore.index.segmentindex.core.maintenance;

import java.util.function.LongSupplier;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.routing.StableSegmentAccess;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;

/**
 * Stable-segment runtime capability view used outside the maintenance package.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface StableSegmentMaintenanceAccess<K, V> {

    static <K, V> StableSegmentMaintenanceAccess<K, V> create(
            final Logger logger,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SplitMaintenanceSynchronization<K, V> splitSynchronization,
            final StableSegmentAccess<K, V> stableSegmentGateway,
            final IndexRetryPolicy retryPolicy, final Stats stats) {
        return create(logger, keyToSegmentMap, segmentRegistry,
                splitSynchronization, stableSegmentGateway, retryPolicy,
                stats, System::nanoTime);
    }

    static <K, V> StableSegmentMaintenanceAccess<K, V> create(
            final Logger logger,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SplitMaintenanceSynchronization<K, V> splitSynchronization,
            final StableSegmentAccess<K, V> stableSegmentGateway,
            final IndexRetryPolicy retryPolicy, final Stats stats,
            final LongSupplier nanoTimeSupplier) {
        return new StableSegmentCoordinator<>(
                Vldtn.requireNonNull(logger, "logger"),
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                Vldtn.requireNonNull(splitSynchronization,
                        "splitSynchronization"),
                Vldtn.requireNonNull(stableSegmentGateway,
                        "stableSegmentGateway"),
                Vldtn.requireNonNull(retryPolicy, "retryPolicy"),
                Vldtn.requireNonNull(stats, "stats"),
                Vldtn.requireNonNull(nanoTimeSupplier, "nanoTimeSupplier"));
    }

    void flushSegments(boolean waitForCompletion);

    void flushMappedSegmentsAndWait();

    void compactMappedSegmentsAndFlush();

    void compactSegment(SegmentId segmentId, boolean waitForCompletion);

    EntryIterator<K, V> openIteratorWithRetry(SegmentId segmentId,
            SegmentIteratorIsolation isolation);

    void invalidateIterators();
}
