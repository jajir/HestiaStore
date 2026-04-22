package org.hestiastore.index.segmentindex.core.splitplanner;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.routing.BackgroundSplitCoordinator;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Dispatches planner-selected candidates into split execution admission.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SplitTaskDispatcher<K, V> {

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final Stats stats;

    public SplitTaskDispatcher(final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final Stats stats) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
        this.stats = Vldtn.requireNonNull(stats, "stats");
    }

    public boolean isCandidateMapped(final SegmentId segmentId) {
        return segmentId != null && keyToSegmentMap.getSegmentIds()
                .contains(segmentId);
    }

    public boolean dispatchAllMappedCandidates(final int splitThreshold,
            final boolean forceRetry) {
        return dispatchCandidates(keyToSegmentMap.getSegmentIds(),
                splitThreshold, forceRetry);
    }

    public boolean dispatchCandidates(final List<SegmentId> segmentIds,
            final int splitThreshold, final boolean forceRetry) {
        boolean scheduledAny = false;
        for (final SegmentId segmentId : segmentIds) {
            scheduledAny |= dispatchCandidate(segmentId, splitThreshold,
                    forceRetry);
        }
        return scheduledAny;
    }

    private boolean dispatchCandidate(final SegmentId segmentId,
            final int splitThreshold, final boolean forceRetry) {
        if (!isCandidateMapped(segmentId)) {
            return false;
        }
        final SegmentHandle<K, V> segmentHandle = tryLoadCandidate(segmentId);
        if (segmentHandle == null) {
            return false;
        }
        final boolean scheduled = backgroundSplitCoordinator
                .handleSplitCandidate(segmentHandle, splitThreshold,
                        forceRetry);
        if (scheduled) {
            stats.recordSplitScheduled();
        }
        return scheduled;
    }

    private SegmentHandle<K, V> tryLoadCandidate(final SegmentId segmentId) {
        try {
            final Optional<SegmentHandle<K, V>> loaded = segmentRegistry
                    .tryGetSegment(segmentId);
            if (loaded.isPresent()) {
                return loaded.get();
            }
            return null;
        } catch (final IndexException e) {
            if (!isCandidateMapped(segmentId)) {
                return null;
            }
            throw e;
        }
    }
}
