package org.hestiastore.index.segmentindex.core.storage;

import java.util.function.Predicate;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.RouteMapSnapshot;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies map-to-segment consistency and repairs empty mapped segments when
 * they are still empty under full iterator isolation.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class RouteMapConsistencyChecker<K, V> {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(RouteMapConsistencyChecker.class);
    private static final String ERROR_MSG = "Index is broken. "
            + "File 'index.map' containing information about segments is corrupted. ";
    private static final Predicate<SegmentId> ALL_SEGMENTS =
            segmentId -> true;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentRouteMap<K> keyToSegmentMap;

    RouteMapConsistencyChecker(final SegmentRouteMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
    }

    /**
     * Scans all segments and verifies map-to-segment consistency, attempting to
     * repair obvious corruption when possible.
     */
    void checkAndRepairConsistency() {
        checkAndRepairConsistency(ALL_SEGMENTS);
    }

    /**
     * Scans filtered segments and repairs empty mapped segments.
     *
     * @param segmentFilter predicate deciding which mapped segments are checked
     */
    void checkAndRepairConsistency(final Predicate<SegmentId> segmentFilter) {
        final RouteMapSnapshot<K> snapshot = keyToSegmentMap.snapshot();
        final Predicate<SegmentId> nonNullSegmentFilter = Vldtn.requireNonNull(
                segmentFilter, "segmentFilter");
        for (final SegmentId segmentId : snapshot.getSegmentIds(
                SegmentWindow.unbounded())) {
            if (segmentId == null) {
                throw new IndexException(ERROR_MSG + "Segment id is null.");
            }
            if (!nonNullSegmentFilter.test(segmentId)) {
                LOGGER.debug("Skipping consistency check for segment '{}'.",
                        segmentId);
                continue;
            }
            LOGGER.debug("checking segment '{}'.", segmentId);
            final BlockingSegment<K, V> segment = awaitLoadedSegment(segmentId);
            final K maxKey = segment.checkAndRepairConsistency();
            if (maxKey == null) {
                removeEmptySegment(segmentId, segment);
                continue;
            }
            final SegmentId routedSegmentId = snapshot.findSegmentIdForKey(
                    maxKey);
            if (!segmentId.equals(routedSegmentId)) {
                throw new IndexException(String.format(ERROR_MSG
                        + "Segment '%s' contains max key '%s', which routes to segment '%s' in the index map.",
                        segmentId, maxKey, routedSegmentId));
            }
            LOGGER.debug("Checking segment '{}' id done.", segmentId);
        }
    }

    private void removeEmptySegment(final SegmentId segmentId,
            final BlockingSegment<K, V> segment) {
        if (!confirmEmptyUnderIsolation(segment)) {
            return;
        }
        LOGGER.warn("Segment '{}' is empty. Removing it from index map.",
                segmentId);
        keyToSegmentMap.removeSegmentRoute(segmentId);
        keyToSegmentMap.flushIfDirty();
        segmentRegistry.deleteSegment(segmentId);
    }

    private boolean confirmEmptyUnderIsolation(
            final BlockingSegment<K, V> segment) {
        try (EntryIterator<K, V> iterator = segment
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION)) {
            return !iterator.hasNext();
        }
    }

    private BlockingSegment<K, V> awaitLoadedSegment(final SegmentId segmentId) {
        try {
            return segmentRegistry.loadSegment(segmentId);
        } catch (final IndexException e) {
            throw new IndexException(String.format(
                    ERROR_MSG + "Segment '%s' is not found in index.",
                    segmentId), e);
        }
    }

}
