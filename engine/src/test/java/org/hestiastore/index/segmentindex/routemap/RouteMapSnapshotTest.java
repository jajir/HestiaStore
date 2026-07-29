package org.hestiastore.index.segmentindex.routemap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.TreeMap;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class RouteMapSnapshotTest {

    @Test
    void containsSegmentIdFindsMappedSegment() {
        final TreeMap<Integer, SegmentId> routes = new TreeMap<>();
        routes.put(10, SegmentId.of(1));
        routes.put(20, SegmentId.of(2));
        final RouteMapSnapshot<Integer> snapshot = new RouteMapSnapshot<>(
                routes, 0);

        assertTrue(snapshot.containsSegmentId(SegmentId.of(2)));
        assertFalse(snapshot.containsSegmentId(SegmentId.of(3)));
    }

    @Test
    void containsSegmentIdRejectsNull() {
        final RouteMapSnapshot<Integer> snapshot = new RouteMapSnapshot<>(
                new TreeMap<>(), 0);

        assertThrows(IllegalArgumentException.class,
                () -> snapshot.containsSegmentId(null));
    }

    @Test
    void getSegmentIdsSelectsOnlyIntersectingRangeRoutes() {
        final RouteMapSnapshot<Integer> snapshot = snapshotWithThreeRoutes();

        assertEquals(List.of(SegmentId.of(2)),
                snapshot.getSegmentIds(11, 19));
        assertEquals(List.of(SegmentId.of(1), SegmentId.of(2)),
                snapshot.getSegmentIds(10, 20));
        assertEquals(List.of(SegmentId.of(2), SegmentId.of(3)),
                snapshot.getSegmentIds(11, 21));
    }

    @Test
    void getSegmentIdsSupportsOpenEndedTail() {
        final RouteMapSnapshot<Integer> snapshot = snapshotWithThreeRoutes();

        assertEquals(List.of(SegmentId.of(3)),
                snapshot.getSegmentIds(21, null));
        assertEquals(List.of(SegmentId.of(3)),
                snapshot.getSegmentIds(40, null));
    }

    @Test
    void getSegmentIdsReturnsEmptyForEmptyRouteMap() {
        final RouteMapSnapshot<Integer> snapshot = new RouteMapSnapshot<>(
                new TreeMap<>(), 0);

        assertEquals(List.of(), snapshot.getSegmentIds(10, 20));
    }

    private RouteMapSnapshot<Integer> snapshotWithThreeRoutes() {
        final TreeMap<Integer, SegmentId> routes = new TreeMap<>();
        routes.put(10, SegmentId.of(1));
        routes.put(20, SegmentId.of(2));
        routes.put(30, SegmentId.of(3));
        return new RouteMapSnapshot<>(routes, 0);
    }
}
