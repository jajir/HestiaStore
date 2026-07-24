package org.hestiastore.index.segmentindex.routemap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
