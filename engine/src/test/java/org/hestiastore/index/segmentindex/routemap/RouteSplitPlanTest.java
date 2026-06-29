package org.hestiastore.index.segmentindex.routemap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RouteSplitPlanTest {

    private SegmentId replacedSegmentId;
    private SegmentId lowerSegmentId;
    private SegmentId upperSegmentId;
    private RouteSplitPlan<Integer> routeSplit;

    @BeforeEach
    void setUp() {
        replacedSegmentId = SegmentId.of(1);
        lowerSegmentId = SegmentId.of(2);
        upperSegmentId = SegmentId.of(3);
        routeSplit = new RouteSplitPlan<>(replacedSegmentId, lowerSegmentId,
                upperSegmentId, 10, null);
    }

    @AfterEach
    void tearDown() {
        routeSplit = null;
        replacedSegmentId = null;
        lowerSegmentId = null;
        upperSegmentId = null;
    }

    @Test
    void getters_return_configured_values() {
        assertSame(replacedSegmentId, routeSplit.getReplacedSegmentId());
        assertSame(lowerSegmentId, routeSplit.getLowerSegmentId());
        assertSame(upperSegmentId, routeSplit.getUpperSegmentId());
        assertEquals(10, routeSplit.getLowerMaxKey());
        assertEquals(Optional.empty(), routeSplit.getUpperMaxKey());
    }

    @Test
    void getters_return_optional_upper_max_key() {
        final RouteSplitPlan<Integer> split = new RouteSplitPlan<>(
                replacedSegmentId, lowerSegmentId, upperSegmentId, 10, 20);

        assertEquals(Optional.of(20), split.getUpperMaxKey());
    }

    @Test
    void split_requires_upper_segment() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new RouteSplitPlan<>(replacedSegmentId, lowerSegmentId,
                        null, 10, null));
        assertEquals("Property 'upperSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_replaced_segment_id() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new RouteSplitPlan<>(null, lowerSegmentId, upperSegmentId,
                        10, null));
        assertEquals("Property 'replacedSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_lower_segment_id() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new RouteSplitPlan<>(replacedSegmentId, null,
                        upperSegmentId, 10, null));
        assertEquals("Property 'lowerSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_max_key() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new RouteSplitPlan<>(replacedSegmentId, lowerSegmentId,
                        upperSegmentId, null, null));
        assertEquals("Property 'lowerMaxKey' must not be null.",
                err.getMessage());
    }
}
