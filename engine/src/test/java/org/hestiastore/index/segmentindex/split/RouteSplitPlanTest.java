package org.hestiastore.index.segmentindex.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RouteSplitPlanTest {

    private SegmentId replacedSegmentId;
    private SegmentId lowerSegmentId;
    private SegmentId upperSegmentId;
    private RouteSplitPlan<Integer> splitPlan;

    @BeforeEach
    void setUp() {
        replacedSegmentId = SegmentId.of(1);
        lowerSegmentId = SegmentId.of(2);
        upperSegmentId = SegmentId.of(3);
        splitPlan = new RouteSplitPlan<>(replacedSegmentId, lowerSegmentId,
                upperSegmentId, 1, 10, RouteSplitPlan.SplitMode.SPLIT);
    }

    @AfterEach
    void tearDown() {
        splitPlan = null;
        replacedSegmentId = null;
        lowerSegmentId = null;
        upperSegmentId = null;
    }

    @Test
    void getters_return_configured_values() {
        assertSame(replacedSegmentId, splitPlan.getReplacedSegmentId());
        assertSame(lowerSegmentId, splitPlan.getLowerSegmentId());
        assertTrue(splitPlan.getUpperSegmentId().isPresent());
        assertSame(upperSegmentId, splitPlan.getUpperSegmentId().get());
        assertEquals(1, splitPlan.getLowerMinKey());
        assertEquals(10, splitPlan.getLowerMaxKey());
        assertEquals(RouteSplitPlan.SplitMode.SPLIT, splitPlan.getSplitMode());
        assertTrue(splitPlan.isSplit());
    }

    @Test
    void compacted_allows_missing_upper_segment() {
        final RouteSplitPlan<Integer> compacted = new RouteSplitPlan<>(
                replacedSegmentId, lowerSegmentId, null, 1, 10,
                RouteSplitPlan.SplitMode.COMPACTED);
        assertFalse(compacted.getUpperSegmentId().isPresent());
        assertEquals(RouteSplitPlan.SplitMode.COMPACTED,
                compacted.getSplitMode());
        assertFalse(compacted.isSplit());
    }

    @Test
    void split_requires_upper_segment() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new RouteSplitPlan<>(replacedSegmentId, lowerSegmentId,
                        null, 1, 10, RouteSplitPlan.SplitMode.SPLIT));
        assertEquals("Property 'upperSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_replaced_segment_id() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new RouteSplitPlan<>(null, lowerSegmentId, upperSegmentId,
                        1, 10, RouteSplitPlan.SplitMode.SPLIT));
        assertEquals("Property 'replacedSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_lower_segment_id() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new RouteSplitPlan<>(replacedSegmentId, null,
                        upperSegmentId, 1, 10, RouteSplitPlan.SplitMode.SPLIT));
        assertEquals("Property 'lowerSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_min_key() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new RouteSplitPlan<>(replacedSegmentId, lowerSegmentId,
                        upperSegmentId, null, 10,
                        RouteSplitPlan.SplitMode.SPLIT));
        assertEquals("Property 'lowerMinKey' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_max_key() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new RouteSplitPlan<>(replacedSegmentId, lowerSegmentId,
                        upperSegmentId, 1, null,
                        RouteSplitPlan.SplitMode.SPLIT));
        assertEquals("Property 'lowerMaxKey' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_split_mode() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new RouteSplitPlan<>(replacedSegmentId, lowerSegmentId,
                        upperSegmentId, 1, 10, null));
        assertEquals("Property 'splitMode' must not be null.",
                err.getMessage());
    }
}
