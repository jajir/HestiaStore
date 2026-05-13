package org.hestiastore.index.segmentindex.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentRouteSplitPlanTest {

    private SegmentId replacedSegmentId;
    private SegmentId lowerSegmentId;
    private SegmentId upperSegmentId;
    private SegmentRouteSplitPlan<Integer> splitPlan;

    @BeforeEach
    void setUp() {
        replacedSegmentId = SegmentId.of(1);
        lowerSegmentId = SegmentId.of(2);
        upperSegmentId = SegmentId.of(3);
        splitPlan = new SegmentRouteSplitPlan<>(replacedSegmentId, lowerSegmentId,
                upperSegmentId, 10, SegmentRouteSplitPlan.SplitMode.SPLIT);
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
        assertEquals(10, splitPlan.getLowerMaxKey());
        assertEquals(SegmentRouteSplitPlan.SplitMode.SPLIT, splitPlan.getSplitMode());
        assertTrue(splitPlan.isSplit());
    }

    @Test
    void compacted_allows_missing_upper_segment() {
        final SegmentRouteSplitPlan<Integer> compacted = new SegmentRouteSplitPlan<>(
                replacedSegmentId, lowerSegmentId, null, 10,
                SegmentRouteSplitPlan.SplitMode.COMPACTED);
        assertFalse(compacted.getUpperSegmentId().isPresent());
        assertEquals(SegmentRouteSplitPlan.SplitMode.COMPACTED,
                compacted.getSplitMode());
        assertFalse(compacted.isSplit());
    }

    @Test
    void split_requires_upper_segment() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentRouteSplitPlan<>(replacedSegmentId, lowerSegmentId,
                        null, 10, SegmentRouteSplitPlan.SplitMode.SPLIT));
        assertEquals("Property 'upperSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_replaced_segment_id() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentRouteSplitPlan<>(null, lowerSegmentId, upperSegmentId,
                        10, SegmentRouteSplitPlan.SplitMode.SPLIT));
        assertEquals("Property 'replacedSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_lower_segment_id() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentRouteSplitPlan<>(replacedSegmentId, null,
                        upperSegmentId, 10, SegmentRouteSplitPlan.SplitMode.SPLIT));
        assertEquals("Property 'lowerSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_max_key() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentRouteSplitPlan<>(replacedSegmentId, lowerSegmentId,
                        upperSegmentId, null, SegmentRouteSplitPlan.SplitMode.SPLIT));
        assertEquals("Property 'lowerMaxKey' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_split_mode() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentRouteSplitPlan<>(replacedSegmentId, lowerSegmentId,
                        upperSegmentId, 10, null));
        assertEquals("Property 'splitMode' must not be null.",
                err.getMessage());
    }
}
