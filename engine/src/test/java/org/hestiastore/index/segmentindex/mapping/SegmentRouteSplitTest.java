package org.hestiastore.index.segmentindex.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentRouteSplitTest {

    private SegmentId replacedSegmentId;
    private SegmentId lowerSegmentId;
    private SegmentId upperSegmentId;
    private SegmentRouteSplit<Integer> routeSplit;

    @BeforeEach
    void setUp() {
        replacedSegmentId = SegmentId.of(1);
        lowerSegmentId = SegmentId.of(2);
        upperSegmentId = SegmentId.of(3);
        routeSplit = new SegmentRouteSplit<>(replacedSegmentId, lowerSegmentId,
                upperSegmentId, 10);
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
    }

    @Test
    void split_requires_upper_segment() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentRouteSplit<>(replacedSegmentId, lowerSegmentId,
                        null, 10));
        assertEquals("Property 'upperSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_replaced_segment_id() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentRouteSplit<>(null, lowerSegmentId, upperSegmentId,
                        10));
        assertEquals("Property 'replacedSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_lower_segment_id() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentRouteSplit<>(replacedSegmentId, null,
                        upperSegmentId, 10));
        assertEquals("Property 'lowerSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_max_key() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentRouteSplit<>(replacedSegmentId, lowerSegmentId,
                        upperSegmentId, null));
        assertEquals("Property 'lowerMaxKey' must not be null.",
                err.getMessage());
    }
}
