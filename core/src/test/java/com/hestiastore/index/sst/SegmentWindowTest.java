package com.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class SegmentWindowTest {

    @Test
    void test_make_unbounded() {
        assertSame(SegmentWindow.UNBOUNDED, SegmentWindow.unbounded());

        assertEquals(0, SegmentWindow.UNBOUNDED.getIntOffset());
        assertEquals(Integer.MAX_VALUE, SegmentWindow.UNBOUNDED.getIntLimit());
    }

    @Test
    void test_make_43_17() {
        final SegmentWindow segmentWindow = SegmentWindow.of(43, 17);
        assertSame(SegmentWindow.UNBOUNDED, SegmentWindow.unbounded());

        assertEquals(43, segmentWindow.getIntOffset());
        assertEquals(17, segmentWindow.getIntLimit());
    }

    @Test
    void test_make_offset_negative() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> SegmentWindow.of(-43, 17));

        assertEquals("offset must be ≥ 0 (was -43)", e.getMessage());
    }

    @Test
    void test_make_limit_negative() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> SegmentWindow.of(43, -17));

        assertEquals("limit must be ≥ 0 (was -17)", e.getMessage());
    }

}
