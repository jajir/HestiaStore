package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class SegmentIdTest {

    @Test
    void test_getName_379() {
        final SegmentId segmentId = SegmentId.of(379);

        assertEquals("segment-00379", segmentId.getName());
    }

    @Test
    void test_getName_0() {
        final SegmentId segmentId = SegmentId.of(0);

        assertEquals("segment-00000", segmentId.getName());
    }

    @Test
    void test_getName_99999() {
        final SegmentId segmentId = SegmentId.of(99999);

        assertEquals("segment-99999", segmentId.getName());
    }

    @Test
    void test_constrict_with_id_smaller_than_zero() {
        final Exception e = assertThrows(IllegalArgumentException.class, () -> SegmentId.of(-2));

        assertEquals("Segment id must be greater than or equal to 0", e.getMessage());
    }
}
