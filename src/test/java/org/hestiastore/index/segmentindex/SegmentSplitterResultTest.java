package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentSplitterResultTest {

    @Test
    void exposesSplitValues() {
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentSplitterResult<String, String> result = new SegmentSplitterResult<>(
                segmentId, "min", "max",
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);

        assertTrue(result.isSplit());
        assertEquals(segmentId, result.getSegmentId());
        assertEquals("min", result.getMinKey());
        assertEquals("max", result.getMaxKey());
        assertEquals(SegmentSplitterResult.SegmentSplittingStatus.SPLIT,
                result.getStatus());
    }

    @Test
    void compactionIsNotSplit() {
        final SegmentSplitterResult<String, String> result = new SegmentSplitterResult<>(
                SegmentId.of(1), "min", "max",
                SegmentSplitterResult.SegmentSplittingStatus.COMPACTED);

        assertFalse(result.isSplit());
    }
}
