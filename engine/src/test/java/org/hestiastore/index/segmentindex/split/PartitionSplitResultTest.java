package org.hestiastore.index.segmentindex.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class PartitionSplitResultTest {

    @Test
    void exposesSplitValues() {
        final SegmentId segmentId = SegmentId.of(1);
        final PartitionSplitResult<String> result = new PartitionSplitResult<>(
                segmentId, "min", "max",
                PartitionSplitResult.PartitionSplitStatus.SPLIT);

        assertTrue(result.isSplit());
        assertEquals(segmentId, result.getSegmentId());
        assertEquals("min", result.getMinKey());
        assertEquals("max", result.getMaxKey());
        assertEquals(PartitionSplitResult.PartitionSplitStatus.SPLIT,
                result.getStatus());
    }

    @Test
    void compactionIsNotSplit() {
        final PartitionSplitResult<String> result = new PartitionSplitResult<>(
                SegmentId.of(1), "min", "max",
                PartitionSplitResult.PartitionSplitStatus.COMPACTED);

        assertFalse(result.isSplit());
    }
}
