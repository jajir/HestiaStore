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

class PartitionSplitApplyPlanTest {

    private SegmentId oldSegmentId;
    private SegmentId lowerSegmentId;
    private SegmentId upperSegmentId;
    private PartitionSplitApplyPlan<Integer> splitPlan;

    @BeforeEach
    void setUp() {
        oldSegmentId = SegmentId.of(1);
        lowerSegmentId = SegmentId.of(2);
        upperSegmentId = SegmentId.of(3);
        splitPlan = new PartitionSplitApplyPlan<>(oldSegmentId, lowerSegmentId,
                upperSegmentId, 1, 10,
                PartitionSplitResult.PartitionSplitStatus.SPLIT);
    }

    @AfterEach
    void tearDown() {
        splitPlan = null;
        oldSegmentId = null;
        lowerSegmentId = null;
        upperSegmentId = null;
    }

    @Test
    void getters_return_configured_values() {
        assertSame(oldSegmentId, splitPlan.getOldSegmentId());
        assertSame(lowerSegmentId, splitPlan.getLowerSegmentId());
        assertTrue(splitPlan.getUpperSegmentId().isPresent());
        assertSame(upperSegmentId, splitPlan.getUpperSegmentId().get());
        assertEquals(1, splitPlan.getMinKey());
        assertEquals(10, splitPlan.getMaxKey());
        assertEquals(PartitionSplitResult.PartitionSplitStatus.SPLIT,
                splitPlan.getStatus());
    }

    @Test
    void compacted_allows_missing_upper_segment() {
        final PartitionSplitApplyPlan<Integer> compacted = new PartitionSplitApplyPlan<>(
                oldSegmentId, lowerSegmentId, null, 1, 10,
                PartitionSplitResult.PartitionSplitStatus.COMPACTED);
        assertFalse(compacted.getUpperSegmentId().isPresent());
        assertEquals(PartitionSplitResult.PartitionSplitStatus.COMPACTED,
                compacted.getStatus());
    }

    @Test
    void split_requires_upper_segment() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new PartitionSplitApplyPlan<>(oldSegmentId, lowerSegmentId,
                        null, 1, 10,
                        PartitionSplitResult.PartitionSplitStatus.SPLIT));
        assertEquals("Property 'upperSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_old_segment_id() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new PartitionSplitApplyPlan<>(null, lowerSegmentId,
                        upperSegmentId, 1, 10,
                        PartitionSplitResult.PartitionSplitStatus.SPLIT));
        assertEquals("Property 'oldSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_lower_segment_id() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new PartitionSplitApplyPlan<>(oldSegmentId, null,
                        upperSegmentId, 1, 10,
                        PartitionSplitResult.PartitionSplitStatus.SPLIT));
        assertEquals("Property 'lowerSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void rejects_missing_min_key() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new PartitionSplitApplyPlan<>(oldSegmentId, lowerSegmentId,
                        upperSegmentId, null, 10,
                        PartitionSplitResult.PartitionSplitStatus.SPLIT));
        assertEquals("Property 'minKey' must not be null.", err.getMessage());
    }

    @Test
    void rejects_missing_max_key() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new PartitionSplitApplyPlan<>(oldSegmentId, lowerSegmentId,
                        upperSegmentId, 1, null,
                        PartitionSplitResult.PartitionSplitStatus.SPLIT));
        assertEquals("Property 'maxKey' must not be null.", err.getMessage());
    }

    @Test
    void rejects_missing_status() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new PartitionSplitApplyPlan<>(oldSegmentId, lowerSegmentId,
                        upperSegmentId, 1, 10, null));
        assertEquals("Property 'status' must not be null.", err.getMessage());
    }
}
