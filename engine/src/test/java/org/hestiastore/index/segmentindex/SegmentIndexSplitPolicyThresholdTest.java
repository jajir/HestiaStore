package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.Segment;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SegmentIndexSplitPolicyThresholdTest {

    @Test
    void shouldSplitWhenCacheMeetsThreshold() {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        when(segment.getNumberOfKeysInCache()).thenReturn(3L, 2L);

        final SegmentIndexSplitPolicyThreshold<Integer, String> policy = new SegmentIndexSplitPolicyThreshold<>();

        assertTrue(policy.shouldSplit(segment, 3));
        assertFalse(policy.shouldSplit(segment, 3));
    }

    @Test
    void shouldNotSplitWhenThresholdIsNonPositive() {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        final SegmentIndexSplitPolicyThreshold<Integer, String> policy = new SegmentIndexSplitPolicyThreshold<>();

        assertFalse(policy.shouldSplit(segment, 0));
        assertFalse(policy.shouldSplit(segment, -1));

        verifyNoInteractions(segment);
    }
}
