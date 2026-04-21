package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SegmentIndexSplitPolicyThresholdTest {

    @Test
    void shouldSplitWhenCacheMeetsThreshold() {
        @SuppressWarnings("unchecked")
        final SegmentHandle<Integer, String> segmentHandle = Mockito
                .mock(SegmentHandle.class);
        final SegmentHandle.Runtime runtime = Mockito
                .mock(SegmentHandle.Runtime.class);
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(runtime.getNumberOfKeysInCache()).thenReturn(3L, 2L);

        final SegmentIndexSplitPolicyThreshold<Integer, String> policy = new SegmentIndexSplitPolicyThreshold<>();

        assertTrue(policy.shouldSplit(segmentHandle, 3));
        assertFalse(policy.shouldSplit(segmentHandle, 3));
    }

    @Test
    void shouldNotSplitWhenThresholdIsNonPositive() {
        @SuppressWarnings("unchecked")
        final SegmentHandle<Integer, String> segmentHandle = Mockito
                .mock(SegmentHandle.class);
        final SegmentIndexSplitPolicyThreshold<Integer, String> policy = new SegmentIndexSplitPolicyThreshold<>();

        assertFalse(policy.shouldSplit(segmentHandle, 0));
        assertFalse(policy.shouldSplit(segmentHandle, -1));

        verifyNoInteractions(segmentHandle);
    }
}
