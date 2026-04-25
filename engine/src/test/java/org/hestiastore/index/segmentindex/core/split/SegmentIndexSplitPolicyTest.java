package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SegmentIndexSplitPolicyTest {

    @Test
    void nonePolicyNeverSplits() {
        final SegmentIndexSplitPolicy<Integer, String> policy = SegmentIndexSplitPolicy
                .none();
        @SuppressWarnings("unchecked")
        final BlockingSegment<Integer, String> segmentHandle = Mockito
                .mock(BlockingSegment.class);
        final BlockingSegment.Runtime runtime = Mockito
                .mock(BlockingSegment.Runtime.class);
        when(segmentHandle.getRuntime()).thenReturn(runtime);

        assertFalse(policy.shouldSplit(segmentHandle, 1));
        assertFalse(policy.shouldSplit(segmentHandle, 0));
        assertFalse(policy.shouldSplit(segmentHandle, -5));

        verifyNoInteractions(segmentHandle, runtime);
    }
}
