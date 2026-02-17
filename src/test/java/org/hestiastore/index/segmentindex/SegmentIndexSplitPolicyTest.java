package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.verifyNoInteractions;

import org.hestiastore.index.segment.Segment;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SegmentIndexSplitPolicyTest {

    @Test
    void nonePolicyNeverSplits() {
        final SegmentIndexSplitPolicy<Integer, String> policy = SegmentIndexSplitPolicy
                .none();
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);

        assertFalse(policy.shouldSplit(segment, 1));
        assertFalse(policy.shouldSplit(segment, 0));
        assertFalse(policy.shouldSplit(segment, -5));

        verifyNoInteractions(segment);
    }
}
