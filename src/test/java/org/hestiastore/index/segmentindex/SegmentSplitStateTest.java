package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentSplitStateTest {

    @Test
    void storesMutableSplitState() {
        final SegmentSplitState<String, String> state = new SegmentSplitState<>();

        assertNull(state.getLowerSegmentId());
        assertNull(state.getIterator());
        assertNull(state.getResult());

        final SegmentId lower = SegmentId.of(1);
        final EntryIterator<String, String> iterator = mock(EntryIterator.class);
        final SegmentSplitterResult<String, String> result = new SegmentSplitterResult<>(
                SegmentId.of(2), "a", "z",
                SegmentSplitterResult.SegmentSplittingStatus.COMPACTED);

        state.setLowerSegmentId(lower);
        state.setIterator(iterator);
        state.setResult(result);

        assertSame(lower, state.getLowerSegmentId());
        assertSame(iterator, state.getIterator());
        assertSame(result, state.getResult());
    }
}
