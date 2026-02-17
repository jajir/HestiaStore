package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitStateTest {

    @Mock
    private EntryIterator<String, String> iterator;

    private SegmentSplitState<String, String> state;

    @BeforeEach
    void setUp() {
        state = new SegmentSplitState<>();
    }

    @AfterEach
    void tearDown() {
        state = null;
    }

    @Test
    void storesMutableSplitState() {
        assertNull(state.getLowerSegmentId());
        assertNull(state.getIterator());
        assertNull(state.getResult());

        final SegmentId lower = SegmentId.of(1);
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
