package org.hestiastore.index.segmentindex.core.session.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.Test;

class IndexStateClosedTest {

    @Test
    void rejectsAllOperations() {
        final IndexStateClosed<Integer, String> state = new IndexStateClosed<>();

        assertEquals(SegmentIndexState.CLOSED, state.state());
        assertThrows(IllegalStateException.class,
                state::onReady);
        assertThrows(IllegalStateException.class, state::onClose);
        assertThrows(IllegalStateException.class, state::finishClose);
        assertThrows(IllegalStateException.class, state::tryPerformOperation);
    }
}
