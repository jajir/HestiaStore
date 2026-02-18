package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class IndexStateClosedTest {

    @Test
    void rejectsAllOperations() {
        final IndexStateClosed<Integer, String> state = new IndexStateClosed<>();

        assertThrows(IllegalStateException.class,
                () -> state.onReady(null));
        assertThrows(IllegalStateException.class,
                () -> state.onClose(null));
        assertThrows(IllegalStateException.class, state::tryPerformOperation);
    }
}
