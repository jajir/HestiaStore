package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class IndexStateCloseTest {

    @Test
    void rejectsAllOperations() {
        final IndexStateClose<Integer, String> state = new IndexStateClose<>();

        assertThrows(IllegalStateException.class,
                () -> state.onReady(null));
        assertThrows(IllegalStateException.class,
                () -> state.onClose(null));
        assertThrows(IllegalStateException.class, state::tryPerformOperation);
    }
}
