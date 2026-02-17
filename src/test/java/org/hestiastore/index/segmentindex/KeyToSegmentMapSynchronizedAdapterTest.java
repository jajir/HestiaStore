package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class KeyToSegmentMapSynchronizedAdapterTest {

    @Test
    void constructorRejectsNullDelegate() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new KeyToSegmentMapSynchronizedAdapter<Integer>(null));
        assertEquals("Property 'delegate' must not be null.", e.getMessage());
    }
}
