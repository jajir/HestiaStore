package org.hestiastore.index.segmentindex.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KeyToSegmentMapLifecycleTest {

    private KeyToSegmentMapImpl<Integer> keyToSegmentMap;

    @BeforeEach
    void setUp() {
        final MemDirectory directory = new MemDirectory();
        keyToSegmentMap = new KeyToSegmentMapImpl<>(directory,
                new TypeDescriptorInteger());
    }

    @AfterEach
    void tearDown() {
        if (keyToSegmentMap != null && !keyToSegmentMap.wasClosed()) {
            keyToSegmentMap.close();
        }
    }

    @Test
    void findSegmentIdForKeyThrowsAfterClose() {
        keyToSegmentMap.close();
        final Exception e = assertThrows(IllegalStateException.class,
                () -> keyToSegmentMap.findSegmentIdForKey(1));
        assertEquals("KeyToSegmentMapImpl already closed", e.getMessage());
    }

    @Test
    void insertSegmentThrowsAfterClose() {
        final SegmentId segmentId = SegmentId.of(0);
        keyToSegmentMap.close();
        assertThrows(IllegalStateException.class,
                () -> keyToSegmentMap.insertSegment(1, segmentId));
    }

    @Test
    void flushIfDirtyThrowsAfterClose() {
        keyToSegmentMap.close();
        assertThrows(IllegalStateException.class,
                () -> keyToSegmentMap.flushIfDirty());
    }
}
