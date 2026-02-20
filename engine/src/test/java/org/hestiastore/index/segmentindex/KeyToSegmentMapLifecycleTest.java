package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KeyToSegmentMapLifecycleTest {

    private KeyToSegmentMap<Integer> keyToSegmentMap;

    @BeforeEach
    void setUp() {
        final MemDirectory directory = new MemDirectory();
        keyToSegmentMap = new KeyToSegmentMap<>(
                directory,
                new TypeDescriptorInteger());
    }

    @AfterEach
    void tearDown() {
        if (keyToSegmentMap != null && !keyToSegmentMap.wasClosed()) {
            keyToSegmentMap.close();
        }
    }

    @Test
    void findSegmentIdThrowsAfterClose() {
        keyToSegmentMap.close();
        final Exception e = assertThrows(IllegalStateException.class,
                () -> keyToSegmentMap.findSegmentId(1));
        assertEquals("KeyToSegmentMap already closed", e.getMessage());
    }

    @Test
    void insertSegmentThrowsAfterClose() {
        keyToSegmentMap.close();
        assertThrows(IllegalStateException.class,
                () -> keyToSegmentMap.insertSegment(1, SegmentId.of(0)));
    }

    @Test
    void optionalyFlushThrowsAfterClose() {
        keyToSegmentMap.close();
        assertThrows(IllegalStateException.class,
                () -> keyToSegmentMap.optionalyFlush());
    }
}
