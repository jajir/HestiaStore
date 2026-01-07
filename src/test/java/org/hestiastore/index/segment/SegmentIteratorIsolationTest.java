package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class SegmentIteratorIsolationTest {

    @Test
    void valuesContainExpectedModes() {
        final List<SegmentIteratorIsolation> values = List
                .of(SegmentIteratorIsolation.values());

        assertEquals(2, values.size());
        assertTrue(values.contains(SegmentIteratorIsolation.FAIL_FAST));
        assertTrue(values.contains(SegmentIteratorIsolation.FULL_ISOLATION));
    }
}
