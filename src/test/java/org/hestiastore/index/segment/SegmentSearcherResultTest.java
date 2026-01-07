package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class SegmentSearcherResultTest {

    @Test
    void setAndGetValue() {
        final SegmentSearcherResult<String> result = new SegmentSearcherResult<>();

        assertNull(result.getValue());

        result.setValue("value");
        assertEquals("value", result.getValue());
    }
}
