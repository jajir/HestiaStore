package org.hestiastore.index.segmentindex.configuration.tuning;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RuntimeTuningValueTest {

    @Test
    void exposesIntegerValues() {
        assertEquals(5, RuntimeTuningValue.ofInt(5).asInt());
        assertEquals("5", RuntimeTuningValue.ofInt(5).toString());
    }
}
