package org.hestiastore.index.segmentindex.configuration.tuning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RuntimeTuningValueTest {

    @Test
    void exposesTypedValues() {
        assertEquals(5, RuntimeTuningValue.ofInt(5).asInt());
        assertEquals(7L, RuntimeTuningValue.ofLong(7L).asLong());
        assertTrue(RuntimeTuningValue.ofBoolean(true).asBoolean());
        assertEquals(0.5D, RuntimeTuningValue.ofDouble(0.5D).asDouble(),
                0.0D);
        assertEquals("text", RuntimeTuningValue.ofString("text").asString());
        assertEquals("INT", RuntimeTuningValue
                .ofEnum(RuntimeTuningValueType.INT).asString());
    }

    @Test
    void rejectsWrongTypedAccessor() {
        final RuntimeTuningValue value = RuntimeTuningValue.ofInt(5);

        assertThrows(IllegalStateException.class, value::asBoolean);
    }
}
