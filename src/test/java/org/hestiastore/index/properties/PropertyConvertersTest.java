package org.hestiastore.index.properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class PropertyConvertersTest {

    private final PropertyConverters converters = new PropertyConverters();

    @Test
    void toString_throws_on_null() {
        assertThrows(IndexException.class, () -> converters.toString(null));
    }

    @Test
    void primitive_conversions_handle_null_defaults() {
        assertEquals(0, converters.toInt(null));
        assertEquals(0L, converters.toLong(null));
        assertEquals(0D, converters.toDouble(null));
        assertFalse(converters.toBoolean(null));
    }

    @Test
    void primitive_conversions_parse_values() {
        assertEquals("value", converters.toString("value"));
        assertEquals(123, converters.toInt("123"));
        assertEquals(123L, converters.toLong("123"));
        assertEquals(1.5D, converters.toDouble("1.5"));
        assertTrue(converters.toBoolean("true"));
        assertFalse(converters.toBoolean("false"));
    }
}
