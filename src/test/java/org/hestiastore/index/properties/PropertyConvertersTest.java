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

    @Test
    void primitive_conversions_parse_values_with_underscores() {
        assertEquals(1_234_567, converters.toInt("1_234_567"));
        assertEquals(9_876_543_210L, converters.toLong("9_876_543_210"));
        assertEquals(12_345.678D, converters.toDouble("12_345.678"));
    }

    @Test
    void formatting_uses_underscore_grouping_and_dot_decimal_separator() {
        assertEquals("1_234_567", converters.formatInt(1_234_567));
        assertEquals("-9_876_543_210", converters.formatLong(-9_876_543_210L));
        assertEquals("12_345.678", converters.formatDouble(12_345.678D));
    }
}
