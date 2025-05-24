package com.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class VldtnTest {

    @Test
    void test_requireNonNull_misingPropertyName() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireNonNull(null, null));
        assertEquals("Property 'propertyName' must not be null.",
                e.getMessage());
    }

    @Test
    void test_requireNonNull_returnValue() {
        final String value = "duck";
        assertSame(value, Vldtn.requireNonNull(value, "myProperty"));
    }

    @Test
    void test_requireNonNull() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireNonNull(null, "maxNumberOfKeysInIndex"));
        assertEquals("Property 'maxNumberOfKeysInIndex' must not be null.",
                e.getMessage());
    }

}
