package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class VldtnTest {

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

    @Test
    void test_ioBufferSize() {
        assertEquals(1024, Vldtn.ioBufferSize(1024));
    }

    @Test
    void test_ioBufferSize_less_then_zero() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.ioBufferSize(-1));

        assertEquals("Property 'ioBufferSize' must be greater than 0",
                e.getMessage());
    }

    @Test
    void test_ioBufferSize_is_zero() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.ioBufferSize(0));

        assertEquals("Property 'ioBufferSize' must be greater than 0",
                e.getMessage());
    }

    @Test
    void test_ioBufferSize_dividion_by_1024() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.ioBufferSize(100000));

        assertEquals(
                "Propety 'ioBufferSize' must be divisible "
                        + "by 1024 (e.g., 1024, 2048, 4096). Got: '100000'",
                e.getMessage());
    }

}
