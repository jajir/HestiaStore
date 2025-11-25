package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import java.util.List;

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
    void test_requireIoBufferSize() {
        assertEquals(1024, Vldtn.requireIoBufferSize(1024));
    }

    @Test
    void test_requireIoBufferSize_less_then_zero() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireIoBufferSize(-1));

        assertEquals("Property 'ioBufferSize' must be greater than 0",
                e.getMessage());
    }

    @Test
    void test_requireIoBufferSize_is_zero() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireIoBufferSize(0));

        assertEquals("Property 'ioBufferSize' must be greater than 0",
                e.getMessage());
    }

    @Test
    void test_requireIoBufferSize_dividion_by_1024() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireIoBufferSize(100000));

        assertEquals(
                "Propety 'ioBufferSize' must be divisible "
                        + "by 1024 (e.g., 1024, 2048, 4096). Got: '100000'",
                e.getMessage());
    }

    @Test
    void test_requireBetween() {
        assertEquals(5, Vldtn.requireBetween(5, 1, 10, "testProperty"));
    }

    @Test
    void test_requireBetween_high() {
        assertEquals(10, Vldtn.requireBetween(10, 1, 10, "testProperty"));
    }

    @Test
    void test_requireBetween_higher() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireBetween(11, 1, 10, "testProperty"));
        assertEquals("Property 'testProperty' must be between 1 and 10 "
                + "(inclusive). Got: 11", e.getMessage());
    }

    @Test
    void test_requireBetween_low() {
        assertEquals(1, Vldtn.requireBetween(1, 1, 10, "testProperty"));
    }

    @Test
    void test_requireBetween_lower() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireBetween(0, 1, 10, "testProperty"));
        assertEquals("Property 'testProperty' must be between 1 and 10 "
                + "(inclusive). Got: 0", e.getMessage());
    }

    @Test
    void test_requireBetween_nullPropertyName() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireBetween(5, 1, 10, null));
        assertEquals("Property 'propertyName' must not be null.",
                e.getMessage());
    }

    @Test
    void test_requireCellSize_invalidCellSize() {
        assertEquals(16, Vldtn.requireCellSize(16, "blockPayloadSize"));
        assertEquals(1024, Vldtn.requireCellSize(1024, "blockPayloadSize"));
    }

    @Test
    void test_requireCellSize_invalidCellSize_0() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireCellSize(0, "blockPayloadSize"));
        assertEquals("Property 'blockPayloadSize' must be greater than 0",
                e.getMessage());
    }

    @Test
    void test_requireCellSize_invalidCellSize_minus100() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireCellSize(-100, "blockPayloadSize"));
        assertEquals("Property 'blockPayloadSize' must be greater than 0",
                e.getMessage());
    }

    @Test
    void test_requireCellSize_invalidCellSize_20() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireCellSize(20, "blockPayloadSize"));
        assertEquals(
                "Property 'blockPayloadSize' must be divisible by 16 (e.g., 16, 32, 64). Got: '20'",
                e.getMessage());
    }

    @Test
    void test_requireCellSize_null_propertyName() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireCellSize(20, null));
        assertEquals("Property 'propertyName' must not be null.",
                e.getMessage());
    }

    @Test
    void test_requireNotEmpty_returnsCollection() {
        final List<String> data = List.of("a", "b");
        assertSame(data, Vldtn.requireNotEmpty(data, "items"));
    }

    @Test
    void test_requireNotEmpty_nullCollection() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireNotEmpty(null, "items"));
        assertEquals("Property 'items' must not be null.", e.getMessage());
    }

    @Test
    void test_requireNotEmpty_nullPropertyName() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireNotEmpty(List.of("a"), null));
        assertEquals("Property 'propertyName' must not be null.",
                e.getMessage());
    }

    @Test
    void test_requireNotEmpty_emptyCollection() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireNotEmpty(List.of(), "items"));
        assertEquals("Property 'items' must not be empty.", e.getMessage());
    }

    @Test
    void test_requireGreaterThanZero_long_value() {
        assertEquals(42L, Vldtn.requireGreaterThanZero(42L, "limit"));
    }

    @Test
    void test_requireGreaterThanZero_long_invalid() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Vldtn.requireGreaterThanZero(0L, "limit"));
        assertEquals("Property 'limit' must be greater than 0",
                e.getMessage());
    }

}
