package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuMergeFunctionRegistryTest {

    private SenkuMergeFunctionRegistry<Integer, Integer> registry;

    @BeforeEach
    void setUp() {
        registry = new SenkuMergeFunctionRegistry<>();
    }

    @Test
    void register_returnsRegistryAndStoresFunction() {
        final SenkuMergeFunction<Integer, Integer> function =
                (key, first, second) -> first + second;

        assertSame(registry, registry.register(function));
        assertSame(function, registry.requireFunction());
    }

    @Test
    void register_rejectsNullFunction() {
        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, () -> registry.register(null));

        assertEquals("Property 'mergeFunction' must not be null.",
                error.getMessage());
    }

    @Test
    void register_rejectsSecondFunction() {
        registry.register((key, first, second) -> first);

        final IndexException error = assertThrows(IndexException.class,
                () -> registry.register((key, first, second) -> second));

        assertEquals(
                "Senku merge function registry accepts exactly one function.",
                error.getMessage());
    }

    @Test
    void requireFunction_rejectsEmptyRegistry() {
        final IndexException error = assertThrows(IndexException.class,
                registry::requireFunction);

        assertEquals(
                "Senku merge function registry requires exactly one function.",
                error.getMessage());
    }

    @Test
    void freezeRejectsLaterRegistration() {
        registry.register((key, first, second) -> first);
        registry.freeze();

        final IndexException error = assertThrows(IndexException.class,
                () -> registry.register((key, first, second) -> second));

        assertEquals("Senku merge function registry is frozen.",
                error.getMessage());
    }
}
