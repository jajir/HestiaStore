package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class SenkuReadyTest {

    @Test
    void contract_isCloseableAndExposesOnlyStreamLifecycle() {
        final Set<String> methodNames = Arrays
                .stream(SenkuReady.class.getDeclaredMethods())
                .map(Method::getName).collect(Collectors.toSet());

        assertEquals(Set.of("openStream", "close"), methodNames);
        assertTrue(AutoCloseable.class.isAssignableFrom(SenkuReady.class));
    }
}
