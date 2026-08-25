package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class SenkuWritingTest {

    @Test
    void contract_exposesOnlyPutAndFinishWriting() {
        final Set<String> methodNames = Arrays
                .stream(SenkuWriting.class.getDeclaredMethods())
                .map(Method::getName).collect(Collectors.toSet());

        assertEquals(Set.of("put", "finishWriting"), methodNames);
        assertFalse(AutoCloseable.class.isAssignableFrom(SenkuWriting.class));
    }
}
