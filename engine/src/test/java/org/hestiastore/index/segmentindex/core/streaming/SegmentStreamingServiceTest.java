package org.hestiastore.index.segmentindex.core.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class SegmentStreamingServiceTest {

    @Test
    void declaresStableSegmentStreamingOperations() {
        final List<String> declaredMethodNames = Arrays
                .stream(SegmentStreamingService.class.getDeclaredMethods())
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .map(Method::getName)
                .sorted()
                .toList();

        assertEquals(List.of("invalidateIterators", "openIterator"),
                declaredMethodNames);
    }
}
