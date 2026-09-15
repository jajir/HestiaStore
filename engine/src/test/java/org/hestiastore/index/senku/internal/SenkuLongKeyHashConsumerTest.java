package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class SenkuLongKeyHashConsumerTest {
    @Test
    void conveysPrimitiveExtremesAndDoesNotReplaceConsumerFailure() {
        final long[] observed = new long[2];
        final SenkuLongKeyHashConsumer consumer = (key, hash) -> {
            observed[0] = key;
            observed[1] = hash;
        };
        consumer.accept(Long.MIN_VALUE, Integer.MIN_VALUE);
        assertEquals(Long.MIN_VALUE, observed[0]);
        assertEquals(Integer.MIN_VALUE, observed[1]);
        final IndexException expected = new IndexException("consumer");
        final SenkuLongKeyHashConsumer failing = (key, hash) -> {
            throw expected;
        };
        assertSame(expected,
                assertThrows(IndexException.class, () -> failing.accept(0, 0)));
    }
}
