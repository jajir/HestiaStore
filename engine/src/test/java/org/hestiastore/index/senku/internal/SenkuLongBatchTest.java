package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class SenkuLongBatchTest {
    @Test
    void boundedWindowsKeepExactKeysAndOriginalHashesGroupedByStripe() {
        final long[] input = { 99, Long.MIN_VALUE, 0, Long.MAX_VALUE, -1, 7,
                88 };
        final SenkuLongBatch batch = new SenkuLongBatch(5);
        final AtomicInteger calls = new AtomicInteger();
        batch.load(input, 1, 5, key -> {
            calls.incrementAndGet();
            return Long.hashCode(key);
        });
        final Set<Long> actual = new HashSet<>();
        for (int stripe = 0; stripe < SenkuLongBatch.STRIPE_COUNT; stripe++) {
            for (int index = batch.first(stripe); index >= 0; index = batch
                    .next(index)) {
                assertEquals(stripe,
                        SenkuIngestor.stripeFromHash(batch.hash(index)));
                assertEquals(Long.hashCode(batch.key(index)),
                        batch.hash(index));
                assertTrue(actual.add(batch.key(index)));
            }
        }
        assertEquals(Set.of(Long.MIN_VALUE, 0L, Long.MAX_VALUE, -1L, 7L),
                actual);
        assertEquals(5, calls.get());
        batch.load(input, 5, 1, key -> -9);
        assertEquals(7L,
                batch.key(batch.first(SenkuIngestor.stripeFromHash(-9))));
        batch.load(input, input.length, 0, key -> 0);
        for (int stripe = 0; stripe < SenkuLongBatch.STRIPE_COUNT; stripe++) {
            assertEquals(-1, batch.first(stripe));
        }
    }

    @Test
    void validatesWholeSliceAndBoundsScratchAndPreservesHashFailure() {
        final long[] keys = { 1 };
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuLongBatch(0));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuLongBatch(SenkuLongBatch.WINDOW_KEYS + 1));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuLongBatch.validateSlice(null, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuLongBatch.validateSlice(keys, -1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuLongBatch.validateSlice(keys, 0, -1));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuLongBatch.validateSlice(keys, 2, 0));
        assertThrows(IllegalArgumentException.class, () -> SenkuLongBatch
                .validateSlice(keys, Integer.MAX_VALUE, Integer.MAX_VALUE));
        final SenkuLongBatch batch = new SenkuLongBatch(1);
        final long[] tooLarge = { 1, 2 };
        assertThrows(IllegalArgumentException.class,
                () -> batch.load(tooLarge, 0, 2, key -> 0));
        assertThrows(IllegalArgumentException.class,
                () -> batch.load(keys, 0, 1, null));
        final IndexException failure = new IndexException("routing");
        assertSame(failure, assertThrows(IndexException.class,
                () -> batch.load(keys, 0, 1, key -> {
                    throw failure;
                })));
        final IllegalStateException cause = new IllegalStateException(
                "routing cause");
        assertSame(cause, assertThrows(IndexException.class,
                () -> batch.load(keys, 0, 1, key -> {
                    throw cause;
                })).getCause());
    }
}
