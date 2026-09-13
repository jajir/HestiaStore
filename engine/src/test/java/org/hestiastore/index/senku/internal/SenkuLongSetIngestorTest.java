package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongToIntFunction;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.senku.SenkuMergeFunctions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SenkuLongSetIngestorTest {

    @Mock
    private SenkuFlushWriter<Long, NullValue> flushWriter;

    @Test
    void multiWindowBatchRotatesAtBoundedThresholdAndHashesEachKeyOnce() {
        final TreeSet<Long> flushedKeys = new TreeSet<>();
        final List<Integer> sizes = new ArrayList<>();
        doAnswer(invocation -> {
            final List<SenkuIngestionMap<Long, NullValue>> maps = invocation
                    .getArgument(1);
            int size = 0;
            for (final SenkuIngestionMap<Long, NullValue> map : maps) {
                size += map.size();
                map.forEachLong(flushedKeys::add);
            }
            sizes.add(size);
            return null;
        }).when(flushWriter).write(anyLong(), anyList());
        final AtomicInteger hashes = new AtomicInteger();
        final SenkuIngestor<Long, NullValue> ingestor = new SenkuIngestor<>(
                new ReentrantLock(), SenkuMergeFunctions.longSet(),
                key -> key.hashCode(), flushWriter, 257, 4, key -> {
                    hashes.incrementAndGet();
                    return 0; // One stripe also exercises the 64-key hold cap.
                });
        final long[] input = LongStream.range(-3, 5002).toArray();
        ingestor.putLongs(input, 3, 5000);
        assertEquals(5000, hashes.get());
        assertTrue(ingestor.stopAcceptingAndFlush());
        assertEquals(LongStream.range(0, 5000).boxed().toList(),
                List.copyOf(flushedKeys));
        assertTrue(sizes.size() > 1);
        assertTrue(sizes.stream().allMatch(size -> size <= 257));
        assertThrows(IndexException.class,
                () -> ingestor.putLongs(input, 0, 0));
    }

    @Test
    void batchMayAcceptEarlierWindowsBeforeRoutingFailureButInvalidSliceAcceptsNothing() {
        final long[] input = LongStream.range(0, 4096).toArray();
        final IndexException failure = new IndexException("second window");
        final SenkuIngestor<Long, NullValue> ingestor = new SenkuIngestor<>(
                new ReentrantLock(), SenkuMergeFunctions.longSet(),
                key -> key.hashCode(), flushWriter, 10_000, 4, key -> {
                    if (key == SenkuLongBatch.WINDOW_KEYS)
                        throw failure;
                    return Long.hashCode(key);
                });
        assertThrows(IllegalArgumentException.class,
                () -> ingestor.putLongs(input, 1, input.length));
        assertEquals(0, ingestor.size());
        ingestor.putLongs(input, input.length, 0);
        assertSame(failure, assertThrows(IndexException.class,
                () -> ingestor.putLongs(input, 0, input.length)));
        assertEquals(SenkuLongBatch.WINDOW_KEYS, ingestor.size());
        ingestor.fail();
    }

    @Test
    void batchFlushFailureAndPreexistingInterruptionPreserveFailureAndFlag() {
        final SenkuIngestor<Long, NullValue> ingestor = newIngestor(
                Long::hashCode);
        final long[] input = { Long.MIN_VALUE, 0, Long.MAX_VALUE, 5 };
        Thread.currentThread().interrupt();
        try {
            assertThrows(IndexException.class,
                    () -> ingestor.putLongs(input, 0, input.length));
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(0, ingestor.size());
        } finally {
            Thread.interrupted();
        }
        final IndexException failure = new IndexException("batch flush");
        doThrow(failure).when(flushWriter).write(anyLong(), anyList());
        assertSame(failure, assertThrows(IndexException.class,
                () -> ingestor.putLongs(input, 0, input.length)));
        assertSame(failure, assertThrows(IndexException.class,
                () -> ingestor.putLongs(input, 0, 0)));
        assertFalse(ingestor.isPaused());
    }

    @Test
    void rotationDetachesPrimitiveBatchesAndNextBatchForgetsOldKeys() {
        final List<List<Long>> batches = new ArrayList<>();
        doAnswer(invocation -> {
            final List<SenkuIngestionMap<Long, NullValue>> maps = invocation
                    .getArgument(1);
            final TreeSet<Long> keys = new TreeSet<>();
            for (final SenkuIngestionMap<Long, NullValue> map : maps) {
                assertTrue(map.isLongSet());
                map.forEachLong(keys::add);
            }
            batches.add(List.copyOf(keys));
            return null;
        }).when(flushWriter).write(anyLong(), anyList());
        final SenkuIngestor<Long, NullValue> ingestor = newIngestor(
                Long::hashCode);
        ingestor.putLong(0L);
        ingestor.putLong(0L);
        assertEquals(1, ingestor.size());
        ingestor.putLong(Long.MIN_VALUE);
        ingestor.putLong(Long.MAX_VALUE);
        assertEquals(0, ingestor.size());
        ingestor.putLong(0L);
        assertEquals(1, ingestor.size());
        ingestor.stopAcceptingAndFlush();
        assertEquals(List.of(List.of(Long.MIN_VALUE, 0L, Long.MAX_VALUE),
                List.of(0L)), batches);
        assertThrows(IndexException.class, () -> ingestor.putLong(1L));
    }

    @Test
    void primitiveHashAndFlushFailuresRetainIdentity() {
        final IndexException hashFailure = new IndexException("hash");
        final SenkuIngestor<Long, NullValue> badHash = newIngestor(key -> {
            throw hashFailure;
        });
        assertSame(hashFailure,
                assertThrows(IndexException.class, () -> badHash.putLong(0L)));
        assertEquals(0, badHash.size());
        final IndexException flushFailure = new IndexException("flush");
        doThrow(flushFailure).when(flushWriter).write(anyLong(), anyList());
        final SenkuIngestor<Long, NullValue> ingestor = newIngestor(
                Long::hashCode);
        ingestor.putLong(0L);
        assertSame(flushFailure,
                assertThrows(IndexException.class, ingestor::flushRemaining));
        assertSame(flushFailure,
                assertThrows(IndexException.class, () -> ingestor.putLong(1L)));
        assertFalse(ingestor.isPaused());
    }

    @Test
    void genericIngestorRejectsPrimitiveEntryPointInsteadOfInferringNullValues() {
        final SenkuIngestor<Long, NullValue> ingestor = new SenkuIngestor<>(
                new ReentrantLock(), (key, first, second) -> NullValue.NULL,
                key -> key.hashCode(), flushWriter, 3, 4);
        assertThrows(IllegalStateException.class, () -> ingestor.putLong(0L));
        final long[] keys = { 0L };
        assertThrows(IllegalStateException.class,
                () -> ingestor.putLongs(keys, 0, 1));
        assertEquals(0, ingestor.size());
    }

    private SenkuIngestor<Long, NullValue> newIngestor(
            final LongToIntFunction hash) {
        return new SenkuIngestor<>(new ReentrantLock(),
                SenkuMergeFunctions.longSet(), key -> {
                    throw new AssertionError(
                            "Primitive ingestion must not box for routing");
                }, flushWriter, 3, 4, hash);
    }
}
