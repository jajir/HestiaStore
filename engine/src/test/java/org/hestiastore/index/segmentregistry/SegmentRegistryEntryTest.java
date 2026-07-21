package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.Segment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistryEntryTest {

    @Mock
    private Segment<Integer, String> segment;

    @Mock
    private Segment<Integer, String> otherSegment;

    @Test
    void transitionsFromLoadingToReadyToUnloading() {
        final SegmentRegistryEntry<Integer, String> entry = new SegmentRegistryEntry<>(
                7L);

        assertTrue(entry.tryStartLoad());

        entry.finishLoad(segment);
        assertEquals(segment, entry.waitWhileLoading(8L));

        assertTrue(entry.tryStartUnload(segment));
        assertEquals(segment, entry.getValueForUnload());
        entry.finishUnload();

        assertThrows(SegmentBusyException.class,
                () -> entry.waitWhileLoading(9L));
    }

    @Test
    void invalidTransitionsFailPredictably() {
        final SegmentRegistryEntry<Integer, String> entry = new SegmentRegistryEntry<>(
                1L);

        entry.finishLoad(segment);

        assertThrows(IndexException.class, () -> entry.finishLoad(otherSegment));
        final IndexException failure = new IndexException("boom");
        assertThrows(IndexException.class, () -> entry.fail(failure));
        assertThrows(IndexException.class, entry::finishUnload);
    }

    @Test
    void readyValueDoesNotWaitForEntryLock() {
        final SegmentRegistryEntry<Integer, String> entry = new SegmentRegistryEntry<>(
                7L);
        entry.finishLoad(segment);
        final ReentrantLock lock = readLock(entry);
        final ExecutorService caller = Executors.newSingleThreadExecutor();
        lock.lock();
        try {
            final Future<Segment<Integer, String>> cacheHit = caller
                    .submit(() -> entry.waitWhileLoading(8L));

            assertSame(segment,
                    assertDoesNotThrow(() -> cacheHit.get(250,
                            TimeUnit.MILLISECONDS)));
        } finally {
            lock.unlock();
            caller.shutdownNow();
        }
    }

    private static ReentrantLock readLock(final SegmentRegistryEntry<?, ?> entry) {
        try {
            final Field field = SegmentRegistryEntry.class
                    .getDeclaredField("lock");
            field.setAccessible(true);
            return (ReentrantLock) field.get(entry);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException("Unable to read entry lock", ex);
        }
    }
}
