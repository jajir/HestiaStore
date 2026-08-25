package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.senku.SenkuReady;
import org.hestiastore.index.senku.SenkuWriting;
import org.junit.jupiter.api.Test;

class SenkuRuntimeTest {

    @Test
    void secondWritingHandleFailsWithoutReleasingFirstHandleLock() {
        final MemDirectory directory = new MemDirectory();
        final SenkuWriting<Integer, Long> first = create(directory, 1);

        assertThrows(IndexException.class, () -> create(directory, 1));
        first.put(1, 1L);
        final SenkuReady<Integer, Long> ready = first.finishWriting();
        try (Stream<Entry<Integer, Long>> stream =
                ready.openStream()) {
            assertEquals(List.of(Entry.of(1, 1L)), stream.toList());
        }
        ready.close();
    }

    @Test
    void failedReadyOpenReleasesItsAcquiredLock() {
        final MemDirectory directory = new MemDirectory();

        assertThrows(IndexException.class,
                () -> SenkuRuntime.open(directory, new TypeDescriptorInteger(),
                        new TypeDescriptorLong(), 1_024));

        final SenkuReady<Integer, Long> ready = create(directory, 1)
                .finishWriting();
        ready.close();
    }

    @Test
    void secondReadyOpenDoesNotReleaseFirstReadyLock() {
        final MemDirectory directory = new MemDirectory();
        final SenkuReady<Integer, Long> first = create(directory, 1)
                .finishWriting();

        assertThrows(IndexException.class,
                () -> SenkuRuntime.open(directory, new TypeDescriptorInteger(),
                        new TypeDescriptorLong(), 1_024));
        assertTrue(directory.isFileExists(SenkuFileNames.LOCK_FILE));

        first.close();
        final SenkuReady<Integer, Long> reopened = SenkuRuntime.open(directory,
                new TypeDescriptorInteger(), new TypeDescriptorLong(), 1_024);
        reopened.close();
    }

    @Test
    void ownedThreadsAreNamedNonDaemonAndTerminateAfterReadyClose() {
        final MemDirectory directory = new MemDirectory();
        final SenkuWriting<Integer, Long> writing = create(directory, 1);
        await(() -> threads("senku-coordinator-") == 1);
        assertTrue(Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName()
                        .startsWith("senku-coordinator-"))
                .noneMatch(Thread::isDaemon));

        final SenkuReady<Integer, Long> ready = writing.finishWriting();
        ready.close();
        await(() -> threads("senku-coordinator-") == 0
                && threads("senku-maintenance-") == 0);

        assertFalse(directory.isFileExists(SenkuFileNames.LOCK_FILE));
    }

    private static SenkuWriting<Integer, Long> create(
            final MemDirectory directory, final int shardCount) {
        return SenkuRuntime.create(directory, new TypeDescriptorInteger(),
                new TypeDescriptorLong(),
                (key, first, second) -> first + second, key -> key, shardCount,
                2, 3, 1, 2, 2, 2, 1_024, 3L);
    }

    private static long threads(final String prefix) {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(thread -> thread.getName().startsWith(prefix)).count();
    }

    private static void await(final BooleanSupplier condition) {
        final long deadline = System.nanoTime()
                + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        assertTrue(condition.getAsBoolean(), "Timed out waiting for condition");
    }
}
