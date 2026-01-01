package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class SegmentSynchronizationAdapterConcurrencyTest {

    @Test
    void read_operations_do_not_block_each_other() throws Exception {
        try (SegmentImplSynchronizationAdapter<Integer, String> segment = newAdapter()) {
            final EntryIterator<Integer, String> iterator = segment
                    .openIterator();
            final ExecutorService executor = Executors
                    .newSingleThreadExecutor();
            try {
                final Future<String> future = executor
                        .submit(() -> segment.get(1));
                assertNull(future.get(1, TimeUnit.SECONDS));
            } finally {
                iterator.close();
                executor.shutdownNow();
            }
        }
    }

    @Test
    void write_operation_waits_for_reader_to_close() throws Exception {
        try (SegmentImplSynchronizationAdapter<Integer, String> segment = newAdapter()) {
            final EntryIterator<Integer, String> iterator = segment
                    .openIterator();
            final ExecutorService executor = Executors
                    .newSingleThreadExecutor();
            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch acquired = new CountDownLatch(1);
            final CountDownLatch closeSignal = new CountDownLatch(1);
            try {
                final Future<?> future = executor.submit(() -> {
                    started.countDown();
                    segment.executeWithWriteLock(() -> {
                        acquired.countDown();
                        try {
                            closeSignal.await();
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(
                                    "Interrupted while holding write lock", e);
                        }
                        return null;
                    });
                    return null;
                });
                assertTrue(started.await(1, TimeUnit.SECONDS),
                        "Writer task did not start");
                assertTrue(acquired.await(2, TimeUnit.SECONDS),
                        "Writer did not acquire lock");
                closeSignal.countDown();
                future.get(2, TimeUnit.SECONDS);
                iterator.close();
            } finally {
                if (!iterator.wasClosed()) {
                    iterator.close();
                }
                closeSignal.countDown();
                executor.shutdownNow();
            }
        }
    }

    @Test
    void read_operation_waits_for_writer_to_close() throws Exception {
        try (SegmentImplSynchronizationAdapter<Integer, String> segment = newAdapter()) {
            final ExecutorService writerExecutor = Executors
                    .newSingleThreadExecutor();
            final ExecutorService readerExecutor = Executors
                    .newSingleThreadExecutor();
            final CountDownLatch writerAcquired = new CountDownLatch(1);
            final CountDownLatch writerRelease = new CountDownLatch(1);
            final CountDownLatch readerStarted = new CountDownLatch(1);
            final CountDownLatch readerAcquired = new CountDownLatch(1);
            final CountDownLatch readerRelease = new CountDownLatch(1);
            try {
                final Future<?> writerFuture = writerExecutor.submit(() -> {
                    segment.executeWithWriteLock(() -> {
                        writerAcquired.countDown();
                        try {
                            writerRelease.await();
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(
                                    "Interrupted while holding write lock", e);
                        }
                        return null;
                    });
                    return null;
                });
                assertTrue(writerAcquired.await(1, TimeUnit.SECONDS),
                        "Writer did not acquire lock");
                final Future<?> readerFuture = readerExecutor.submit(() -> {
                    readerStarted.countDown();
                    try (EntryIterator<Integer, String> iterator = segment
                            .openIterator()) {
                        readerAcquired.countDown();
                        readerRelease.await();
                    }
                    return null;
                });
                assertTrue(readerStarted.await(1, TimeUnit.SECONDS),
                        "Reader task did not start");
                assertFalse(readerAcquired.await(250, TimeUnit.MILLISECONDS),
                        "Reader acquired lock while writer was open");
                writerRelease.countDown();
                assertTrue(readerAcquired.await(2, TimeUnit.SECONDS),
                        "Reader did not acquire lock after writer closed");
                readerRelease.countDown();
                writerFuture.get(2, TimeUnit.SECONDS);
                readerFuture.get(2, TimeUnit.SECONDS);
            } finally {
                writerRelease.countDown();
                readerRelease.countDown();
                writerExecutor.shutdownNow();
                readerExecutor.shutdownNow();
            }
        }
    }

    @Test
    void second_writer_waits_for_first_writer_to_close() throws Exception {
        try (SegmentImplSynchronizationAdapter<Integer, String> segment = newAdapter()) {
            final ExecutorService firstExecutor = Executors
                    .newSingleThreadExecutor();
            final ExecutorService secondExecutor = Executors
                    .newSingleThreadExecutor();
            final CountDownLatch firstAcquired = new CountDownLatch(1);
            final CountDownLatch firstRelease = new CountDownLatch(1);
            final CountDownLatch secondStarted = new CountDownLatch(1);
            final CountDownLatch secondAcquired = new CountDownLatch(1);
            final CountDownLatch secondRelease = new CountDownLatch(1);
            try {
                final Future<?> firstFuture = firstExecutor.submit(() -> {
                    segment.executeWithWriteLock(() -> {
                        firstAcquired.countDown();
                        try {
                            firstRelease.await();
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(
                                    "Interrupted while holding write lock", e);
                        }
                        return null;
                    });
                    return null;
                });
                assertTrue(firstAcquired.await(1, TimeUnit.SECONDS),
                        "First writer did not acquire lock");
                final Future<?> secondFuture = secondExecutor.submit(() -> {
                    secondStarted.countDown();
                    segment.executeWithWriteLock(() -> {
                        secondAcquired.countDown();
                        try {
                            secondRelease.await();
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(
                                    "Interrupted while holding write lock", e);
                        }
                        return null;
                    });
                    return null;
                });
                assertTrue(secondStarted.await(1, TimeUnit.SECONDS),
                        "Second writer task did not start");
                assertFalse(secondAcquired.await(250, TimeUnit.MILLISECONDS),
                        "Second writer acquired lock while first writer was open");
                firstRelease.countDown();
                assertTrue(secondAcquired.await(2, TimeUnit.SECONDS),
                        "Second writer did not acquire lock after close");
                secondRelease.countDown();
                firstFuture.get(2, TimeUnit.SECONDS);
                secondFuture.get(2, TimeUnit.SECONDS);
            } finally {
                firstRelease.countDown();
                secondRelease.countDown();
                firstExecutor.shutdownNow();
                secondExecutor.shutdownNow();
            }
        }
    }

    private static SegmentImplSynchronizationAdapter<Integer, String> newAdapter() {
        final Directory directory = new MemDirectory();
        final Segment<Integer, String> segment = Segment
                .<Integer, String>builder()//
                .withAsyncDirectory(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))//
                .withId(SegmentId.of(1))//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
        return new SegmentImplSynchronizationAdapter<>(segment);
    }
}
