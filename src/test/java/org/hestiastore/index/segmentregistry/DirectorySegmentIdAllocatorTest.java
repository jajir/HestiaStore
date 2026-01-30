package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class DirectorySegmentIdAllocatorTest {

    @Test
    void nextIdStartsAtOneWhenEmpty() {
        final AsyncDirectory directory = AsyncDirectoryAdapter
                .wrap(new MemDirectory());

        final DirectorySegmentIdAllocator allocator = new DirectorySegmentIdAllocator(
                directory);

        assertEquals(SegmentId.of(1), allocator.nextId());
        assertEquals(SegmentId.of(2), allocator.nextId());
    }

    @Test
    void nextIdUsesMaxSegmentDirectory() {
        final MemDirectory directory = new MemDirectory();
        directory.mkdir("segment-00001");
        directory.mkdir("segment-00005");
        directory.touch("index.map");
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);

        final DirectorySegmentIdAllocator allocator = new DirectorySegmentIdAllocator(
                asyncDirectory);

        assertEquals(SegmentId.of(6), allocator.nextId());
    }

    @Test
    void nextIdIsThreadSafe() throws Exception {
        final MemDirectory directory = new MemDirectory();
        directory.mkdir("segment-00003");
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        final DirectorySegmentIdAllocator allocator = new DirectorySegmentIdAllocator(
                asyncDirectory);
        final int threads = 4;
        final int perThread = 50;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch ready = new CountDownLatch(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        final Set<Integer> ids = new ConcurrentSkipListSet<>();

        try {
            for (int t = 0; t < threads; t++) {
                executor.execute(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            ids.add(allocator.nextId().getId());
                        }
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }

            assertTrue(ready.await(5, TimeUnit.SECONDS),
                    "Workers did not start in time");
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS),
                    "Workers did not finish in time");
        } finally {
            executor.shutdownNow();
        }

        final int expected = threads * perThread;
        assertEquals(expected, ids.size());
        assertEquals(4, ids.iterator().next().intValue());
        assertEquals(4 + expected - 1,
                ids.stream().mapToInt(Integer::intValue).max().orElse(-1));
    }
}
