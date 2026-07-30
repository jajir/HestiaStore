package org.hestiastore.index.segmentindex.routemap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentRouteMapSplitConcurrencyTest {

    private PersistentSegmentRouteMap<Integer> adapter;
    private ExecutorService executor;
    private RouteSplitPlan<Integer> plan;

    @BeforeEach
    void setUp() {
        adapter = newCacheWithEntries(
                List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));
        plan = new RouteSplitPlan<>(SegmentId.of(1), SegmentId.of(3),
                SegmentId.of(4), 5, null);
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        if (adapter != null && !adapter.wasClosed()) {
            adapter.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
        adapter = null;
        executor = null;
        plan = null;
    }

    @Test
    void concurrentSnapshotsDuringSplitKeepRoutesAndVersionTogether() {
        final RouteMapSnapshot<Integer> initial = adapter.snapshot();
        final long initialVersion = initial.version();
        final SegmentId initialSegmentId = SegmentId.of(1);
        final SegmentId splitSegmentId = SegmentId.of(3);
        final AtomicBoolean keepReading = new AtomicBoolean(true);
        final AtomicBoolean inconsistent = new AtomicBoolean(false);
        final AtomicInteger nextKey = new AtomicInteger(31);
        final CountDownLatch ready = new CountDownLatch(2);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch startedOps = new CountDownLatch(2);
        final CountDownLatch publishedSeen = new CountDownLatch(1);

        final Future<?> reader = executor.submit(() -> {
            ready.countDown();
            await(start);
            startedOps.countDown();
            while (keepReading.get()) {
                final RouteMapSnapshot<Integer> current = adapter.snapshot();
                final SegmentId routedSegmentId = current
                        .findSegmentIdForKey(5);
                if (current.version() == initialVersion) {
                    if (!initialSegmentId.equals(routedSegmentId)) {
                        inconsistent.set(true);
                        break;
                    }
                } else if (current.version() == initialVersion + 1) {
                    if (!splitSegmentId.equals(routedSegmentId)) {
                        inconsistent.set(true);
                        break;
                    }
                    publishedSeen.countDown();
                } else {
                    inconsistent.set(true);
                    break;
                }
            }
        });

        final Future<?> writer = executor.submit(() -> {
            ready.countDown();
            await(start);
            for (int i = 0; i < 500; i++) {
                adapter.insertKeyToSegment(nextKey.getAndIncrement());
                if (i == 0) {
                    startedOps.countDown();
                }
            }
        });

        assertTrue(await(ready, 5), "Workers did not start in time");
        start.countDown();
        assertTrue(await(startedOps, 5),
                "Workers did not perform initial ops in time");

        try {
            assertTrue(adapter.tryReplaceRouteWithSplit(plan));
            assertTrue(await(publishedSeen, 5),
                    "Reader did not observe the published split in time");
            adapter.flushIfDirty();
        } finally {
            keepReading.set(false);
        }

        awaitFuture(reader, "Reader did not finish in time");
        awaitFuture(writer, "Writer did not finish in time");

        assertFalse(inconsistent.get());
    }

    private void awaitFuture(final Future<?> future, final String message) {
        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(message + ": " + e.getMessage());
        } catch (final Exception e) {
            fail(message + ": " + e.getMessage());
        }
    }

    private void await(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean await(final CountDownLatch latch, final int timeoutSeconds) {
        try {
            return latch.await(timeoutSeconds, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private PersistentSegmentRouteMap<Integer> newCacheWithEntries(
            final List<Entry<Integer, SegmentId>> entries) {
        final MemDirectory dir = new MemDirectory();
        final SortedDataFile<Integer, SegmentId> sdf = SortedDataFile
                .<Integer, SegmentId>builder()
                .withDirectory(dir)
                .withFileName("index.map")
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new SegmentIdTypeDescriptor())
                .build();
        sdf.openWriterTx()
                .execute(writer -> entries.stream().sorted(
                        (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey()))
                        .forEach(writer::write));
        return new PersistentSegmentRouteMap<>(dir,
                new TypeDescriptorInteger());
    }
}
