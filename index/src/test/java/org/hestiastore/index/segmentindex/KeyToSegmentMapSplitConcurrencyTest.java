package org.hestiastore.index.segmentindex;

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
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KeyToSegmentMapSplitConcurrencyTest {

    private KeyToSegmentMapSynchronizedAdapter<Integer> adapter;
    private ExecutorService executor;
    private SegmentSplitApplyPlan<Integer, String> plan;

    @BeforeEach
    void setUp() {
        final KeyToSegmentMap<Integer> rawKeyMap = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));
        adapter = new KeyToSegmentMapSynchronizedAdapter<>(rawKeyMap);
        plan = new SegmentSplitApplyPlan<>(SegmentId.of(1), SegmentId.of(3),
                SegmentId.of(4), 1, 5,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
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
    void concurrent_get_put_during_split_never_sees_missing_mapping() {
        final AtomicBoolean missing = new AtomicBoolean(false);
        final AtomicInteger nextKey = new AtomicInteger(31);
        final CountDownLatch ready = new CountDownLatch(2);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch startedOps = new CountDownLatch(2);

        final Future<?> reader = executor.submit(() -> {
            ready.countDown();
            await(start);
            for (int i = 0; i < 1_000; i++) {
                if (adapter.findSegmentId(5) == null) {
                    missing.set(true);
                    break;
                }
                if (i == 0) {
                    startedOps.countDown();
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

        assertTrue(adapter.applySplitPlan(plan));
        adapter.optionalyFlush();

        awaitFuture(reader, "Reader did not finish in time");
        awaitFuture(writer, "Writer did not finish in time");

        assertFalse(missing.get());
    }

    private void awaitFuture(final Future<?> future, final String message) {
        try {
            future.get(5, TimeUnit.SECONDS);
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

    private KeyToSegmentMap<Integer> newCacheWithEntries(
            final List<Entry<Integer, SegmentId>> entries) {
        final MemDirectory dir = new MemDirectory();
        final SortedDataFile<Integer, SegmentId> sdf = SortedDataFile
                .<Integer, SegmentId>builder()
                .withDirectory(dir)
                .withFileName("index.map")
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorSegmentId())
                .build();
        sdf.openWriterTx()
                .execute(writer -> entries.stream().sorted(
                        (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey()))
                        .forEach(writer::write));
        return new KeyToSegmentMap<>(dir,
                new TypeDescriptorInteger());
    }
}
