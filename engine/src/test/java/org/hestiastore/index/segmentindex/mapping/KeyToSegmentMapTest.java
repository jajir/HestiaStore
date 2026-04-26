package org.hestiastore.index.segmentindex.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.split.RouteSplitPlan;
import org.junit.jupiter.api.Test;

class KeyToSegmentMapTest {

    @Test
    void insertSegmentRejectsDuplicateId() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(
                List.of());
        final SegmentId existingId = SegmentId.of(0);
        cache.insertSegment(5, existingId);
        assertThrows(IllegalArgumentException.class,
                () -> cache.insertSegment(6, existingId));
    }

    @Test
    void tryApplySplitPlan_replacesOldSegmentWithLowerAndUpper() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));
        final RouteSplitPlan<Integer> plan = new RouteSplitPlan<>(SegmentId.of(1),
                SegmentId.of(3), SegmentId.of(4), 5,
                RouteSplitPlan.SplitMode.SPLIT);

        assertTrue(cache.tryApplySplitPlan(plan));

        assertEquals(List.of(SegmentId.of(3), SegmentId.of(4), SegmentId.of(2)),
                cache.getSegmentIds());
    }

    @Test
    void tryApplySplitPlan_replacesOldSegmentWhenCompacted() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));
        final RouteSplitPlan<Integer> plan = new RouteSplitPlan<>(SegmentId.of(1),
                SegmentId.of(3), null, 10,
                RouteSplitPlan.SplitMode.COMPACTED);

        assertTrue(cache.tryApplySplitPlan(plan));

        assertEquals(List.of(SegmentId.of(3), SegmentId.of(2)),
                cache.getSegmentIds());
    }

    @Test
    void synchronizedAdapterDelegatesToCache() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(
                List.of());
        final KeyToSegmentMapSynchronizedAdapter<Integer> adapter = new KeyToSegmentMapSynchronizedAdapter<>(
                cache);

        adapter.insertSegment(1, SegmentId.of(0));
        adapter.insertSegment(10, SegmentId.of(1));

        assertEquals(List.of(SegmentId.of(0), SegmentId.of(1)),
                cache.getSegmentIds());
        adapter.close();
    }

    @Test
    void synchronizedAdapterHandlesConcurrentInsertions() throws Exception {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(
                List.of());
        final KeyToSegmentMapSynchronizedAdapter<Integer> adapter = new KeyToSegmentMapSynchronizedAdapter<>(
                cache);
        final int threads = 4;
        final int perThread = 50;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch ready = new CountDownLatch(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);

        try {
            for (int t = 0; t < threads; t++) {
                final int base = t * perThread;
                executor.execute(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            final int value = base + i;
                            adapter.insertSegment(value, SegmentId.of(value));
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

        assertEquals(threads * perThread, adapter.getSegmentIds().size());
        adapter.close();
    }

    @Test
    void snapshotRetainsSegmentOrderAfterLaterSplitMutation() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(20, SegmentId.of(2)),
                Entry.of(30, SegmentId.of(3))));
        final Snapshot<Integer> snapshot = cache.snapshot();

        final RouteSplitPlan<Integer> plan = new RouteSplitPlan<>(SegmentId.of(2),
                SegmentId.of(4), SegmentId.of(5), 15,
                RouteSplitPlan.SplitMode.SPLIT);
        assertTrue(cache.tryApplySplitPlan(plan));

        assertEquals(List.of(SegmentId.of(1), SegmentId.of(2), SegmentId.of(3)),
                snapshot.getSegmentIds(SegmentWindow.unbounded()));
        assertTrue(cache.isAtVersion(cache.snapshot().version()));
        assertFalse(cache.isAtVersion(snapshot.version()));
    }

    private KeyToSegmentMapImpl<Integer> newCacheWithEntries(
            final List<Entry<Integer, SegmentId>> entries) {
        final MemDirectory dir = new MemDirectory();
        final var sdf = org.hestiastore.index.sorteddatafile.SortedDataFile
                .<Integer, SegmentId>builder()//
                .withDirectory(
                        dir)//
                .withFileName("index.map")//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorSegmentId())//
                .build();
        // seed file contents
        sdf.openWriterTx()
                .execute(writer -> entries.stream().sorted(
                        (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey()))
                        .forEach(writer::write));
        return new KeyToSegmentMapImpl<>(
                dir,
                new TypeDescriptorInteger());
    }
}
