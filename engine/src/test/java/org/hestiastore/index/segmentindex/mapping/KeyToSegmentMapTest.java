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
    void insertSegmentRejectsDuplicateBoundaryKey() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1))));

        assertThrows(IllegalArgumentException.class,
                () -> cache.insertSegment(10, SegmentId.of(2)));
    }

    @Test
    void findSegmentIdForKeyRoutesPastLastBoundaryToTail() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));

        assertEquals(SegmentId.of(2), cache.findSegmentIdForKey(31));
        assertEquals(SegmentId.of(2),
                cache.snapshot().findSegmentIdForKey(31));
    }

    @Test
    void extendMaxKeyIfNeededDoesNotMutateNonEmptyTailBoundary() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(
                List.of());
        assertTrue(cache.extendMaxKeyIfNeeded(10));
        final Snapshot<Integer> before = cache.snapshot();
        final List<SegmentId> segmentIdsBefore = cache.getSegmentIds();

        assertTrue(cache.extendMaxKeyIfNeeded(20));

        assertTrue(cache.isAtVersion(before.version()));
        assertEquals(segmentIdsBefore, cache.getSegmentIds());
        assertEquals(SegmentId.of(0), cache.findSegmentIdForKey(20));
    }

    @Test
    void tryReplaceRouteWithSplit_replacesOldSegmentWithLowerAndUpper() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));
        final SegmentRouteSplit<Integer> routeSplit = new SegmentRouteSplit<>(
                SegmentId.of(1), SegmentId.of(3), SegmentId.of(4), 5, null);

        assertTrue(cache.tryReplaceRouteWithSplit(routeSplit));

        assertEquals(List.of(SegmentId.of(3), SegmentId.of(4), SegmentId.of(2)),
                cache.getSegmentIds());
    }

    @Test
    void tryReplaceTailRouteWithSplitUsesUpperMaxKey() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1))));
        final SegmentRouteSplit<Integer> routeSplit = new SegmentRouteSplit<>(
                SegmentId.of(1), SegmentId.of(3), SegmentId.of(4), 20, 40);

        assertTrue(cache.tryReplaceRouteWithSplit(routeSplit));

        assertEquals(List.of(SegmentId.of(3), SegmentId.of(4)),
                cache.getSegmentIds());
        assertEquals(SegmentId.of(3), cache.findSegmentIdForKey(20));
        assertEquals(SegmentId.of(4), cache.findSegmentIdForKey(21));
        assertEquals(SegmentId.of(4), cache.findSegmentIdForKey(41));
    }

    @Test
    void tryReplaceNonTailRouteWithSplitPreservesOldUpperBoundary() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));
        final SegmentRouteSplit<Integer> routeSplit = new SegmentRouteSplit<>(
                SegmentId.of(1), SegmentId.of(3), SegmentId.of(4), 5, 8);

        assertTrue(cache.tryReplaceRouteWithSplit(routeSplit));

        assertEquals(SegmentId.of(3), cache.findSegmentIdForKey(5));
        assertEquals(SegmentId.of(4), cache.findSegmentIdForKey(6));
        assertEquals(SegmentId.of(4), cache.findSegmentIdForKey(10));
        assertEquals(SegmentId.of(2), cache.findSegmentIdForKey(11));
    }

    @Test
    void tryReplaceTailRouteWithSplitRequiresUpperMaxKey() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1))));
        final SegmentRouteSplit<Integer> routeSplit = new SegmentRouteSplit<>(
                SegmentId.of(1), SegmentId.of(3), SegmentId.of(4), 5, null);

        assertThrows(IllegalArgumentException.class,
                () -> cache.tryReplaceRouteWithSplit(routeSplit));
    }

    @Test
    void tryReplaceRouteWithSplitRejectsInvalidChildBoundaryOrder() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1))));
        final SegmentRouteSplit<Integer> routeSplit = new SegmentRouteSplit<>(
                SegmentId.of(1), SegmentId.of(3), SegmentId.of(4), 40, 30);

        assertThrows(IllegalArgumentException.class,
                () -> cache.tryReplaceRouteWithSplit(routeSplit));
    }

    @Test
    void tryReplaceRouteWithSplitRejectsDuplicateBoundaryKey() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(20, SegmentId.of(2)),
                Entry.of(30, SegmentId.of(3))));
        final SegmentRouteSplit<Integer> routeSplit = new SegmentRouteSplit<>(
                SegmentId.of(2), SegmentId.of(4), SegmentId.of(5), 10, 25);

        assertThrows(IllegalArgumentException.class,
                () -> cache.tryReplaceRouteWithSplit(routeSplit));
    }

    @Test
    void tryReplaceRouteWithSplitReturnsFalseWithoutMutationWhenRouteMissing() {
        final KeyToSegmentMapImpl<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));
        final Snapshot<Integer> before = cache.snapshot();
        final SegmentRouteSplit<Integer> routeSplit = new SegmentRouteSplit<>(
                SegmentId.of(9), SegmentId.of(3), SegmentId.of(4), 5, null);

        assertFalse(cache.tryReplaceRouteWithSplit(routeSplit));

        assertEquals(before.getSegmentIds(SegmentWindow.unbounded()),
                cache.getSegmentIds());
        assertTrue(cache.isAtVersion(before.version()));
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

        final SegmentRouteSplit<Integer> routeSplit = new SegmentRouteSplit<>(
                SegmentId.of(2), SegmentId.of(4), SegmentId.of(5), 15, null);
        assertTrue(cache.tryReplaceRouteWithSplit(routeSplit));

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
