package org.hestiastore.index.segmentindex.routemap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
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

class SegmentRouteMapTest {

    @Test
    void insertSegmentRejectsDuplicateId() {
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(
                List.of());
        final SegmentId existingId = SegmentId.of(0);
        cache.insertSegment(5, existingId);
        assertThrows(IllegalArgumentException.class,
                () -> cache.insertSegment(6, existingId));
    }

    @Test
    void insertSegmentRejectsDuplicateBoundaryKey() {
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1))));

        assertThrows(IllegalArgumentException.class,
                () -> cache.insertSegment(10, SegmentId.of(2)));
    }

    @Test
    void findSegmentIdForKeyRoutesPastLastBoundaryToTail() {
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));

        assertEquals(SegmentId.of(2), cache.findSegmentIdForKey(31));
        assertEquals(SegmentId.of(2),
                cache.snapshot().findSegmentIdForKey(31));
    }

    @Test
    void extendMaxKeyIfNeededDoesNotMutateNonEmptyTailBoundary() {
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(
                List.of());
        cache.extendMaxKeyIfNeeded(10);
        final RouteMapSnapshot<Integer> before = cache.snapshot();
        final List<SegmentId> segmentIdsBefore = cache.getSegmentIds();

        cache.extendMaxKeyIfNeeded(20);

        assertSame(before, cache.snapshot());
        assertTrue(cache.isAtVersion(before.version()));
        assertEquals(segmentIdsBefore, cache.getSegmentIds());
        assertEquals(SegmentId.of(0), cache.findSegmentIdForKey(20));
    }

    @Test
    void snapshotReusesPublishedInstanceUntilRouteChanges() {
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(
                List.of());
        final RouteMapSnapshot<Integer> initial = cache.snapshot();

        assertSame(initial, cache.snapshot());

        cache.extendMaxKeyIfNeeded(10);
        final RouteMapSnapshot<Integer> published = cache.snapshot();

        assertNotSame(initial, published);
        assertEquals(initial.version() + 1, published.version());
        assertSame(published, cache.snapshot());

        cache.extendMaxKeyIfNeeded(20);

        assertSame(published, cache.snapshot());
    }

    @Test
    void tryReplaceRouteWithSplit_replacesOldSegmentWithLowerAndUpper() {
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));
        final RouteSplitPlan<Integer> routeSplit = new RouteSplitPlan<>(
                SegmentId.of(1), SegmentId.of(3), SegmentId.of(4), 5, null);

        assertTrue(cache.tryReplaceRouteWithSplit(routeSplit));

        assertEquals(List.of(SegmentId.of(3), SegmentId.of(4), SegmentId.of(2)),
                cache.getSegmentIds());
    }

    @Test
    void tryReplaceTailRouteWithSplitUsesUpperMaxKey() {
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1))));
        final RouteSplitPlan<Integer> routeSplit = new RouteSplitPlan<>(
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
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));
        final RouteSplitPlan<Integer> routeSplit = new RouteSplitPlan<>(
                SegmentId.of(1), SegmentId.of(3), SegmentId.of(4), 5, 8);

        assertTrue(cache.tryReplaceRouteWithSplit(routeSplit));

        assertEquals(SegmentId.of(3), cache.findSegmentIdForKey(5));
        assertEquals(SegmentId.of(4), cache.findSegmentIdForKey(6));
        assertEquals(SegmentId.of(4), cache.findSegmentIdForKey(10));
        assertEquals(SegmentId.of(2), cache.findSegmentIdForKey(11));
    }

    @Test
    void tryReplaceTailRouteWithSplitRequiresUpperMaxKey() {
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1))));
        final RouteSplitPlan<Integer> routeSplit = new RouteSplitPlan<>(
                SegmentId.of(1), SegmentId.of(3), SegmentId.of(4), 5, null);

        assertThrows(IllegalArgumentException.class,
                () -> cache.tryReplaceRouteWithSplit(routeSplit));
    }

    @Test
    void tryReplaceRouteWithSplitRejectsInvalidChildBoundaryOrder() {
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1))));
        final RouteSplitPlan<Integer> routeSplit = new RouteSplitPlan<>(
                SegmentId.of(1), SegmentId.of(3), SegmentId.of(4), 40, 30);

        assertThrows(IllegalArgumentException.class,
                () -> cache.tryReplaceRouteWithSplit(routeSplit));
    }

    @Test
    void tryReplaceRouteWithSplitRejectsDuplicateBoundaryKey() {
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(20, SegmentId.of(2)),
                Entry.of(30, SegmentId.of(3))));
        final RouteSplitPlan<Integer> routeSplit = new RouteSplitPlan<>(
                SegmentId.of(2), SegmentId.of(4), SegmentId.of(5), 10, 25);

        assertThrows(IllegalArgumentException.class,
                () -> cache.tryReplaceRouteWithSplit(routeSplit));
    }

    @Test
    void tryReplaceRouteWithSplitReturnsFalseWithoutMutationWhenRouteMissing() {
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));
        final RouteMapSnapshot<Integer> before = cache.snapshot();
        final RouteSplitPlan<Integer> routeSplit = new RouteSplitPlan<>(
                SegmentId.of(9), SegmentId.of(3), SegmentId.of(4), 5, null);

        assertFalse(cache.tryReplaceRouteWithSplit(routeSplit));

        assertEquals(before.getSegmentIds(SegmentWindow.unbounded()),
                cache.getSegmentIds());
        assertSame(before, cache.snapshot());
        assertTrue(cache.isAtVersion(before.version()));
    }

    @Test
    void directMutationsPublishExactlyOneNewSnapshot() {
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(
                List.of());
        RouteMapSnapshot<Integer> previous = cache.snapshot();

        cache.insertKeyToSegment(10);
        RouteMapSnapshot<Integer> current = cache.snapshot();
        assertNotSame(previous, current);
        assertEquals(previous.version() + 1, current.version());
        previous = current;

        cache.insertKeyToSegment(11);
        assertSame(previous, cache.snapshot());

        cache.insertSegment(20, SegmentId.of(1));
        current = cache.snapshot();
        assertNotSame(previous, current);
        assertEquals(previous.version() + 1, current.version());
        previous = current;

        cache.updateSegmentMaxKey(SegmentId.of(1), 30);
        current = cache.snapshot();
        assertNotSame(previous, current);
        assertEquals(previous.version() + 1, current.version());
        previous = current;

        cache.removeSegmentRoute(SegmentId.of(1));
        current = cache.snapshot();
        assertNotSame(previous, current);
        assertEquals(previous.version() + 1, current.version());

        cache.removeSegmentRoute(SegmentId.of(1));
        assertSame(current, cache.snapshot());
    }

    @Test
    void mapAppliesDirectMutations() {
        final PersistentSegmentRouteMap<Integer> adapter = newCacheWithEntries(
                List.of());

        adapter.insertSegment(1, SegmentId.of(0));
        adapter.insertSegment(10, SegmentId.of(1));

        assertEquals(List.of(SegmentId.of(0), SegmentId.of(1)),
                adapter.getSegmentIds());
        adapter.close();
    }

    @Test
    void mapHandlesConcurrentInsertions() throws Exception {
        final PersistentSegmentRouteMap<Integer> adapter = newCacheWithEntries(
                List.of());
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
        final PersistentSegmentRouteMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(20, SegmentId.of(2)),
                Entry.of(30, SegmentId.of(3))));
        final RouteMapSnapshot<Integer> snapshot = cache.snapshot();

        final RouteSplitPlan<Integer> routeSplit = new RouteSplitPlan<>(
                SegmentId.of(2), SegmentId.of(4), SegmentId.of(5), 15, null);
        assertTrue(cache.tryReplaceRouteWithSplit(routeSplit));

        assertEquals(List.of(SegmentId.of(1), SegmentId.of(2), SegmentId.of(3)),
                snapshot.getSegmentIds(SegmentWindow.unbounded()));
        final RouteMapSnapshot<Integer> published = cache.snapshot();
        assertNotSame(snapshot, published);
        assertEquals(snapshot.version() + 1, published.version());
        assertSame(published, cache.snapshot());
        assertTrue(cache.isAtVersion(published.version()));
        assertFalse(cache.isAtVersion(snapshot.version()));
    }

    private PersistentSegmentRouteMap<Integer> newCacheWithEntries(
            final List<Entry<Integer, SegmentId>> entries) {
        final MemDirectory dir = new MemDirectory();
        final var sdf = org.hestiastore.index.sorteddatafile.SortedDataFile
                .<Integer, SegmentId>builder()//
                .withDirectory(
                        dir)//
                .withFileName("index.map")//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new SegmentIdTypeDescriptor())//
                .build();
        // seed file contents
        sdf.openWriterTx()
                .execute(writer -> entries.stream().sorted(
                        (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey()))
                        .forEach(writer::write));
        return new PersistentSegmentRouteMap<>(
                dir,
                new TypeDescriptorInteger());
    }
}
