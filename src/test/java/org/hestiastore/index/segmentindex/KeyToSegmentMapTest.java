package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.junit.jupiter.api.Test;

class KeyToSegmentMapTest {

    @Test
    void findNewSegmentIdUsesMaxPlusOne() {
        final KeyToSegmentMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(0)), Entry.of(20, SegmentId.of(2))));
        assertEquals(SegmentId.of(3), cache.findNewSegmentId());
    }

    @Test
    void findNewSegmentIdUsesHighestIdNotKeyOrder() {
        final KeyToSegmentMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(5)), Entry.of(20, SegmentId.of(1))));
        // highest id is 5 even though its key is lower
        assertEquals(SegmentId.of(6), cache.findNewSegmentId());
    }

    @Test
    void findNewSegmentIdBridgesGaps() {
        final KeyToSegmentMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(5, SegmentId.of(1)), Entry.of(6, SegmentId.of(3))));
        assertEquals(SegmentId.of(4), cache.findNewSegmentId());
    }

    @Test
    void findNewSegmentIdStartsAtZeroWhenEmpty() {
        final KeyToSegmentMap<Integer> cache = newCacheWithEntries(List.of());
        assertEquals(SegmentId.of(0), cache.findNewSegmentId());
    }

    @Test
    void insertSegmentRejectsDuplicateId() {
        final KeyToSegmentMap<Integer> cache = newCacheWithEntries(List.of());
        cache.insertSegment(5, SegmentId.of(0));
        assertThrows(IllegalArgumentException.class,
                () -> cache.insertSegment(6, SegmentId.of(0)));
    }

    @Test
    void applySplitPlan_replaces_old_segment_with_lower_and_upper() {
        final KeyToSegmentMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                SegmentId.of(1), SegmentId.of(3), SegmentId.of(4), 1, 5,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);

        final String previous = System.getProperty(
                "hestiastore.enforceSplitLockOrder");
        System.setProperty("hestiastore.enforceSplitLockOrder", "false");
        try {
            assertTrue(cache.applySplitPlan(plan));
        } finally {
            if (previous == null) {
                System.clearProperty("hestiastore.enforceSplitLockOrder");
            } else {
                System.setProperty("hestiastore.enforceSplitLockOrder",
                        previous);
            }
        }

        assertEquals(List.of(SegmentId.of(3), SegmentId.of(4), SegmentId.of(2)),
                cache.getSegmentIds());
    }

    @Test
    void applySplitPlan_replaces_old_segment_when_compacted() {
        final KeyToSegmentMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(2))));
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                SegmentId.of(1), SegmentId.of(3), null, 1, 10,
                SegmentSplitterResult.SegmentSplittingStatus.COMPACTED);

        final String previous = System.getProperty(
                "hestiastore.enforceSplitLockOrder");
        System.setProperty("hestiastore.enforceSplitLockOrder", "false");
        try {
            assertTrue(cache.applySplitPlan(plan));
        } finally {
            if (previous == null) {
                System.clearProperty("hestiastore.enforceSplitLockOrder");
            } else {
                System.setProperty("hestiastore.enforceSplitLockOrder",
                        previous);
            }
        }

        assertEquals(List.of(SegmentId.of(3), SegmentId.of(2)),
                cache.getSegmentIds());
    }

    @Test
    void applySplitPlan_requires_registry_lock_when_enforced() {
        final KeyToSegmentMap<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1))));
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 1, 5,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
        final String previousEnforce = System.getProperty(
                "hestiastore.enforceSplitLockOrder");
        final String previousRegistry = System.getProperty(
                "hestiastore.registryLockHeld");
        final String previousKeyMap = System
                .getProperty("hestiastore.keyMapLockHeld");
        System.setProperty("hestiastore.enforceSplitLockOrder", "true");
        System.setProperty("hestiastore.keyMapLockHeld", "true");
        System.clearProperty("hestiastore.registryLockHeld");
        try {
            assertThrows(IllegalStateException.class,
                    () -> cache.applySplitPlan(plan));
        } finally {
            if (previousEnforce == null) {
                System.clearProperty("hestiastore.enforceSplitLockOrder");
            } else {
                System.setProperty("hestiastore.enforceSplitLockOrder",
                        previousEnforce);
            }
            if (previousRegistry == null) {
                System.clearProperty("hestiastore.registryLockHeld");
            } else {
                System.setProperty("hestiastore.registryLockHeld",
                        previousRegistry);
            }
            if (previousKeyMap == null) {
                System.clearProperty("hestiastore.keyMapLockHeld");
            } else {
                System.setProperty("hestiastore.keyMapLockHeld",
                        previousKeyMap);
            }
        }
    }

    @Test
    void synchronizedAdapterDelegatesToCache() {
        final KeyToSegmentMap<Integer> cache = newCacheWithEntries(List.of());
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
        final KeyToSegmentMap<Integer> cache = newCacheWithEntries(List.of());
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

    private KeyToSegmentMap<Integer> newCacheWithEntries(
            final List<Entry<Integer, SegmentId>> entries) {
        final MemDirectory dir = new MemDirectory();
        final var sdf = org.hestiastore.index.sorteddatafile.SortedDataFile
                .<Integer, SegmentId>builder()//
                .withAsyncDirectory(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(dir))//
                .withFileName("index.map")//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorSegmentId())//
                .build();
        // seed file contents
        sdf.openWriterTx()
                .execute(writer -> entries.stream().sorted(
                        (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey()))
                        .forEach(writer::write));
        return new KeyToSegmentMap<>(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(dir),
                new TypeDescriptorInteger());
    }
}
