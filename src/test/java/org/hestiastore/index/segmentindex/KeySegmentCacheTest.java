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

class KeySegmentCacheTest {

    @Test
    void findNewSegmentIdUsesMaxPlusOne() {
        final KeySegmentCache<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(0)), Entry.of(20, SegmentId.of(2))));
        assertEquals(SegmentId.of(3), cache.findNewSegmentId());
    }

    @Test
    void findNewSegmentIdUsesHighestIdNotKeyOrder() {
        final KeySegmentCache<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(5)), Entry.of(20, SegmentId.of(1))));
        // highest id is 5 even though its key is lower
        assertEquals(SegmentId.of(6), cache.findNewSegmentId());
    }

    @Test
    void findNewSegmentIdBridgesGaps() {
        final KeySegmentCache<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(5, SegmentId.of(1)), Entry.of(6, SegmentId.of(3))));
        assertEquals(SegmentId.of(4), cache.findNewSegmentId());
    }

    @Test
    void findNewSegmentIdStartsAtZeroWhenEmpty() {
        final KeySegmentCache<Integer> cache = newCacheWithEntries(List.of());
        assertEquals(SegmentId.of(0), cache.findNewSegmentId());
    }

    @Test
    void insertSegmentRejectsDuplicateId() {
        final KeySegmentCache<Integer> cache = newCacheWithEntries(List.of());
        cache.insertSegment(5, SegmentId.of(0));
        assertThrows(IllegalArgumentException.class,
                () -> cache.insertSegment(6, SegmentId.of(0)));
    }

    @Test
    void synchronizedAdapterDelegatesToCache() {
        final KeySegmentCache<Integer> cache = newCacheWithEntries(List.of());
        final KeySegmentCacheSynchronizedAdapter<Integer> adapter = new KeySegmentCacheSynchronizedAdapter<>(
                cache);

        adapter.insertSegment(1, SegmentId.of(0));
        adapter.insertSegment(10, SegmentId.of(1));

        assertEquals(List.of(SegmentId.of(0), SegmentId.of(1)),
                cache.getSegmentIds());
        adapter.close();
    }

    @Test
    void synchronizedAdapterHandlesConcurrentInsertions() throws Exception {
        final KeySegmentCache<Integer> cache = newCacheWithEntries(List.of());
        final KeySegmentCacheSynchronizedAdapter<Integer> adapter = new KeySegmentCacheSynchronizedAdapter<>(
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

        adapter.close();
        assertEquals(threads * perThread, adapter.getSegmentIds().size());
    }

    private KeySegmentCache<Integer> newCacheWithEntries(
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
        return new KeySegmentCache<>(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(dir),
                new TypeDescriptorInteger());
    }
}
