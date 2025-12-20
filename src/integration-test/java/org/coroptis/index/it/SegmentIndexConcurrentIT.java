package org.coroptis.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.junit.jupiter.api.Test;

/**
 * Multi-threaded access safety check for {@link SegmentIndex} using the
 * thread-safe configuration.
 * <p>
 * The tests avoid relying on a globally deterministic ordering across threads
 * (which is not guaranteed) and instead validate deterministic end states:
 * disjoint keyspaces (order-independent) and "last write wins" for a single
 * key.
 */
class SegmentIndexConcurrentIT {

    @Test
    void concurrent_put_delete_on_disjoint_keyspaces_produces_consistent_state()
            throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = newThreadSafeConfiguration(
                "concurrent-index", 4, 15);

        final SegmentIndex<Integer, Integer> index = SegmentIndex
                .create(directory, conf);

        final int threads = 4;
        final int operationsPerThread = 200;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch doneGate = new CountDownLatch(threads);
        @SuppressWarnings("unchecked")
        final Map<Integer, Integer>[] expectedByThread = new Map[threads];

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                final Random rnd = new Random(12345L + threadId);
                final int keySpaceOffset = threadId * 1_000;
                final Map<Integer, Integer> expectedLocal = new HashMap<>();
                startGate.await();
                for (int i = 0; i < operationsPerThread; i++) {
                    final int key = keySpaceOffset + rnd.nextInt(50);
                    final double p = rnd.nextDouble();
                    if (p < 0.6) {
                        final int value = rnd.nextInt(10_000);
                        expectedLocal.put(key, value);
                        index.put(key, value);
                    } else if (p < 0.9) {
                        expectedLocal.remove(key);
                        index.delete(key);
                    } else {
                        index.get(key); // exercise reads
                    }
                }
                expectedByThread[threadId] = expectedLocal;
                doneGate.countDown();
                return null;
            });
        }

        startGate.countDown();
        assertTrue(doneGate.await(30, TimeUnit.SECONDS),
                "Writer threads did not finish in time");
        executor.shutdownNow();

        index.flush();

        final Map<Integer, Integer> expected = new java.util.HashMap<>();
        for (final Map<Integer, Integer> local : expectedByThread) {
            expected.putAll(local);
        }

        final Map<Integer, Integer> actual = index.getStream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (first, second) -> second));

        assertEquals(expected, actual);

        index.close();

        // Re-open to ensure persisted state matches expectations
        final SegmentIndex<Integer, Integer> reopened = SegmentIndex
                .open(directory, conf);
        final Map<Integer, Integer> reloaded = reopened.getStream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (first, second) -> second));
        assertEquals(expected, reloaded);
        reopened.close();
    }

    @Test
    void concurrent_mutations_on_same_key_last_write_wins() throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = newThreadSafeConfiguration(
                "concurrent-index", 4, 15);

        final SegmentIndex<Integer, Integer> index = SegmentIndex
                .create(directory, conf);

        final int threads = 4;
        final int operationsPerThread = 200;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch doneGate = new CountDownLatch(threads);
        final int key = 1;

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                final Random rnd = new Random(98765L + threadId);
                startGate.await();
                for (int i = 0; i < operationsPerThread; i++) {
                    if (rnd.nextDouble() < 0.6) {
                        index.put(key, rnd.nextInt(10_000));
                    } else {
                        index.delete(key);
                    }
                }
                doneGate.countDown();
                return null;
            });
        }

        startGate.countDown();
        assertTrue(doneGate.await(30, TimeUnit.SECONDS),
                "Writer threads did not finish in time");
        executor.shutdownNow();

        final int finalValue = 42_4242;
        index.put(key, finalValue);
        index.flush();
        assertEquals(finalValue, index.get(key));
        index.close();

        final SegmentIndex<Integer, Integer> reopened = SegmentIndex
                .open(directory, conf);
        assertEquals(finalValue, reopened.get(key));
        reopened.close();
    }

    @Test
    void concurrent_put_delete_with_background_flush_compact_produces_consistent_state()
            throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = newThreadSafeConfiguration(
                "concurrent-index-maintenance", 4, 8);

        final SegmentIndex<Integer, Integer> index = SegmentIndex
                .create(directory, conf);

        final int threads = 4;
        final int operationsPerThread = 400;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch doneGate = new CountDownLatch(threads);
        @SuppressWarnings("unchecked")
        final Map<Integer, Integer>[] expectedByThread = new Map[threads];

        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicReference<Throwable> maintenanceError = new AtomicReference<>();
        final Thread maintenance = new Thread(() -> {
            try {
                startGate.await();
                int iteration = 0;
                while (!stop.get()) {
                    index.flush();
                    if ((iteration++ % 3) == 0) {
                        index.compact();
                    }
                    Thread.yield();
                }
            } catch (final Throwable t) {
                maintenanceError.compareAndSet(null, t);
                stop.set(true);
            }
        }, "segment-index-maintenance");
        maintenance.start();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                final Random rnd = new Random(112233L + threadId);
                final int keySpaceOffset = threadId * 1_000;
                final Map<Integer, Integer> expectedLocal = new HashMap<>();
                startGate.await();
                for (int i = 0; i < operationsPerThread; i++) {
                    final int key = keySpaceOffset + rnd.nextInt(80);
                    if (rnd.nextDouble() < 0.7) {
                        final int value = rnd.nextInt(10_000);
                        expectedLocal.put(key, value);
                        index.put(key, value);
                    } else {
                        expectedLocal.remove(key);
                        index.delete(key);
                    }
                }
                expectedByThread[threadId] = expectedLocal;
                doneGate.countDown();
                return null;
            });
        }

        startGate.countDown();
        assertTrue(doneGate.await(60, TimeUnit.SECONDS),
                "Writer threads did not finish in time");

        stop.set(true);
        maintenance.join(TimeUnit.SECONDS.toMillis(10));
        assertTrue(!maintenance.isAlive(),
                "Maintenance thread did not stop in time");
        executor.shutdownNow();

        final Throwable backgroundFailure = maintenanceError.get();
        if (backgroundFailure != null) {
            throw new AssertionError("Maintenance thread failed",
                    backgroundFailure);
        }

        index.checkAndRepairConsistency();
        index.flush();

        final Map<Integer, Integer> expected = new java.util.HashMap<>();
        for (final Map<Integer, Integer> local : expectedByThread) {
            expected.putAll(local);
        }

        // Validate point-lookup visibility before asserting stream state. This
        // helps to distinguish between missing persisted entries vs. iterator
        // omissions (e.g., entries still present only in the write cache).
        for (final Map.Entry<Integer, Integer> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), index.get(entry.getKey()),
                    "Point lookup differs for key " + entry.getKey());
        }

        // Ensure any values that are still buffered in the write cache are
        // persisted before validating the full stream view.
        index.flush();

        final Map<Integer, Integer> actual = index.getStream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (first, second) -> second));
        assertEquals(expected, actual);

        index.close();

        final SegmentIndex<Integer, Integer> reopened = SegmentIndex
                .open(directory, conf);
        final Map<Integer, Integer> reloaded = reopened.getStream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (first, second) -> second));
        assertEquals(expected, reloaded);
        reopened.close();
    }

    @Test
    void concurrent_reads_do_not_return_corrupted_values() throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = newThreadSafeConfiguration(
                "concurrent-index-readers", 4, 15);
        final SegmentIndex<Integer, Integer> index = SegmentIndex
                .create(directory, conf);

        final int keyCount = 120;
        for (int key = 0; key < keyCount; key++) {
            index.put(key, encodeValue(key, 0));
        }

        final int writerThreads = 4;
        final int readerThreads = 4;
        final int writesPerThread = 500;
        final int readsPerThread = 2_000;
        final ExecutorService writers = Executors
                .newFixedThreadPool(writerThreads);
        final ExecutorService readers = Executors
                .newFixedThreadPool(readerThreads);
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch writersDone = new CountDownLatch(writerThreads);
        final CountDownLatch readersDone = new CountDownLatch(readerThreads);

        for (int t = 0; t < writerThreads; t++) {
            final int threadId = t;
            writers.submit(() -> {
                final Random rnd = new Random(556677L + threadId);
                startGate.await();
                for (int i = 0; i < writesPerThread; i++) {
                    final int key = rnd.nextInt(keyCount);
                    final int value = encodeValue(key, rnd.nextInt(1_000_000));
                    index.put(key, value);
                }
                writersDone.countDown();
                return null;
            });
        }

        for (int t = 0; t < readerThreads; t++) {
            final int threadId = t;
            readers.submit(() -> {
                final Random rnd = new Random(998877L + threadId);
                startGate.await();
                for (int i = 0; i < readsPerThread; i++) {
                    final int key = rnd.nextInt(keyCount);
                    final Integer value = index.get(key);
                    if (value != null) {
                        assertEquals(key, decodeKey(value));
                    }
                }
                readersDone.countDown();
                return null;
            });
        }

        startGate.countDown();
        assertTrue(writersDone.await(30, TimeUnit.SECONDS),
                "Writer threads did not finish in time");
        assertTrue(readersDone.await(30, TimeUnit.SECONDS),
                "Reader threads did not finish in time");
        writers.shutdownNow();
        readers.shutdownNow();

        index.flush();
        index.getStream().forEach(entry -> assertEquals(entry.getKey(),
                decodeKey(entry.getValue())));
        index.close();
    }

    @Test
    void concurrent_async_put_delete_on_disjoint_keyspaces_produces_consistent_state()
            throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = newThreadSafeConfiguration(
                "concurrent-index-async", 4, 12);
        final SegmentIndex<Integer, Integer> index = SegmentIndex
                .create(directory, conf);

        final int threads = 4;
        final int operationsPerThread = 200;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch doneGate = new CountDownLatch(threads);
        @SuppressWarnings("unchecked")
        final Map<Integer, Integer>[] expectedByThread = new Map[threads];

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                final Random rnd = new Random(4444L + threadId);
                final int keySpaceOffset = threadId * 1_000;
                final Map<Integer, Integer> expectedLocal = new HashMap<>();

                startGate.await();

                for (int i = 0; i < operationsPerThread; i++) {
                    final int key = keySpaceOffset + rnd.nextInt(50);
                    if (rnd.nextDouble() < 0.6) {
                        final int value = rnd.nextInt(10_000);
                        expectedLocal.put(key, value);
                        index.putAsync(key, value).toCompletableFuture()
                                .orTimeout(30, TimeUnit.SECONDS).join();
                    } else {
                        expectedLocal.remove(key);
                        index.deleteAsync(key).toCompletableFuture()
                                .orTimeout(30, TimeUnit.SECONDS).join();
                    }
                }

                expectedByThread[threadId] = expectedLocal;
                doneGate.countDown();
                return null;
            });
        }

        startGate.countDown();
        assertTrue(doneGate.await(60, TimeUnit.SECONDS),
                "Worker threads did not finish in time");
        executor.shutdownNow();

        index.flush();

        final Map<Integer, Integer> expected = new java.util.HashMap<>();
        for (final Map<Integer, Integer> local : expectedByThread) {
            expected.putAll(local);
        }

        // Validate point lookups first so we can tell whether missing values in
        // the stream are due to persistence/iteration or due to lost writes.
        for (final Map.Entry<Integer, Integer> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), index.get(entry.getKey()),
                    "Point lookup differs for key " + entry.getKey());
        }

        // Make sure buffered entries are flushed before collecting the stream
        // (getStream does not emit brand-new keys that exist only in the write
        // cache).
        index.flush();

        final Map<Integer, Integer> actual = index.getStream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (first, second) -> second));
        assertEquals(expected, actual);

        index.close();

        final SegmentIndex<Integer, Integer> reopened = SegmentIndex
                .open(directory, conf);
        final Map<Integer, Integer> reloaded = reopened.getStream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (first, second) -> second));
        assertEquals(expected, reloaded);
        reopened.close();
    }

    private static int encodeValue(final int key, final int sequence) {
        return key * 1_000_000 + sequence;
    }

    private static int decodeKey(final int value) {
        return value / 1_000_000;
    }

    private static IndexConfiguration<Integer, Integer> newThreadSafeConfiguration(
            final String name, final int cpuThreads,
            final int maxNumberOfKeysInCache) {
        return IndexConfiguration.<Integer, Integer>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorInteger())//
                .withName(name)//
                .withThreadSafe(true)//
                .withMaxNumberOfKeysInSegment(20)// small to trigger splits
                .withMaxNumberOfKeysInSegmentCache(30)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(40)//
                .withMaxNumberOfKeysInSegmentChunk(5)//
                .withMaxNumberOfKeysInCache(maxNumberOfKeysInCache)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withNumberOfCpuThreads(cpuThreads)//
                .build();
    }
}
