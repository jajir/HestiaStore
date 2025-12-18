package org.coroptis.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
        final IndexConfiguration<Integer, Integer> conf = IndexConfiguration
                .<Integer, Integer>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorInteger())//
                .withName("concurrent-index")//
                .withThreadSafe(true)//
                .withMaxNumberOfKeysInSegment(20)// small to trigger splits
                .withMaxNumberOfKeysInSegmentCache(30)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(40)//
                .withMaxNumberOfKeysInSegmentChunk(5)//
                .withMaxNumberOfKeysInCache(15)// force periodic flushes
                .withBloomFilterIndexSizeInBytes(1024)// keep bloom filter
                                                      // enabled with tiny
                                                      // footprint
                .withBloomFilterNumberOfHashFunctions(1)// minimal hashing
                .withNumberOfCpuThreads(4)//
                .build();

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
        doneGate.await(30, TimeUnit.SECONDS);
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
        final IndexConfiguration<Integer, Integer> conf = IndexConfiguration
                .<Integer, Integer>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorInteger())//
                .withName("concurrent-index")//
                .withThreadSafe(true)//
                .withMaxNumberOfKeysInSegment(20)//
                .withMaxNumberOfKeysInSegmentCache(30)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(40)//
                .withMaxNumberOfKeysInSegmentChunk(5)//
                .withMaxNumberOfKeysInCache(15)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withNumberOfCpuThreads(4)//
                .build();

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
        doneGate.await(30, TimeUnit.SECONDS);
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
}
