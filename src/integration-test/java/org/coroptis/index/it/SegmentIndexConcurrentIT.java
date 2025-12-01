package org.coroptis.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Comparator;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
 * thread-safe configuration. Threads perform random puts, deletes and gets; the
 * resulting on-disk state is compared with a deterministic replay of the
 * completed mutations.
 */
class SegmentIndexConcurrentIT {

    private enum OpType {
        PUT, DELETE
    }

    private static final class Op {
        final long seq;
        final OpType type;
        final int key;
        final Integer value; // null for delete

        Op(final long seq, final OpType type, final int key,
                final Integer value) {
            this.seq = seq;
            this.type = type;
            this.key = key;
            this.value = value;
        }
    }

    @Test
    void concurrent_put_delete_produces_consistent_state() throws Exception {
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
                .build();

        final SegmentIndex<Integer, Integer> index = SegmentIndex
                .create(directory, conf);

        final int threads = 4;
        final int operationsPerThread = 200;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch doneGate = new CountDownLatch(threads);
        final Queue<Op> operations = new ConcurrentLinkedQueue<>();
        final AtomicLong seq = new AtomicLong(0);

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                final Random rnd = new Random(12345L + threadId);
                startGate.await();
                for (int i = 0; i < operationsPerThread; i++) {
                    final int key = rnd.nextInt(50);
                    final double p = rnd.nextDouble();
                    if (p < 0.6) {
                        final int value = rnd.nextInt(10_000);
                        index.put(key, value);
                        operations.add(new Op(seq.incrementAndGet(), OpType.PUT,
                                key, value));
                    } else if (p < 0.9) {
                        index.delete(key);
                        operations.add(new Op(seq.incrementAndGet(),
                                OpType.DELETE, key, null));
                    } else {
                        index.get(key); // exercise reads
                    }
                }
                doneGate.countDown();
                return null;
            });
        }

        startGate.countDown();
        doneGate.await(30, TimeUnit.SECONDS);
        executor.shutdownNow();

        index.flush();

        final Map<Integer, Integer> expected = new java.util.HashMap<>();
        operations.stream().sorted(Comparator.comparingLong(op -> op.seq))
                .forEach(op -> {
                    if (op.type == OpType.PUT) {
                        expected.put(op.key, op.value);
                    } else {
                        expected.remove(op.key);
                    }
                });

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
}
