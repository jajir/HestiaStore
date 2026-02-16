package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentregistry.SegmentRegistryCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SegmentRegistryConcurrencyStressTest {

    private static final int WAIT_SECONDS = 10;

    @Test
    @Timeout(20)
    void sameKeySingleFlight_underHighContention() throws Exception {
        final AtomicInteger loaderCalls = new AtomicInteger();
        final CountDownLatch loaderStarted = new CountDownLatch(1);
        final CountDownLatch allowLoadFinish = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = new SegmentRegistryCache<>(
                32, key -> {
                    loaderCalls.incrementAndGet();
                    loaderStarted.countDown();
                    awaitLatch(allowLoadFinish);
                    return "v-" + key;
                }, value -> {
                });

        final int threadCount = 24;
        final ExecutorService executor = Executors.newFixedThreadPool(
                threadCount);
        try {
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                futures.add(executor.submit(() -> {
                    awaitLatch(start);
                    return cache.get(7);
                }));
            }

            start.countDown();
            assertTrue(loaderStarted.await(WAIT_SECONDS, TimeUnit.SECONDS));
            allowLoadFinish.countDown();

            for (final Future<String> future : futures) {
                assertEquals("v-7", future.get(WAIT_SECONDS,
                        TimeUnit.SECONDS));
            }
            assertEquals(1, loaderCalls.get(),
                    "Same-key contention must execute loader exactly once.");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(20)
    void differentKeys_doNotBlockEachOther() throws Exception {
        final CountDownLatch keyOneLoaderStarted = new CountDownLatch(1);
        final CountDownLatch allowKeyOneFinish = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = new SegmentRegistryCache<>(
                32, key -> {
                    if (key.intValue() == 1) {
                        keyOneLoaderStarted.countDown();
                        awaitLatch(allowKeyOneFinish);
                    }
                    return "v-" + key;
                }, value -> {
                });

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<String> blockedKey = executor
                    .submit(() -> cache.get(1));
            assertTrue(keyOneLoaderStarted.await(WAIT_SECONDS,
                    TimeUnit.SECONDS));

            final long startNanos = System.nanoTime();
            final String differentKeyValue = cache.get(2);
            final long elapsedMillis = TimeUnit.NANOSECONDS
                    .toMillis(System.nanoTime() - startNanos);

            assertEquals("v-2", differentKeyValue);
            assertTrue(elapsedMillis < 250,
                    "Different key must not wait for another key load.");

            allowKeyOneFinish.countDown();
            assertEquals("v-1",
                    blockedKey.get(WAIT_SECONDS, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(30)
    void evictionGetsAndSplitInteraction_preservesWrittenKeys()
            throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentIndex<Integer, String> index = newIndex(directory);
        final int keyCount = 160;
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicBoolean writersDone = new AtomicBoolean(false);
        final CountDownLatch start = new CountDownLatch(1);
        try {
            final List<Future<?>> readers = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                final int seed = i + 1;
                readers.add(executor.submit(() -> {
                    final SplittableRandom random = new SplittableRandom(seed);
                    awaitLatch(start);
                    while (!writersDone.get()) {
                        final int key = random.nextInt(keyCount);
                        try {
                            index.get(key);
                        } catch (final RuntimeException ex) {
                            failure.compareAndSet(null, ex);
                            return;
                        }
                    }
                }));
            }

            final List<Future<?>> writers = new ArrayList<>();
            for (int key = 0; key < keyCount; key++) {
                final int currentKey = key;
                writers.add(executor.submit(() -> {
                    awaitLatch(start);
                    index.put(currentKey, "v-" + currentKey);
                }));
            }

            start.countDown();
            for (final Future<?> writer : writers) {
                writer.get(WAIT_SECONDS, TimeUnit.SECONDS);
            }
            writersDone.set(true);
            for (final Future<?> reader : readers) {
                reader.get(WAIT_SECONDS, TimeUnit.SECONDS);
            }

            assertNull(failure.get(),
                    "Concurrent gets should not fail during split/eviction load.");

            index.flushAndWait();
            for (int key = 0; key < keyCount; key++) {
                assertEquals("v-" + key, index.get(key));
            }
        } finally {
            executor.shutdownNow();
            index.close();
        }
    }

    private SegmentIndex<Integer, String> newIndex(final Directory directory) {
        final TypeDescriptorInteger keyType = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueType = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(keyType)//
                .withValueTypeDescriptor(valueType)//
                .withMaxNumberOfKeysInSegmentCache(4)//
                .withMaxNumberOfKeysInSegmentWriteCache(4)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInCache(500)//
                .withMaxNumberOfKeysInSegment(8)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterIndexSizeInBytes(1000)//
                .withBloomFilterNumberOfHashFunctions(3)//
                .withSegmentMaintenanceAutoEnabled(true)//
                .withIndexWorkerThreadCount(8)//
                .withNumberOfIoThreads(2)//
                .withName("registry_concurrency_stress")//
                .build();
        return SegmentIndex.create(directory, conf);
    }

    private static void awaitLatch(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting", e);
        }
    }
}
