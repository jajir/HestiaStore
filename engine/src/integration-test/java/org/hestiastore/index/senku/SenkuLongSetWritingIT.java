package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SenkuLongSetWritingIT {

    @Test
    void parallelPrimitiveBatchesAndScalarPutsProduceTheSameExactSet()
            throws Exception {
        final MemDirectory directory = new MemDirectory();
        final SenkuLongSetWriting writing = batchBuilder(directory)
                .createLongSet(Long::hashCode);
        final ExecutorService workers = Executors.newFixedThreadPool(4);
        final CountDownLatch start = new CountDownLatch(1);
        try {
            final List<? extends Future<?>> tasks = IntStream.range(0, 4)
                    .mapToObj(worker -> workers.submit(() -> {
                        await(start);
                        final long[] keys = LongStream.range(-20, 9000)
                                .map(key -> key % 500).toArray();
                        writing.putLongs(keys, 3, keys.length - 6);
                        writing.putLong(Long.MIN_VALUE);
                        writing.putLong(Long.MAX_VALUE);
                    })).toList();
            start.countDown();
            for (final Future<?> task : tasks)
                task.get(30, TimeUnit.SECONDS);
        } finally {
            start.countDown();
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
        try (SenkuReady<Long, NullValue> ready = writing.finishWriting();
                Stream<Entry<Long, NullValue>> entries = ready.openStream()) {
            final List<Long> expected = LongStream
                    .concat(LongStream.of(Long.MIN_VALUE),
                            LongStream.concat(LongStream.range(-17, 500),
                                    LongStream.of(Long.MAX_VALUE)))
                    .boxed().toList();
            assertEquals(expected, entries.map(Entry::getKey).toList());
        }
        assertFalse(directory.isFileExists(".lock"));
    }

    @Test
    void finishingDuringSecondWindowHashingKeepsOnlyPreviouslyAcceptedKeys()
            throws Exception {
        final MemDirectory directory = new MemDirectory();
        final CountDownLatch secondWindowHash = new CountDownLatch(1);
        final CountDownLatch releaseHash = new CountDownLatch(1);
        final SenkuLongSetWriting writing = batchBuilder(directory)
                .createLongSet(key -> {
                    if (key == 2048) {
                        secondWindowHash.countDown();
                        await(releaseHash);
                    }
                    return Long.hashCode(key);
                });
        final ExecutorService caller = Executors.newSingleThreadExecutor();
        final long[] input = LongStream.range(0, 4096).toArray();
        final Future<?> batch = caller
                .submit(() -> writing.putLongs(input, 0, input.length));
        try {
            assertTrue(secondWindowHash.await(10, TimeUnit.SECONDS));
            try (SenkuReady<Long, NullValue> ready = writing.finishWriting();
                    Stream<Entry<Long, NullValue>> entries = ready
                            .openStream()) {
                releaseHash.countDown();
                final ExecutionException failure = assertThrows(
                        ExecutionException.class,
                        () -> batch.get(10, TimeUnit.SECONDS));
                assertInstanceOf(IndexException.class, failure.getCause());
                assertEquals(LongStream.range(0, 2048).boxed().toList(),
                        entries.map(Entry::getKey).toList());
            }
            try (SenkuReady<Long, NullValue> reopened = SenkuIndex.open(
                    directory, new TypeDescriptorLong(),
                    new TypeDescriptorNull(), 8192)) {
                assertEquals(2048, reopened.recordCount());
            }
        } finally {
            releaseHash.countDown();
            caller.shutdownNow();
            assertTrue(caller.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertFalse(directory.isFileExists(".lock"));
    }

    private static SenkuIndexBuilder<Long, NullValue> batchBuilder(
            final MemDirectory directory) {
        final SenkuMergeFunctionRegistry<Long, NullValue> functions = new SenkuMergeFunctionRegistry<>();
        functions.register(SenkuMergeFunctions.longSet());
        return SenkuIndex
                .builder(directory, new TypeDescriptorLong(),
                        new TypeDescriptorNull(), functions)
                .shardCount(3).maxInMemoryEntries(512).maxKeysPerPage(128)
                .maxEntriesPerPart(10_000).mergeFanIn(3).maintenanceThreads(2)
                .maintenanceQueueSize(8).diskIoBufferSize(8192);
    }

    private static void await(final CountDownLatch latch) {
        try {
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IndexException("Interrupted controlled batch test", e);
        }
    }

    @Test
    void parallelPrimitivePutsUseTheSameExactLifecycleAndRotation()
            throws Exception {
        final MemDirectory directory = new MemDirectory();
        final SenkuMergeFunctionRegistry<Long, NullValue> functions = new SenkuMergeFunctionRegistry<>();
        functions.register(SenkuMergeFunctions.longSet());
        final SenkuLongSetWriting writing = SenkuIndex
                .builder(directory, new TypeDescriptorLong(),
                        new TypeDescriptorNull(), functions)
                .shardCount(2).maxInMemoryEntries(128).maxKeysPerPage(32)
                .maxEntriesPerPart(1000).mergeFanIn(3).maintenanceThreads(2)
                .maintenanceQueueSize(8).diskIoBufferSize(8192)
                .createLongSet(Long::hashCode);
        final ExecutorService workers = Executors.newFixedThreadPool(4);
        final CountDownLatch start = new CountDownLatch(1);
        try {
            final List<? extends Future<?>> tasks = IntStream.range(0, 4)
                    .mapToObj(worker -> workers.submit(() -> {
                        try {
                            assertTrue(start.await(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw e;
                        }
                        for (long key = -100; key < 200; key++) {
                            writing.putLong(key);
                        }
                        return null;
                    })).toList();
            start.countDown();
            for (final Future<?> task : tasks) {
                task.get(20, TimeUnit.SECONDS);
            }
        } finally {
            start.countDown();
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
        try (SenkuReady<Long, NullValue> ready = writing.finishWriting();
                Stream<Entry<Long, NullValue>> entries = ready.openStream()) {
            assertEquals(LongStream.range(-100, 200).boxed().toList(),
                    entries.map(Entry::getKey).toList());
        }
    }
}
