package org.hestiastore.index.segmentindex;

import static org.hestiastore.index.AbstractDataTest.verifySegmentIndexData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.junit.jupiter.api.Test;

class IntegrationSegmentIndexConcurrencyTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    private static final int OPERATION_TIMEOUT_SECONDS = 30;

    @Test
    void parallel_puts_and_gets_on_different_keys() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentIndex<Integer, String> index = newIndex(directory, 4, 1);
        final ExecutorService executor = Executors.newFixedThreadPool(40);
        try {
            final List<CompletableFuture<Void>> writes = new ArrayList<>();
            final CountDownLatch start = new CountDownLatch(1);
            IntStream.range(0, 50)
                    .forEach(i -> writes.add(CompletableFuture.runAsync(() -> {
                        try {
                            start.await();
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(
                                    "Writer interrupted", e);
                        }
                        index.put(i, "v-" + i);
                    }, executor)));
            start.countDown();
            CompletableFuture.allOf(writes.toArray(new CompletableFuture[0]))
                    .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            index.flushAndWait();

            final List<Entry<Integer, String>> expected = IntStream.range(0, 50)
                    .mapToObj(i -> Entry.of(i, "v-" + i)).toList();
            verifySegmentIndexData(index, expected);
        } finally {
            executor.shutdownNow();
            index.close();
        }
    }

    @Test
    void concurrentReadsWithWriterOnSameKey() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentIndex<Integer, String> index = newIndex(directory, 4, 1);
        try {
            index.put(1, "init");
            final CountDownLatch start = new CountDownLatch(1);
            final var executor = Executors.newFixedThreadPool(4);
            final var readerTasks = IntStream.range(0, 3)
                    .mapToObj(i -> executor.submit(() -> {
                        start.await();
                        for (int j = 0; j < 50; j++) {
                            index.get(1);
                        }
                        return null;
                    })).toList();
            final var writerTask = executor.submit(() -> {
                start.await();
                for (int j = 0; j < 10; j++) {
                    index.put(1, "v-" + j);
                }
                return null;
            });

            start.countDown();
            for (final var f : readerTasks) {
                f.get(5, TimeUnit.SECONDS);
            }
            writerTask.get(5, TimeUnit.SECONDS);
            executor.shutdownNow();

            index.flush();
            assertEquals("v-9", index.get(1));
        } finally {
            index.close();
        }
    }

    @Test
    void interleavedFlushCompactWithReadsAndWrites() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentIndex<Integer, String> index = newIndex(directory, 4, 1);
        try {
            final CountDownLatch start = new CountDownLatch(1);
            final var executor = Executors.newFixedThreadPool(4);

            final var writer = executor.submit(() -> {
                start.await();
                for (int i = 0; i < 40; i++) {
                    index.put(i, "v-" + i);
                }
                return null;
            });

            final var reader = executor.submit(() -> {
                start.await();
                for (int i = 0; i < 40; i++) {
                    index.get(i);
                }
                return null;
            });

            final var maint = executor.submit(() -> {
                start.await();
                for (int i = 0; i < 5; i++) {
                    index.flushAndWait();
                    index.compactAndWait();
                }
                return null;
            });

            start.countDown();
            writer.get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            reader.get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            maint.get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            executor.shutdownNow();

            index.flushAndWait();
            index.compactAndWait();
            final long count = index.getStream(SegmentWindow.unbounded(),
                    SegmentIteratorIsolation.FULL_ISOLATION).count();
            assertTrue(count >= 40, "Expected at least 40 entries");
        } finally {
            index.close();
        }
    }

    private SegmentIndex<Integer, String> newIndex(final Directory directory,
            final int cpuThreads, final int ioThreads) {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi) //
                .withValueTypeDescriptor(tds) //
                .withMaxNumberOfKeysInSegmentCache(3) //
                .withMaxNumberOfKeysInSegment(4) //
                .withMaxNumberOfKeysInSegmentChunk(2) //
                .withMaxNumberOfKeysInCache(10) //
                .withMaxNumberOfSegmentsInCache(5) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(3) //
                .withSegmentMaintenanceAutoEnabled(true) //
                .withNumberOfCpuThreads(cpuThreads)//
                .withNumberOfIoThreads(ioThreads)//
                .withName("concurrency_index") //
                .build();
        return SegmentIndex.create(directory, conf);
    }
}
