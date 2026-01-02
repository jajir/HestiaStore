package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class IntegrationSegmentIndexConcurrencyTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    @Test
    void parallelPutsAndGetsOnDifferentKeys() {
        final Directory directory = new MemDirectory();
        final SegmentIndex<Integer, String> index = newIndex(directory, 4, 1);
        try {
            final List<CompletableFuture<Void>> writes = new ArrayList<>();
            IntStream.range(0, 50).forEach(i -> writes.add(
                    CompletableFuture.runAsync(() -> index.put(i, "v-" + i))));
            CompletableFuture.allOf(writes.toArray(new CompletableFuture[0]))
                    .join();

            index.flush();

            IntStream.range(0, 50).forEach(i -> assertEquals("v-" + i,
                    index.getAsync(i).toCompletableFuture().join()));
            assertEquals(50,
                    index.getStream(SegmentWindow.unbounded()).count());
        } finally {
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
                    index.flush();
                    index.compact();
                }
                return null;
            });

            start.countDown();
            writer.get(10, TimeUnit.SECONDS);
            reader.get(10, TimeUnit.SECONDS);
            maint.get(10, TimeUnit.SECONDS);
            executor.shutdownNow();

            index.flush();
            final long count = index.getStream(SegmentWindow.unbounded())
                    .count();
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
                .withNumberOfCpuThreads(cpuThreads)//
                .withNumberOfIoThreads(ioThreads)//
                .withName("concurrency_index") //
                .build();
        return SegmentIndex.create(directory, conf);
    }
}
