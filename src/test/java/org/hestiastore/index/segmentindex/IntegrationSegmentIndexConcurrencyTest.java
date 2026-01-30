package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class IntegrationSegmentIndexConcurrencyTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    private static final int OPERATION_TIMEOUT_SECONDS = 30;

    @ParameterizedTest(name = "{0}")
    @MethodSource("parallelPutsScenarios")
    void parallel_puts_and_gets_on_different_keys(
            final ParallelPutsScenario scenario) throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentIndex<Integer, String> index = newIndex(directory,
                scenario);
        final ExecutorService executor = Executors
                .newFixedThreadPool(scenario.writerThreads());
        try {
            runParallelPuts(index, executor, scenario.keyCount());

            index.flushAndWait();

            final List<Entry<Integer, String>> expected = expectedEntries(
                    scenario.keyCount());
            verifySegmentIndexDataWithDiagnostics(index, expected);
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

    private SegmentIndex<Integer, String> newIndex(final Directory directory,
            final ParallelPutsScenario scenario) {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi) //
                .withValueTypeDescriptor(tds) //
                .withMaxNumberOfKeysInSegmentCache(
                        scenario.maxNumberOfKeysInSegmentCache()) //
                .withMaxNumberOfKeysInSegmentWriteCache(
                        scenario.maxNumberOfKeysInSegmentWriteCache()) //
                .withMaxNumberOfKeysInSegmentChunk(
                        scenario.maxNumberOfKeysInSegmentChunk()) //
                .withMaxNumberOfKeysInCache(scenario.maxNumberOfKeysInCache()) //
                .withMaxNumberOfKeysInSegment(
                        scenario.maxNumberOfKeysInSegment()) //
                .withMaxNumberOfSegmentsInCache(
                        scenario.maxNumberOfSegmentsInCache()) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(3) //
                .withSegmentMaintenanceAutoEnabled(true) //
                .withNumberOfCpuThreads(scenario.cpuThreads())//
                .withNumberOfIoThreads(scenario.ioThreads())//
                .withName("concurrency_index_" + scenario.name()) //
                .build();
        return SegmentIndex.create(directory, conf);
    }

    private static void runParallelPuts(
            final SegmentIndex<Integer, String> index,
            final ExecutorService executor, final int keyCount)
            throws Exception {
        final List<CompletableFuture<Void>> writes = new ArrayList<>();
        final CountDownLatch start = new CountDownLatch(1);
        IntStream.range(0, keyCount)
                .forEach(i -> writes.add(CompletableFuture.runAsync(() -> {
                    try {
                        start.await();
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Writer interrupted",
                                e);
                    }
                    index.put(i, "v-" + i);
                }, executor)));
        start.countDown();
        CompletableFuture.allOf(writes.toArray(new CompletableFuture[0]))
                .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static List<Entry<Integer, String>> expectedEntries(
            final int keyCount) {
        return IntStream.range(0, keyCount).mapToObj(i -> Entry.of(i, "v-" + i))
                .toList();
    }

    private static void verifySegmentIndexDataWithDiagnostics(
            final SegmentIndex<Integer, String> index,
            final List<Entry<Integer, String>> expected) {
        final List<Entry<Integer, String>> actual;
        try (var stream = index.getStream(SegmentWindow.unbounded(),
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            actual = stream.toList();
        }
        if (actual.size() != expected.size()) {
            final var actualKeys = actual.stream().map(Entry::getKey).toList();
            final var missingKeys = expected.stream().map(Entry::getKey)
                    .filter(key -> !actualKeys.contains(key)).toList();
            final var extraKeys = actualKeys.stream().filter(key -> expected
                    .stream().noneMatch(e -> e.getKey().equals(key))).toList();
            throw new AssertionError(String.format(
                    "Unexpected number of entries in index, expected %d but was %d. Missing keys: %s. Extra keys: %s",
                    expected.size(), actual.size(), missingKeys, extraKeys));
        }
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }

    private static Stream<ParallelPutsScenario> parallelPutsScenarios() {
        return Stream.of(
                new ParallelPutsScenario("small-cache-50-keys", 50, 3, 3, 2, 10,
                        4, 5, 4, 1, 40),
                new ParallelPutsScenario("write-cache-64-200-keys", 200, 128,
                        64, 16, 250, 128, 10, 8, 2, 64),
                new ParallelPutsScenario("write-cache-128-small-segment", 300,
                        128, 128, 16, 350, 32, 5, 8, 2, 64),
                new ParallelPutsScenario("large-segment-512", 300, 256, 64, 32,
                        400, 512, 10, 8, 2, 40));
    }

    private record ParallelPutsScenario(String name, int keyCount,
            int maxNumberOfKeysInSegmentCache,
            int maxNumberOfKeysInSegmentWriteCache,
            int maxNumberOfKeysInSegmentChunk, int maxNumberOfKeysInCache,
            int maxNumberOfKeysInSegment, int maxNumberOfSegmentsInCache,
            int cpuThreads, int ioThreads, int writerThreads) {
        @Override
        public String toString() {
            return name;
        }
    }
}
