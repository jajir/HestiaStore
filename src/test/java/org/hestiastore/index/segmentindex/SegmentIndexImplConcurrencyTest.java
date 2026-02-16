package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentIndexImplConcurrencyTest {

    private IndexInternalConcurrent<Integer, String> index;

    @BeforeEach
    void setUp() {
        index = new IndexInternalConcurrent<>(
                AsyncDirectoryAdapter.wrap(new MemDirectory()),
                new TypeDescriptorInteger(),
                new TypeDescriptorShortString(),
                buildConf());
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    @Test
    void parallelPutAndGetDoNotSerialize() throws Exception {
        final int threads = 4;
        final int keysPerThread = 25;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(threads);
            final AtomicReference<Throwable> failure = new AtomicReference<>();

            for (int t = 0; t < threads; t++) {
                final int base = t * keysPerThread;
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < keysPerThread; i++) {
                            final int key = base + i;
                            final String value = "v" + key;
                            index.put(key, value);
                            final String stored = index.get(key);
                            if (!value.equals(stored)) {
                                throw new IllegalStateException(
                                        "Unexpected value for key " + key);
                            }
                        }
                    } catch (final Throwable ex) {
                        failure.compareAndSet(null, ex);
                    } finally {
                        done.countDown();
                    }
                });
            }

            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS),
                    "Parallel writes did not finish");
            if (failure.get() != null) {
                throw new AssertionError("Parallel operations failed",
                        failure.get());
            }

            for (int key = 0; key < threads * keysPerThread; key++) {
                assertEquals("v" + key, index.get(key));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("segment-index-concurrency-test")//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(100)//
                .withMaxNumberOfKeysInSegmentWriteCache(50)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(60)//
                .withMaxNumberOfKeysInSegmentChunk(10)//
                .withMaxNumberOfKeysInCache(100)//
                .withMaxNumberOfKeysInSegment(1000)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withIndexWorkerThreadCount(1)//
                .withNumberOfIoThreads(1)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }
}
