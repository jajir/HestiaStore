package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class IntegrationSegmentIndexMetricsSnapshotConcurrencyTest {

    @Test
    void metricsSnapshotCountsConcurrentGetsConsistently() throws Exception {
        final int threads = 8;
        final int callsPerThread = 500;
        final int expectedGetCount = threads * callsPerThread;

        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(keyDescriptor) //
                .withValueTypeDescriptor(valueDescriptor) //
                .withBloomFilterIndexSizeInBytes(0) //
                .withContextLoggingEnabled(false) //
                .withName("metrics_concurrency_test_index") //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            final ExecutorService executor = Executors.newFixedThreadPool(threads);
            try {
                final List<Callable<Void>> tasks = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    tasks.add(() -> {
                        for (int i = 0; i < callsPerThread; i++) {
                            assertNull(index.get(i));
                        }
                        return null;
                    });
                }
                final List<Future<Void>> futures = executor.invokeAll(tasks);
                for (final Future<Void> future : futures) {
                    future.get();
                }
            } finally {
                executor.shutdownNow();
            }

            final SegmentIndexMetricsSnapshot snapshot = index
                    .metricsSnapshot();
            assertEquals(expectedGetCount, snapshot.getGetOperationCount());
            assertEquals(0L, snapshot.getPutOperationCount());
            assertEquals(0L, snapshot.getDeleteOperationCount());
            assertEquals(SegmentIndexState.READY, snapshot.getState());
        }
    }
}
