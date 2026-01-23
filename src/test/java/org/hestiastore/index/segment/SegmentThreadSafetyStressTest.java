package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAwait;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SegmentThreadSafetyStressTest {

    private static final int ITERATIONS = 250;

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void mixed_workload_does_not_deadlock_or_error() throws Exception {
        try (ClosingExecutor maintenanceExecutor = new ClosingExecutor(1);
                ClosingExecutor workerExecutor = new ClosingExecutor(3)) {
            final Segment<Integer, String> segment = newSegment(
                    maintenanceExecutor.get());
            try {
            final CountDownLatch start = new CountDownLatch(1);
            final Future<?> writer = workerExecutor.get().submit(() -> {
                awaitStart(start);
                for (int i = 0; i < ITERATIONS; i++) {
                    final int key = i % 64;
                    SegmentResult<Void> result = segment.put(key, "v-" + i);
                    while (result.getStatus() == SegmentResultStatus.BUSY) {
                        Thread.onSpinWait();
                        result = segment.put(key, "v-" + i);
                    }
                    if (result.getStatus() != SegmentResultStatus.OK) {
                        throw new IllegalStateException(
                                "Put returned " + result.getStatus());
                    }
                }
                return null;
            });
            final Future<?> reader = workerExecutor.get().submit(() -> {
                awaitStart(start);
                for (int i = 0; i < ITERATIONS; i++) {
                    final int key = i % 64;
                    final SegmentResult<String> result = segment.get(key);
                    if (result.getStatus() == SegmentResultStatus.BUSY) {
                        Thread.onSpinWait();
                        continue;
                    }
                    if (result.getStatus() != SegmentResultStatus.OK) {
                        throw new IllegalStateException(
                                "Get returned " + result.getStatus());
                    }
                }
                return null;
            });
            final Future<?> maintenance = workerExecutor.get().submit(() -> {
                awaitStart(start);
                for (int i = 0; i < ITERATIONS / 10; i++) {
                    awaitCompletion(segment.flush(), "flush");
                    awaitCompletion(segment.compact(), "compact");
                }
                return null;
            });

            start.countDown();
            writer.get(5, TimeUnit.SECONDS);
            reader.get(5, TimeUnit.SECONDS);
            maintenance.get(5, TimeUnit.SECONDS);

            assertNotEquals(SegmentState.ERROR, segment.getState());
            } finally {
                closeAndAwait(segment);
            }
        }
    }

    private static void awaitStart(final CountDownLatch start) {
        try {
            start.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted before starting workload", e);
        }
    }

    private static void awaitCompletion(
            final SegmentResult<CompletionStage<Void>> result,
            final String operation) {
        if (result.getStatus() == SegmentResultStatus.BUSY) {
            return;
        }
        if (result.getStatus() != SegmentResultStatus.OK) {
            throw new IllegalStateException(
                    operation + " returned " + result.getStatus());
        }
        final CompletionStage<Void> stage = result.getValue();
        if (stage == null) {
            return;
        }
        try {
            stage.toCompletableFuture().get(2, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    operation + " interrupted while waiting", e);
        } catch (final ExecutionException | TimeoutException e) {
            throw new IllegalStateException(
                    operation + " failed to complete", e);
        }
    }

    private static Segment<Integer, String> newSegment(
            final Executor maintenanceExecutor) {
        return Segment.<Integer, String>builder(
                AsyncDirectoryAdapter.wrap(new MemDirectory()))
                .withId(SegmentId.of(1))
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withMaxNumberOfKeysInSegmentWriteCache(64)
                .withMaxNumberOfKeysInSegmentCache(128)
                .withMaxNumberOfKeysInSegmentChunk(4)
                .withBloomFilterIndexSizeInBytes(0)
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .withMaintenanceExecutor(maintenanceExecutor)
                .build();
    }

    private static final class ClosingExecutor implements AutoCloseable {
        private final ExecutorService executor;

        private ClosingExecutor(final int threads) {
            this.executor = Executors.newFixedThreadPool(threads);
        }

        private ExecutorService get() {
            return executor;
        }

        @Override
        public void close() {
            executor.shutdownNow();
            try {
                executor.awaitTermination(1, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
