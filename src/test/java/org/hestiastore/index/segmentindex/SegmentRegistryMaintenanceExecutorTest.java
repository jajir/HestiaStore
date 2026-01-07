package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentRegistryMaintenanceExecutorTest {

    @Test
    void provided_executor_is_used_and_not_closed() {
        final RecordingExecutor executor = new RecordingExecutor();
        final IndexConfiguration<Integer, String> conf = buildConf(executor);
        final SegmentRegistry<Integer, String> registry = new SegmentRegistry<>(
                AsyncDirectoryAdapter.wrap(new MemDirectory()),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                conf);
        try {
            final Segment<Integer, String> segment = registry
                    .getSegment(SegmentId.of(1));
            segment.flush();

            assertTrue(executor.getExecutedCount() > 0);
            registry.close();
            assertFalse(executor.isShutdown());
        } finally {
            executor.shutdownNow();
        }
    }

    private IndexConfiguration<Integer, String> buildConf(
            final ExecutorService executor) {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("maintenance-executor-test")//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInSegmentWriteCache(5)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringFlush(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInCache(10)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withContextLoggingEnabled(false)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withMaintenanceExecutor(executor)//
                .build();
    }

    private static final class RecordingExecutor extends AbstractExecutorService {

        private final AtomicInteger executed = new AtomicInteger();
        private volatile boolean shutdown;

        @Override
        public void execute(final Runnable command) {
            executed.incrementAndGet();
            command.run();
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(final long timeout,
                final TimeUnit unit) {
            return shutdown;
        }

        int getExecutedCount() {
            return executed.get();
        }
    }
}
