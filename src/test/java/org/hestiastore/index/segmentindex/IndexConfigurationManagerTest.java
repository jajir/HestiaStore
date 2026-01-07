package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;

class IndexConfigurationManagerTest {

    @Test
    void mergeWithStored_preservesMaintenanceExecutorWithoutSaving() {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final IndexConfiguration<Integer, String> stored = buildStored();
            final TestStorage<Integer, String> storage = new TestStorage<>(
                    stored);
            final IndexConfigurationManager<Integer, String> manager = new IndexConfigurationManager<>(
                    storage);
            final IndexConfiguration<Integer, String> runtime = IndexConfiguration
                    .<Integer, String>builder()
                    .withMaintenanceExecutor(executor)
                    .build();

            final IndexConfiguration<Integer, String> merged = manager
                    .mergeWithStored(runtime);

            assertSame(executor, merged.getMaintenanceExecutor());
            assertNull(storage.getSaved());
        } finally {
            executor.shutdownNow();
        }
    }

    private IndexConfiguration<Integer, String> buildStored() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("test-index")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInSegmentWriteCache(5)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringFlush(6)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInCache(10)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withNumberOfCpuThreads(1)
                .withNumberOfIoThreads(1)
                .withEncodingFilters(
                        List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(
                        List.of(new ChunkFilterDoNothing()))
                .build();
    }

    private static final class TestStorage<K, V>
            extends IndexConfiguratonStorage<K, V> {

        private IndexConfiguration<K, V> stored;
        private IndexConfiguration<K, V> saved;

        private TestStorage(final IndexConfiguration<K, V> stored) {
            super(AsyncDirectoryAdapter.wrap(new MemDirectory()));
            this.stored = stored;
        }

        @Override
        IndexConfiguration<K, V> load() {
            return stored;
        }

        @Override
        public void save(final IndexConfiguration<K, V> indexConfiguration) {
            saved = indexConfiguration;
        }

        @Override
        boolean exists() {
            return stored != null;
        }

        private IndexConfiguration<K, V> getSaved() {
            return saved;
        }
    }
}
