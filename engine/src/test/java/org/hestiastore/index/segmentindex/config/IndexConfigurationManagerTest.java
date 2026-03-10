package org.hestiastore.index.segmentindex.config;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.junit.jupiter.api.Test;

class IndexConfigurationManagerTest {

    @Test
    void mergeWithStored_noOverridesDoesNotSave() {
        final IndexConfiguration<Integer, String> stored = buildStored();
        final TestStorage<Integer, String> storage = new TestStorage<>(stored);
        final IndexConfigurationManager<Integer, String> manager = new IndexConfigurationManager<>(
                storage);
        final IndexConfiguration<Integer, String> runtime = IndexConfiguration
                .<Integer, String>builder()
                .build();

        manager.mergeWithStored(runtime);

        assertNull(storage.getSaved());
    }

    @Test
    void applyDefaultsDerivesPartitionLimitsFromPartitionNames() {
        final TestStorage<Integer, String> storage = new TestStorage<>(null);
        final IndexConfigurationManager<Integer, String> manager = new IndexConfigurationManager<>(
                storage);
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("test-index")
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(4)
                .build();

        final IndexConfiguration<Integer, String> applied = manager
                .applyDefaults(conf);

        assertEquals(4, applied.getMaxNumberOfKeysInActivePartition());
        assertEquals(6, applied.getMaxNumberOfKeysInPartitionBuffer());
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
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfKeysInPartitionBuffer(6)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfDeltaCacheFiles(
                        IndexConfigurationContract.MAX_NUMBER_OF_DELTA_CACHE_FILES)
                .withMaxNumberOfKeysInPartitionBeforeSplit(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withIndexWorkerThreadCount(1)
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
            super(new MemDirectory());
            this.stored = stored;
        }

        @Override
        public IndexConfiguration<K, V> load() {
            return stored;
        }

        @Override
        public void save(final IndexConfiguration<K, V> indexConfiguration) {
            saved = indexConfiguration;
        }

        @Override
        public boolean exists() {
            return stored != null;
        }

        private IndexConfiguration<K, V> getSaved() {
            return saved;
        }
    }
}
