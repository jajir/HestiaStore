package org.hestiastore.index.segmentindex.configuration.persistence;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.junit.jupiter.api.Test;

class IndexConfigurationManagerTest {

    @Test
    void mergeWithStored_noOverridesDoesNotSave() {
        final EffectiveIndexConfiguration<Integer, String> stored = effective(
                buildStored());
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
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("test-index"))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(4))
                .build();

        final EffectiveIndexConfiguration<Integer, String> applied = manager
                .applyDefaults(conf);

        assertEquals(4, applied.writePath().segmentWriteCacheKeyLimit());
        assertEquals(6, applied.writePath()
                .segmentWriteCacheKeyLimitDuringMaintenance());
    }

    private IndexConfiguration<Integer, String> buildStored() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("test-index"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.deltaCacheFileLimit(
                        IndexConfigurationContract.DEFAULT_DELTA_CACHE_FILE_LIMIT))
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .build();
    }

    private static final class TestStorage<K, V>
            extends IndexConfigurationStorage<K, V> {

        private EffectiveIndexConfiguration<K, V> stored;
        private EffectiveIndexConfiguration<K, V> saved;

        private TestStorage(final EffectiveIndexConfiguration<K, V> stored) {
            super(new MemDirectory());
            this.stored = stored;
        }

        @Override
        public EffectiveIndexConfiguration<K, V> load() {
            return stored;
        }

        @Override
        public void save(
                final EffectiveIndexConfiguration<K, V> indexConfiguration) {
            saved = indexConfiguration;
        }

        @Override
        public boolean exists() {
            return stored != null;
        }

        private EffectiveIndexConfiguration<K, V> getSaved() {
            return saved;
        }
    }
}
