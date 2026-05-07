package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link SegmentIndexImpl} rejects operations after the index is
 * closed, exercising the {@link IndexState} lifecycle transitions.
 */
class IndexStateTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    private IndexInternalConcurrent<Integer, String> index;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        index = IndexInternalConcurrent.createStarted(
                new MemDirectory(),
                tdi, tds, conf, conf.resolveRuntimeConfiguration(),
                ExecutorRegistryFixture.from(conf));
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    /**
     * Ensures that all public operations fail with an
     * {@link IllegalStateException} once {@link SegmentIndexImpl#close()} has
     * completed.
     */
    @Test
    void operationsFailAfterClose() {
        index.put(1, "one");
        index.close();

        assertThrows(IllegalStateException.class, () -> index.put(2, "two"));
        assertThrows(IllegalStateException.class, () -> index.get(1));
        assertThrows(IllegalStateException.class, () -> index.delete(1));
        assertThrows(IllegalStateException.class,
                () -> index.maintenance().compact());
        assertThrows(IllegalStateException.class,
                () -> index.maintenance().checkAndRepairConsistency());
    }

    /**
     * Builds a minimal configuration for a small in-memory index used by the
     * lifecycle test.
     */
    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(tdi))//
                .identity(identity -> identity.valueTypeDescriptor(tds))//
                .identity(identity -> identity.name("index-state-test"))//
                .segment(segment -> segment.cacheKeyLimit(4))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(2))//
                .segment(segment -> segment.chunkKeyLimit(2))//
                .segment(segment -> segment.maxKeys(10))//
                .segment(segment -> segment.cachedSegmentLimit(3))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))//
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .logging(logging -> logging.contextEnabled(false))//
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))//
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))//
                .build();
    }
}
