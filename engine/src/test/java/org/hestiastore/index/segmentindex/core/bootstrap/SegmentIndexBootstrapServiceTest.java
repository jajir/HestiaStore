package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexResourceClosingAdapter;
import org.junit.jupiter.api.Test;

class SegmentIndexBootstrapServiceTest {

    @Test
    void createStartsBootstrapTransaction() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndex<Integer, String> index =
                new SegmentIndexBootstrapService(directory).create(
                        buildConf("bootstrap-service-create", 1));

        try {
            assertInstanceOf(SegmentIndexResourceClosingAdapter.class, index);
        } finally {
            index.close();
        }
    }

    @Test
    void openStartsBootstrapTransaction() {
        final MemDirectory directory = new MemDirectory();
        new SegmentIndexBootstrapService(directory)
                .create(buildConf("bootstrap-service-open", 1))
                .close();

        final SegmentIndex<Integer, String> index =
                new SegmentIndexBootstrapService(directory).open(
                        buildConf("bootstrap-service-open", 2));

        try {
            assertEquals(2, index.getConfiguration()
                    .maintenance().registryLifecycleThreads());
        } finally {
            index.close();
        }
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName, final int registryLifecycleThreads) {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false))
                .maintenance(maintenance -> maintenance.segmentThreads(1))
                .maintenance(maintenance -> maintenance.registryLifecycleThreads(registryLifecycleThreads))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }
}
