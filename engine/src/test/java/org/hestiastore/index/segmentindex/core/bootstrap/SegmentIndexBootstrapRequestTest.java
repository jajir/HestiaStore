package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolverImpl;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.junit.jupiter.api.Test;

class SegmentIndexBootstrapRequestTest {

    @Test
    void constructorStoresImmutableInputs() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = buildConf();
        final ChunkFilterProviderResolver resolver =
                ChunkFilterProviderResolverImpl.defaultResolver();
        final SegmentIndexBootstrapRequest<Integer, String> request =
                new SegmentIndexBootstrapRequest<>(directory, conf, resolver,
                        SegmentIndexBootstrapMode.CREATE);

        assertSame(directory, request.getDirectory());
        assertSame(conf, request.getUserProvidedConfiguration());
        assertEquals(resolver,
                request.getChunkFilterProviderResolver().orElseThrow());
        assertEquals(SegmentIndexBootstrapMode.CREATE, request.getMode());
    }

    @Test
    void constructorAllowsAbsentProviderResolver() {
        final SegmentIndexBootstrapRequest<Integer, String> request =
                new SegmentIndexBootstrapRequest<>(new MemDirectory(),
                        buildConf(), null, SegmentIndexBootstrapMode.OPEN);

        assertTrue(request.getChunkFilterProviderResolver().isEmpty());
    }

    @Test
    void constructorRejectsNullRequiredInputs() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        final MemDirectory directory = new MemDirectory();

        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexBootstrapRequest<Integer, String>(null,
                        conf, null, SegmentIndexBootstrapMode.CREATE));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexBootstrapRequest<>(directory, null, null,
                        SegmentIndexBootstrapMode.CREATE));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexBootstrapRequest<>(directory, conf, null,
                        null));
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(
                        new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(
                        new TypeDescriptorShortString()))
                .identity(identity -> identity.name(
                        "segment-index-bootstrap-request-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
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
}
