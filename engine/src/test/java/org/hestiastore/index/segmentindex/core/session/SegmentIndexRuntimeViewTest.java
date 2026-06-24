package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentIndexRuntimeViewTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private Object openedRuntime;
    private Object runtime;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        openedRuntime = SegmentIndexRuntimeTestAccess.openRuntime(
                new MemDirectory(), tdi, tds, conf);
        runtime = SegmentIndexRuntimeTestAccess.runtime(openedRuntime);
    }

    @AfterEach
    void tearDown() {
        if (openedRuntime != null) {
            SegmentIndexRuntimeTestAccess.closeRuntime(openedRuntime);
        }
    }

    @Test
    void openBuildsAllCoreCollaborators() {
        assertNotNull(SegmentIndexRuntimeTestAccess.keyToSegmentMap(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.segmentRegistry(runtime));
        assertNotNull(
                SegmentIndexRuntimeTestAccess.runtimeTuningState(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.storageService(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.operationAccess(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.runtimeTuning(runtime));
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(tdi))
                .identity(identity -> identity.valueTypeDescriptor(tds))
                .identity(identity -> identity.name("segment-index-runtime-test"))
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
                .filters(filters -> filters.encodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .build();
    }
}
