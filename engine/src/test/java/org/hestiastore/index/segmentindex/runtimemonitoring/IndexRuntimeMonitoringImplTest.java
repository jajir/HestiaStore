package org.hestiastore.index.segmentindex.runtimemonitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.Test;

class IndexRuntimeMonitoringImplTest {

    @Test
    void snapshot_exposesSuppliedStateAndMetrics() {
        final SegmentIndexMetricsSnapshot metricsSnapshot = mock(
                SegmentIndexMetricsSnapshot.class);
        final IndexRuntimeMonitoringImpl runtimeMonitoring = new IndexRuntimeMonitoringImpl(buildConf(),
                () -> SegmentIndexState.READY,
                () -> metricsSnapshot);

        final IndexRuntimeSnapshot snapshot = runtimeMonitoring.snapshot();

        assertEquals("runtime-snapshot-view-test", snapshot.getIndexName());
        assertEquals(SegmentIndexState.READY, snapshot.getState());
        assertSame(metricsSnapshot, snapshot.getMetrics());
        assertNotNull(snapshot.getCapturedAt());
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("runtime-snapshot-view-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(7))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters
                        .encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters
                        .decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }
}
