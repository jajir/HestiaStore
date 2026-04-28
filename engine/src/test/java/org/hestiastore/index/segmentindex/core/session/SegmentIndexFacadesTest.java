package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.session.state.IndexState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexFacadesTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    @Mock
    private IndexState<Integer, String> indexState;

    @Mock
    private SegmentIndexDataAccess<Integer, String> dataAccess;

    private IndexConfiguration<Integer, String> conf;
    private SegmentIndexTrackedOperationRunner<Integer, String> trackedRunner;

    @BeforeEach
    void setUp() {
        conf = buildConf();
        trackedRunner = new SegmentIndexTrackedOperationRunner<>(
                () -> indexState,
                IndexOperationTrackingAccess.create());
    }

    @Test
    void assembleBuildsDataFacadeUsingTrackedRunnerAndDataAccess() {
        when(dataAccess.get(1)).thenReturn("one");

        final SegmentIndexFacades<Integer, String> composition =
                SegmentIndexFacades.create(conf, trackedRunner,
                        dataAccess);

        assertEquals("one", composition.pointOperationFacade().get(1));
        verify(dataAccess).get(1);
        verify(indexState).tryPerformOperation();
    }

    @Test
    void assembleBuildsReadFacade() {
        final SegmentIndexFacades<Integer, String> composition =
                SegmentIndexFacades.create(conf, trackedRunner,
                        dataAccess);

        assertNotNull(composition.readFacade());
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(tdi))
                .identity(identity -> identity.valueTypeDescriptor(tds))
                .identity(identity -> identity.name("segment-index-facade-composition-test"))
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
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }
}
