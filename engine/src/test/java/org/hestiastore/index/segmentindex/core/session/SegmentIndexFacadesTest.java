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
import org.hestiastore.index.segmentindex.core.routing.SegmentIndexDataAccess;
import org.hestiastore.index.segmentindex.core.routing.SegmentIndexTrackedOperationRunner;
import org.hestiastore.index.segmentindex.core.routing.IndexOperationTrackingAccess;
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

        assertEquals("one", composition.mutationFacade().get(1));
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
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds)
                .withName("segment-index-facade-composition-test")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfKeysInPartitionBuffer(6)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }
}
