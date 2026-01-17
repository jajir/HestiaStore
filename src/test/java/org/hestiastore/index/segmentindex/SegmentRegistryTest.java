package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistryTest {

    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();
    private static final List<ChunkFilter> FILTERS = List
            .of(new ChunkFilterDoNothing());

    @Mock
    private AsyncDirectory directoryFacade;

    @Mock
    private IndexConfiguration<Integer, String> conf;

    private SegmentRegistry<Integer, String> registry;

    @BeforeEach
    void setUp() {
        Mockito.when(conf.getNumberOfSegmentIndexMaintenanceThreads())
                .thenReturn(1);
        Mockito.when(conf.getNumberOfIndexMaintenanceThreads()).thenReturn(1);
        registry = new SegmentRegistry<>(directoryFacade, KEY_DESCRIPTOR,
                VALUE_DESCRIPTOR, conf);
    }

    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.close();
        }
    }

    @Test
    void getSegment_reusesInstanceUntilClosed() {
        stubSegmentConfig();
        final SegmentId segmentId = SegmentId.of(1);

        final SegmentResult<Segment<Integer, String>> firstResult = registry
                .getSegment(segmentId);
        final SegmentResult<Segment<Integer, String>> secondResult = registry
                .getSegment(segmentId);
        assertSame(SegmentResultStatus.OK, firstResult.getStatus());
        assertSame(SegmentResultStatus.OK, secondResult.getStatus());
        final Segment<Integer, String> first = firstResult.getValue();
        final Segment<Integer, String> second = secondResult.getValue();

        assertSame(first, second);
        first.close();
        final Segment<Integer, String> third = registry.getSegment(segmentId)
                .getValue();

        assertNotSame(first, third);
    }

    @Test
    void evictSegmentIfSame_removesOnlyMatchingInstance() {
        stubSegmentConfig();
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentId otherId = SegmentId.of(2);

        final Segment<Integer, String> segment = registry.getSegment(segmentId)
                .getValue();
        final Segment<Integer, String> otherSegment = registry
                .getSegment(otherId).getValue();

        assertFalse(registry.evictSegmentIfSame(segmentId, otherSegment));
        assertSame(segment, registry.getSegment(segmentId).getValue());

        assertTrue(registry.evictSegmentIfSame(segmentId, segment));
        assertNotSame(segment, registry.getSegment(segmentId).getValue());
    }

    @Test
    void createsMaintenanceExecutorFromConfiguration() {
        Mockito.when(conf.getNumberOfSegmentIndexMaintenanceThreads())
                .thenReturn(2);
        registry.close();

        registry = new SegmentRegistry<>(directoryFacade, KEY_DESCRIPTOR,
                VALUE_DESCRIPTOR, conf);

        assertNotNull(registry.getMaintenanceExecutor());
    }

    @Test
    void getSegment_returnsBusyWhileSplitInFlight() {
        stubSegmentConfig();
        final SegmentId segmentId = SegmentId.of(1);

        registry.markSplitInFlight(segmentId);

        final SegmentResult<Segment<Integer, String>> busy = registry
                .getSegment(segmentId);
        assertSame(SegmentResultStatus.BUSY, busy.getStatus());

        registry.clearSplitInFlight(segmentId);
        final SegmentResult<Segment<Integer, String>> ok = registry
                .getSegment(segmentId);
        assertSame(SegmentResultStatus.OK, ok.getStatus());
        assertNotNull(ok.getValue());
    }

    private void stubSegmentConfig() {
        Mockito.when(conf.getMaxNumberOfKeysInSegmentWriteCache())
                .thenReturn(5);
        Mockito.when(conf.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance())
                .thenReturn(6);
        Mockito.when(conf.getMaxNumberOfKeysInSegmentCache()).thenReturn(10);
        Mockito.when(conf.getMaxNumberOfKeysInSegmentChunk()).thenReturn(2);
        Mockito.when(conf.getBloomFilterNumberOfHashFunctions()).thenReturn(1);
        Mockito.when(conf.getBloomFilterIndexSizeInBytes()).thenReturn(1024);
        Mockito.when(conf.getBloomFilterProbabilityOfFalsePositive())
                .thenReturn(0.01D);
        Mockito.when(conf.getDiskIoBufferSize()).thenReturn(1024);
        Mockito.when(conf.getEncodingChunkFilters()).thenReturn(FILTERS);
        Mockito.when(conf.getDecodingChunkFilters()).thenReturn(FILTERS);
        Mockito.when(
                directoryFacade.isFileExistsAsync(ArgumentMatchers.anyString()))
                .thenReturn(CompletableFuture.completedFuture(false));
    }
}
