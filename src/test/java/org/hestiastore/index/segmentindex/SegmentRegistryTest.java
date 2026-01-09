package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
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

    @Mock
    private ExecutorService maintenanceExecutor;

    private SegmentRegistry<Integer, String> registry;

    @BeforeEach
    void setUp() {
        Mockito.when(conf.getNumberOfThreads()).thenReturn(1);
        Mockito.when(conf.getMaintenanceExecutor())
                .thenReturn(maintenanceExecutor);
        Mockito.when(conf.getMaxNumberOfKeysInSegmentWriteCache())
                .thenReturn(5);
        Mockito.when(conf.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush())
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
        final SegmentId segmentId = SegmentId.of(1);

        final Segment<Integer, String> first = registry.getSegment(segmentId);
        final Segment<Integer, String> second = registry.getSegment(segmentId);

        assertSame(first, second);
        first.close();
        final Segment<Integer, String> third = registry.getSegment(segmentId);

        assertNotSame(first, third);
    }

    @Test
    void evictSegmentIfSame_removesOnlyMatchingInstance() {
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentId otherId = SegmentId.of(2);

        final Segment<Integer, String> segment = registry.getSegment(segmentId);
        final Segment<Integer, String> otherSegment = registry
                .getSegment(otherId);

        assertFalse(registry.evictSegmentIfSame(segmentId, otherSegment));
        assertSame(segment, registry.getSegment(segmentId));

        assertTrue(registry.evictSegmentIfSame(segmentId, segment));
        assertNotSame(segment, registry.getSegment(segmentId));
    }

}
