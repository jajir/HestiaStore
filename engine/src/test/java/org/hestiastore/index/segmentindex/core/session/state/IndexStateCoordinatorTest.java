package org.hestiastore.index.segmentindex.core.session.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexTestAccess;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.session.IndexInternalConcurrent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IndexStateCoordinatorTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    private IndexInternalConcurrent<Integer, String> index;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        index = IndexInternalConcurrent.createStarted(new MemDirectory(), tdi, tds,
                conf, conf.resolveRuntimeConfiguration(),
                ExecutorRegistryFixture.from(conf));
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()
                && !(index.getIndexState() instanceof IndexStateClosed)) {
            index.close();
        }
    }

    @Test
    void failWithErrorTransitionsToErrorStateWithOriginalCause() {
        final IllegalStateException failure = new IllegalStateException("boom");

        SegmentIndexTestAccess.stateCoordinator(index).failWithError(failure);

        assertEquals(SegmentIndexState.ERROR, index.runtimeMonitoring().snapshot().getState());
        final IllegalStateException ex = assertThrows(
                IllegalStateException.class, () -> index.get(1));
        assertSame(failure, ex.getCause());
    }

    @Test
    void setSegmentIndexStateUpdatesExposedState() {
        SegmentIndexTestAccess.stateCoordinator(index).beginClose();

        assertEquals(SegmentIndexState.CLOSING, index.runtimeMonitoring().snapshot().getState());

        SegmentIndexTestAccess.stateCoordinator(index)
                .completeCloseStateTransition();
    }

    @Test
    void completeCloseStateTransitionClosesClosingState() {
        SegmentIndexTestAccess.stateCoordinator(index).beginClose();

        SegmentIndexTestAccess.stateCoordinator(index)
                .completeCloseStateTransition();

        assertEquals(SegmentIndexState.CLOSED, index.runtimeMonitoring().snapshot().getState());
        assertTrue(index.getIndexState() instanceof IndexStateClosed);
    }

    @Test
    void completeCloseStateTransitionPreservesErrorSegmentState() {
        SegmentIndexTestAccess.stateCoordinator(index)
                .failWithError(new IllegalStateException("boom"));

        SegmentIndexTestAccess.stateCoordinator(index)
                .completeCloseStateTransition();

        assertEquals(SegmentIndexState.ERROR, index.runtimeMonitoring().snapshot().getState());
        assertTrue(index.getIndexState() instanceof IndexStateError);
    }

    @Test
    void constructorRejectsMismatchedExposedState() {
        assertThrows(IllegalArgumentException.class,
                () -> new IndexStateCoordinator<>(
                        new IndexStateClosed<Integer, String>(),
                        SegmentIndexState.READY));
    }

    @Test
    void completeCloseStateTransitionIsNoOpForClosedState() {
        SegmentIndexTestAccess.stateCoordinator(index).beginClose();
        SegmentIndexTestAccess.stateCoordinator(index)
                .completeCloseStateTransition();

        final IndexState<Integer, String> closedState = index.getIndexState();

        SegmentIndexTestAccess.stateCoordinator(index)
                .completeCloseStateTransition();

        assertSame(closedState, index.getIndexState());
        assertEquals(SegmentIndexState.CLOSED, index.runtimeMonitoring().snapshot().getState());
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(tdi))
                .identity(identity -> identity.valueTypeDescriptor(tds))
                .identity(identity -> identity.name("index-state-coordinator-test"))
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
