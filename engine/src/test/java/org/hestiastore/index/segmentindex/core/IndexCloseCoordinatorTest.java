package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class IndexCloseCoordinatorTest {

    @Mock
    private Logger logger;

    @Mock
    private Runnable beginCloseTransition;

    @Mock
    private Runnable awaitOperations;

    @Mock
    private Runnable prepareDurableState;

    @Mock
    private Runnable awaitBackgroundSplitsIdle;

    @Mock
    private Runnable markClosed;

    @Mock
    private Runnable flushStableSegmentsWithSplitPaused;

    @Mock
    private Supplier<SegmentRegistryResult<Void>> closeSegmentRegistry;

    @Mock
    private Runnable flushKeyToSegmentMap;

    @Mock
    private Runnable checkpointWal;

    @Mock
    private LongSupplier getReadCount;

    @Mock
    private LongSupplier getWriteCount;

    @Mock
    private LongSupplier getDeleteCount;

    @Mock
    private Runnable finishCloseTransition;

    @Mock
    private Runnable closeWalRuntime;

    private IndexCloseCoordinator closeCoordinator;

    @BeforeEach
    void setUp() {
        closeCoordinator = new IndexCloseCoordinator(logger, "test-index",
                beginCloseTransition, awaitOperations, prepareDurableState,
                awaitBackgroundSplitsIdle, markClosed,
                flushStableSegmentsWithSplitPaused, closeSegmentRegistry,
                flushKeyToSegmentMap, checkpointWal, getReadCount,
                getWriteCount, getDeleteCount, finishCloseTransition,
                closeWalRuntime);
    }

    @Test
    void close_runsShutdownStepsInOrder() {
        when(closeSegmentRegistry.get()).thenReturn(SegmentRegistryResult.ok());
        when(logger.isDebugEnabled()).thenReturn(true);
        when(getReadCount.getAsLong()).thenReturn(1L);
        when(getWriteCount.getAsLong()).thenReturn(2L);
        when(getDeleteCount.getAsLong()).thenReturn(3L);

        closeCoordinator.close();

        final InOrder inOrder = inOrder(beginCloseTransition,
                awaitOperations, prepareDurableState,
                awaitBackgroundSplitsIdle, markClosed,
                flushStableSegmentsWithSplitPaused, closeSegmentRegistry,
                flushKeyToSegmentMap, checkpointWal, finishCloseTransition,
                closeWalRuntime);
        inOrder.verify(beginCloseTransition).run();
        inOrder.verify(awaitOperations).run();
        inOrder.verify(prepareDurableState).run();
        inOrder.verify(awaitBackgroundSplitsIdle).run();
        inOrder.verify(prepareDurableState).run();
        inOrder.verify(awaitBackgroundSplitsIdle).run();
        inOrder.verify(markClosed).run();
        inOrder.verify(flushStableSegmentsWithSplitPaused).run();
        inOrder.verify(closeSegmentRegistry).get();
        inOrder.verify(flushKeyToSegmentMap).run();
        inOrder.verify(checkpointWal).run();
        inOrder.verify(finishCloseTransition).run();
        inOrder.verify(closeWalRuntime).run();
    }

    @Test
    void close_stillFinishesAndClosesWalWhenRegistryCloseFails() {
        when(closeSegmentRegistry.get())
                .thenReturn(SegmentRegistryResult.error());

        assertThrows(IndexException.class, () -> closeCoordinator.close());

        verify(markClosed).run();
        verify(flushStableSegmentsWithSplitPaused).run();
        verify(flushKeyToSegmentMap, never()).run();
        verify(checkpointWal, never()).run();
        verify(finishCloseTransition).run();
        verify(closeWalRuntime).run();
    }
}
