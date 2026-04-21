package org.hestiastore.index.segmentindex.core.lifecycle;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentindex.core.observability.Stats;
import org.hestiastore.index.segmentindex.core.operation.IndexOperationTrackingAccess;
import org.hestiastore.index.segmentindex.core.state.IndexStateCoordinator;
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
    private Runnable awaitOperations;

    @Mock
    private Runnable flushStableSegmentsWithSplitPaused;

    @Mock
    private Runnable closeSegmentRegistry;

    @Mock
    private Runnable flushKeyToSegmentMap;

    @Mock
    private Runnable checkpointWal;

    @Mock
    private Runnable finishCloseTransition;

    @Mock
    private Runnable closeWalRuntime;

    @Mock
    private IndexCloseAccess<Integer, String> closeAccess;

    @Mock
    private IndexStateCoordinator<Integer, String> stateCoordinator;

    @Mock
    private IndexOperationTrackingAccess operationTracker;

    private IndexCloseCoordinator<Integer, String> closeCoordinator;

    @BeforeEach
    void setUp() {
        doAnswer(invocation -> {
            awaitOperations.run();
            return null;
        }).when(operationTracker).awaitOperations();
        doAnswer(invocation -> {
            finishCloseTransition.run();
            return null;
        }).when(stateCoordinator).completeCloseStateTransition();
        closeCoordinator = new IndexCloseCoordinator<>(logger, "test-index",
                stateCoordinator, operationTracker, new Stats(), closeAccess);
    }

    @Test
    void close_runsShutdownStepsInOrder() {
        when(logger.isDebugEnabled()).thenReturn(true);

        closeCoordinator.close();

        final InOrder inOrder = inOrder(awaitOperations, closeAccess,
                stateCoordinator,
                finishCloseTransition);
        inOrder.verify(stateCoordinator).beginClose();
        inOrder.verify(awaitOperations).run();
        inOrder.verify(closeAccess).awaitBackgroundSplitsExhausted();
        inOrder.verify(closeAccess).flushStableSegmentsWithSplitSchedulingPaused();
        inOrder.verify(closeAccess).closeSegmentRegistry();
        inOrder.verify(closeAccess).flushKeyToSegmentMap();
        inOrder.verify(closeAccess).checkpointWal();
        inOrder.verify(finishCloseTransition).run();
        inOrder.verify(closeAccess).closeWalRuntime();
    }

    @Test
    void close_stillFinishesAndClosesWalWhenRegistryCloseFails() {
        org.mockito.Mockito.doThrow(new IndexException("close failed"))
                .when(closeAccess).closeSegmentRegistry();

        assertThrows(IndexException.class, () -> closeCoordinator.close());

        verify(closeAccess).flushStableSegmentsWithSplitSchedulingPaused();
        verify(closeAccess, never()).flushKeyToSegmentMap();
        verify(closeAccess, never()).checkpointWal();
        verify(finishCloseTransition).run();
        verify(closeAccess).closeWalRuntime();
    }
}
