package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.metrics.Stats;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexCloseCoordinatorTest {

    @Mock
    private Runnable awaitOperations;

    @Mock
    private Runnable finishCloseTransition;

    @Mock
    private SegmentIndexRuntime<Integer, String> runtime;

    @Mock
    private SegmentIndexStateMachine stateMachine;

    @Mock
    private IndexOperationTrackingAccess operationTracker;

    @Mock
    private Directory directory;

    @Mock
    private FileLock fileLock;

    private IndexCloseCoordinator<Integer, String> closeCoordinator;

    @BeforeEach
    void setUp() {
        when(directory.isFileExists(".lock")).thenReturn(false);
        when(directory.getLock(".lock")).thenReturn(fileLock);
        when(fileLock.isLocked()).thenReturn(false, true);
        doAnswer(invocation -> {
            awaitOperations.run();
            return null;
        }).when(operationTracker).awaitOperations();
        doAnswer(invocation -> {
            finishCloseTransition.run();
            return null;
        }).when(stateMachine).completeClose();
        closeCoordinator = new IndexCloseCoordinator<>("test-index",
                stateMachine, operationTracker, new Stats(), runtime,
                new IndexDirectoryLock(directory));
    }

    @Test
    void close_runsShutdownStepsInOrder() {
        closeCoordinator.close();

        final InOrder inOrder = inOrder(awaitOperations, runtime,
                stateMachine,
                finishCloseTransition, fileLock);
        inOrder.verify(stateMachine).beginClose();
        inOrder.verify(awaitOperations).run();
        inOrder.verify(runtime).closeSplitRuntime();
        inOrder.verify(runtime).flushAndWait();
        inOrder.verify(runtime).closeSegmentRegistry();
        inOrder.verify(runtime).closeKeyToSegmentMapIfOpen();
        inOrder.verify(finishCloseTransition).run();
        inOrder.verify(runtime).closeWalRuntime();
        inOrder.verify(fileLock).unlock();
    }

    @Test
    void close_stillFinishesAndClosesWalWhenRegistryCloseFails() {
        doThrow(new IndexException("close failed")).when(runtime)
                .closeSegmentRegistry();

        assertThrows(IndexException.class, () -> closeCoordinator.close());

        verify(runtime).flushAndWait();
        verify(runtime, never()).closeKeyToSegmentMapIfOpen();
        verify(finishCloseTransition).run();
        verify(runtime).closeWalRuntime();
        verify(fileLock).unlock();
    }
}
