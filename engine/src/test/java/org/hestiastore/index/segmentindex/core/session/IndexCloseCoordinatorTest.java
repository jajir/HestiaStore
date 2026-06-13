package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexCloseCoordinatorTest {

    @Mock
    private Runnable awaitOperationDrain;

    @Mock
    private IndexRuntimeCloseResources<Integer, String> closeResources;

    @Mock
    private SegmentIndexStateMachine stateMachine;

    @Mock
    private SegmentIndexOperationGate operationGate;

    @Mock
    private Directory directory;

    @Mock
    private FileLock fileLock;

    @Mock
    private ExecutorRegistry executorRegistry;

    private IndexCloseCoordinator<Integer, String> closeCoordinator;

    @BeforeEach
    void setUp() {
        when(directory.isFileExists(".lock")).thenReturn(false);
        when(directory.getLock(".lock")).thenReturn(fileLock);
        when(fileLock.isLocked()).thenReturn(false, true);
        doAnswer(invocation -> {
            awaitOperationDrain.run();
            return null;
        }).when(operationGate).awaitOperationDrain();
        closeCoordinator = new IndexCloseCoordinator<>("test-index",
                stateMachine, operationGate, new IndexOperationStatsRecorder(),
                closeResources, executorRegistry,
                new IndexDirectoryLock(directory));
    }

    @Test
    void close_runsShutdownStepsInOrder() {
        closeCoordinator.close();

        final InOrder inOrder = inOrder(awaitOperationDrain, closeResources,
                stateMachine, executorRegistry, fileLock);
        inOrder.verify(stateMachine).beginClose();
        inOrder.verify(awaitOperationDrain).run();
        inOrder.verify(closeResources).closeSplitRuntime();
        inOrder.verify(closeResources).sealAsyncMaintenanceAndWait();
        inOrder.verify(closeResources).flushAndWait();
        inOrder.verify(closeResources).closeCoreStorage();
        inOrder.verify(closeResources).closeWal();
        inOrder.verify(executorRegistry).close();
        inOrder.verify(stateMachine).completeClose();
        inOrder.verify(fileLock).unlock();
    }

    @Test
    void close_marksErrorAndReleasesResourcesWhenCoreStorageCloseFails() {
        final IndexException failure = new IndexException("close failed");
        doThrow(failure).when(closeResources).closeCoreStorage();

        final IndexException thrown = assertThrows(IndexException.class,
                () -> closeCoordinator.close());

        assertSame(failure, thrown);
        verify(closeResources).flushAndWait();
        verify(closeResources).closeWal();
        verify(executorRegistry).close();
        verify(stateMachine).completeClose();
        verify(fileLock).unlock();
        verify(stateMachine).markRuntimeFailure(failure);
    }

    @Test
    void close_releasesLockWhenExecutorShutdownFails() {
        final IndexException failure = new IndexException("executor timeout");
        doThrow(failure).when(executorRegistry).close();

        final IndexException thrown = assertThrows(IndexException.class,
                () -> closeCoordinator.close());

        assertSame(failure, thrown);
        verify(closeResources).closeWal();
        verify(stateMachine).completeClose();
        verify(fileLock).unlock();
        verify(stateMachine).markRuntimeFailure(failure);
    }

    @Test
    void close_suppressesLaterFailuresOnFirstCloseFailure() {
        final IndexException firstFailure = new IndexException(
                "core storage failed");
        final IndexException secondFailure = new IndexException("wal failed");
        doThrow(firstFailure).when(closeResources).closeCoreStorage();
        doThrow(secondFailure).when(closeResources).closeWal();

        final IndexException thrown = assertThrows(IndexException.class,
                () -> closeCoordinator.close());

        assertSame(firstFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(secondFailure, thrown.getSuppressed()[0]);
        verify(executorRegistry).close();
        verify(fileLock).unlock();
        verify(stateMachine).markRuntimeFailure(firstFailure);
    }
}
