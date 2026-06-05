package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.teardown.SegmentIndexTeardownStep;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexCloseTeardownStepsTest {

    @Mock
    private IndexCloseCoordinator<Integer, String> context;

    @Mock
    private SegmentIndexOperationGate operationGate;

    @Mock
    private SegmentIndexRuntime<Integer, String> runtime;

    @Mock
    private IndexOperationStatsRecorder operationStatsRecorder;

    @Mock
    private ExecutorRegistry executorRegistry;

    @Mock
    private SegmentIndexStateMachine stateMachine;

    @Mock
    private IndexDirectoryLock directoryLock;

    @Test
    void closeSteps_returnsStandardStepCount() {
        final var closeSteps = IndexCloseTeardownSteps
                .<Integer, String>closeSteps();

        assertEquals(10, closeSteps.size());
    }

    @Test
    void closeSteps_runStandardCloseOrder() {
        stubContext();
        final var closeSteps = IndexCloseTeardownSteps
                .<Integer, String>closeSteps();

        closeSteps.forEach(step -> step.apply(context));

        final InOrder inOrder = inOrder(context, operationGate, runtime,
                executorRegistry, stateMachine, directoryLock);
        inOrder.verify(context).operationGate();
        inOrder.verify(operationGate).awaitOperationDrain();
        inOrder.verify(context).runtime();
        inOrder.verify(runtime).closeSplitRuntime();
        inOrder.verify(context).runtime();
        inOrder.verify(runtime).sealAsyncMaintenanceAndWait();
        inOrder.verify(context).runtime();
        inOrder.verify(runtime).flushAndWait();
        inOrder.verify(context).runtime();
        inOrder.verify(runtime).closeCoreStorage();
        inOrder.verify(context).operationStatsRecorder();
        inOrder.verify(context).runtime();
        inOrder.verify(runtime).closeWalCoordinator();
        inOrder.verify(context).executorRegistry();
        inOrder.verify(executorRegistry).close();
        inOrder.verify(context).stateMachine();
        inOrder.verify(stateMachine).completeClose();
        inOrder.verify(context).directoryLock();
        inOrder.verify(directoryLock).close();
    }

    @Test
    void closeSteps_returnsImmutableList() {
        final var closeSteps = IndexCloseTeardownSteps
                .<Integer, String>closeSteps();
        final SegmentIndexTeardownStep<IndexCloseCoordinator<Integer, String>> extraStep =
                context -> {
                    throw new AssertionError("not called");
                };

        assertThrows(UnsupportedOperationException.class,
                () -> closeSteps.add(extraStep));
    }

    private void stubContext() {
        when(context.operationGate()).thenReturn(operationGate);
        when(context.runtime()).thenReturn(runtime);
        when(context.operationStatsRecorder()).thenReturn(
                operationStatsRecorder);
        when(context.executorRegistry()).thenReturn(executorRegistry);
        when(context.stateMachine()).thenReturn(stateMachine);
        when(context.directoryLock()).thenReturn(directoryLock);
    }
}
