package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexTrackedOperationRunnerTest {

    @Mock
    private SegmentIndexStateMachine stateMachine;

    @Mock
    private Supplier<String> operation;

    private SegmentIndexTrackedOperationRunner runner;

    @BeforeEach
    void setUp() {
        runner = new SegmentIndexTrackedOperationRunner(stateMachine,
                SegmentIndexOperationGate.create());
    }

    @Test
    void runTrackedChecksStateInsideOperationTrackingBeforeOperation() {
        final List<String> events = new ArrayList<>();
        runner = new SegmentIndexTrackedOperationRunner(stateMachine,
                newOperationGate(events));
        doAnswer(invocation -> {
            events.add("state");
            return null;
        }).when(stateMachine).ensureOperational();
        when(operation.get()).thenAnswer(invocation -> {
            events.add("operation");
            return "value";
        });

        final String result = runner.runTracked(operation);

        assertEquals("value", result);
        assertEquals(List.of("tracked", "state", "operation"), events);
    }

    @Test
    void constructorRejectsMissingCollaborators() {
        final SegmentIndexOperationGate operationGate =
                SegmentIndexOperationGate.create();

        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexTrackedOperationRunner(null,
                        operationGate));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexTrackedOperationRunner(stateMachine,
                        null));
    }

    @Test
    void runTrackedVoidRunsOperationThroughTrackedPath() {
        final AtomicBoolean operationRan = new AtomicBoolean();

        runner.runTrackedVoid(() -> operationRan.set(true));

        assertTrue(operationRan.get());
        verify(stateMachine).ensureOperational();
    }

    private SegmentIndexOperationGate newOperationGate(
            final List<String> events) {
        final SegmentIndexOperationGate operationGate = mock(
                SegmentIndexOperationGate.class);
        doAnswer(invocation -> {
            events.add("tracked");
            final Supplier<?> task = invocation.getArgument(0);
            return task.get();
        }).when(operationGate).trackOperation(any());
        return operationGate;
    }
}
