package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
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

    private SegmentIndexTrackedOperationRunner<Integer, String> runner;

    @BeforeEach
    void setUp() {
        runner = new SegmentIndexTrackedOperationRunner<>(stateMachine,
                newPassThroughOperationGate());
    }

    @Test
    void runTrackedChecksStateInsideOperationTrackingBeforeOperation() {
        final List<String> events = new ArrayList<>();
        runner = new SegmentIndexTrackedOperationRunner<>(stateMachine,
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
                newPassThroughOperationGate();

        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexTrackedOperationRunner<>(null,
                        operationGate));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexTrackedOperationRunner<>(stateMachine,
                        null));
    }

    @Test
    void runTrackedVoidRunsOperationThroughTrackedPath() {
        final AtomicBoolean operationRan = new AtomicBoolean();

        runner.runTrackedVoid(() -> operationRan.set(true));

        assertTrue(operationRan.get());
        verify(stateMachine).ensureOperational();
    }

    private SegmentIndexOperationGate newPassThroughOperationGate() {
        return newOperationGate(new ArrayList<>());
    }

    private SegmentIndexOperationGate newOperationGate(
            final List<String> events) {
        return new SegmentIndexOperationGate() {

            @Override
            public <T> T trackOperation(final Supplier<T> task) {
                events.add("tracked");
                return task.get();
            }

            @Override
            public void awaitOperationDrain() {
            }
        };
    }
}
