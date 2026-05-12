package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.Test;

class SegmentIndexStateMachineTest {

    @Test
    void startsInOpeningState() {
        final SegmentIndexStateMachine stateMachine =
                new SegmentIndexStateMachine();

        assertEquals(SegmentIndexState.OPENING, stateMachine.getState());
    }

    @Test
    void markReadyTransitionsOnlyFromOpening() {
        final SegmentIndexStateMachine stateMachine =
                new SegmentIndexStateMachine();

        stateMachine.markReady();

        assertEquals(SegmentIndexState.READY, stateMachine.getState());
        assertThrows(IllegalStateException.class, stateMachine::markReady);
    }

    @Test
    void beginCloseTransitionsReadyToClosing() {
        final SegmentIndexStateMachine stateMachine = readyStateMachine();

        stateMachine.beginClose();

        assertEquals(SegmentIndexState.CLOSING, stateMachine.getState());
        assertThrows(IllegalStateException.class, stateMachine::beginClose);
    }

    @Test
    void beginCloseRejectsOpeningState() {
        final SegmentIndexStateMachine stateMachine =
                new SegmentIndexStateMachine();

        assertThrows(IllegalStateException.class, stateMachine::beginClose);
        assertEquals(SegmentIndexState.OPENING, stateMachine.getState());
    }

    @Test
    void completeCloseTransitionsClosingToClosed() {
        final SegmentIndexStateMachine stateMachine = closingStateMachine();

        stateMachine.completeClose();

        assertEquals(SegmentIndexState.CLOSED, stateMachine.getState());
        assertDoesNotThrow(stateMachine::completeClose);
    }

    @Test
    void completeClosePreservesErrorState() {
        final SegmentIndexStateMachine stateMachine =
                new SegmentIndexStateMachine();
        final IllegalStateException failure = new IllegalStateException("boom");

        stateMachine.markRuntimeFailure(failure);
        stateMachine.completeClose();

        assertEquals(SegmentIndexState.ERROR, stateMachine.getState());
        final IllegalStateException ex = assertThrows(
                IllegalStateException.class, stateMachine::ensureOperational);
        assertSame(failure, ex.getCause());
    }

    @Test
    void markRuntimeFailureMovesAnyStateToErrorAndPreservesOriginalCause() {
        final SegmentIndexStateMachine stateMachine = closedStateMachine();
        final IllegalStateException firstFailure =
                new IllegalStateException("first");
        final IllegalStateException secondFailure =
                new IllegalStateException("second");

        stateMachine.markRuntimeFailure(firstFailure);
        stateMachine.markRuntimeFailure(secondFailure);

        assertEquals(SegmentIndexState.ERROR, stateMachine.getState());
        final IllegalStateException ex = assertThrows(
                IllegalStateException.class, stateMachine::ensureOperational);
        assertSame(firstFailure, ex.getCause());
    }

    @Test
    void ensureOperationalAllowsOnlyReadyState() {
        assertDoesNotThrow(readyStateMachine()::ensureOperational);
        assertThrows(IllegalStateException.class,
                new SegmentIndexStateMachine()::ensureOperational);
        assertThrows(IllegalStateException.class,
                closingStateMachine()::ensureOperational);
        assertThrows(IllegalStateException.class,
                closedStateMachine()::ensureOperational);
        assertThrows(IllegalStateException.class,
                failedStateMachine()::ensureOperational);
    }

    private static SegmentIndexStateMachine readyStateMachine() {
        final SegmentIndexStateMachine stateMachine =
                new SegmentIndexStateMachine();
        stateMachine.markReady();
        return stateMachine;
    }

    private static SegmentIndexStateMachine closingStateMachine() {
        final SegmentIndexStateMachine stateMachine = readyStateMachine();
        stateMachine.beginClose();
        return stateMachine;
    }

    private static SegmentIndexStateMachine closedStateMachine() {
        final SegmentIndexStateMachine stateMachine = closingStateMachine();
        stateMachine.completeClose();
        return stateMachine;
    }

    private static SegmentIndexStateMachine failedStateMachine() {
        final SegmentIndexStateMachine stateMachine =
                new SegmentIndexStateMachine();
        stateMachine.markRuntimeFailure(new IllegalStateException("boom"));
        return stateMachine;
    }
}
