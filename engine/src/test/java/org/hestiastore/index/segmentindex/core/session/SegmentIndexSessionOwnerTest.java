package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class SegmentIndexSessionOwnerTest {

    private SegmentIndexStateMachine stateMachine;
    private IndexCloseCoordinator<Integer, String> closeCoordinator;
    private SegmentIndexSessionOwner<Integer, String> owner;

    @BeforeEach
    void setUp() {
        stateMachine = mock(SegmentIndexStateMachine.class);
        closeCoordinator = mock(IndexCloseCoordinator.class);
        owner = new SegmentIndexSessionOwner<>(stateMachine, closeCoordinator);
    }

    @Test
    void exposesStateMachine() {
        assertSame(stateMachine, owner.stateMachine());
    }

    @Test
    void closeDelegatesToCloseCoordinator() {
        owner.close();

        verify(closeCoordinator).close();
    }

    @Test
    void ensureOperationalUsesStateMachine() {
        assertDoesNotThrow(owner::ensureOperational);

        verify(stateMachine).ensureOperational();
    }
}
