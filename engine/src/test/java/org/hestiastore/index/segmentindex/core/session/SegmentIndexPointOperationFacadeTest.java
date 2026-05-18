package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexPointOperationFacadeTest {

    @Mock
    private SegmentIndexDataAccess<Integer, String> dataAccess;

    @Mock
    private SegmentIndexStateMachine stateMachine;

    private SegmentIndexPointOperationFacade<Integer, String> pointOperationFacade;

    @BeforeEach
    void setUp() {
        pointOperationFacade = new SegmentIndexPointOperationFacade<>(
                new SegmentIndexTrackedOperationRunner<>(stateMachine,
                        SegmentIndexOperationGate.create()),
                dataAccess);
    }

    @Test
    void putGetDeleteDelegateThroughTrackedRunner() {
        when(dataAccess.get(1)).thenReturn("one");

        pointOperationFacade.put(1, "one");
        assertEquals("one", pointOperationFacade.get(1));
        pointOperationFacade.delete(1);

        verify(dataAccess).put(1, "one");
        verify(dataAccess).get(1);
        verify(dataAccess).delete(1);
        verify(stateMachine, times(3)).ensureOperational();
    }

}
