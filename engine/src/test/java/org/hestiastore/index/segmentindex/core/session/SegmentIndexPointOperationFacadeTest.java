package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.session.state.IndexState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentIndexPointOperationFacadeTest {

    private SegmentIndexDataAccess<Integer, String> dataAccess;
    private SegmentIndexPointOperationFacade<Integer, String> pointOperationFacade;

    @BeforeEach
    void setUp() {
        dataAccess = mock(SegmentIndexDataAccess.class);
        pointOperationFacade = new SegmentIndexPointOperationFacade<>(
                new SegmentIndexTrackedOperationRunner<>(this::readyState,
                        IndexOperationTrackingAccess.create()),
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
    }

    private IndexState<Integer, String> readyState() {
        return new IndexState<>() {
            @Override
            public SegmentIndexState state() {
                return SegmentIndexState.READY;
            }

            @Override
            public IndexState<Integer, String> onReady() {
                return this;
            }

            @Override
            public IndexState<Integer, String> onClose() {
                return this;
            }

            @Override
            public IndexState<Integer, String> finishClose() {
                return this;
            }

            @Override
            public void tryPerformOperation() {
            }
        };
    }
}
