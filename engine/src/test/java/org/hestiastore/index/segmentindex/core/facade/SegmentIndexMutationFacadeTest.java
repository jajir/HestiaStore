package org.hestiastore.index.segmentindex.core.facade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.operation.IndexOperationTrackingAccess;
import org.hestiastore.index.segmentindex.core.state.IndexState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentIndexMutationFacadeTest {

    private SegmentIndexDataAccess<Integer, String> dataAccess;
    private SegmentIndexMutationFacade<Integer, String> mutationFacade;

    @BeforeEach
    void setUp() {
        dataAccess = mock(SegmentIndexDataAccess.class);
        mutationFacade = new SegmentIndexMutationFacade<>(
                new SegmentIndexTrackedOperationRunner<>(this::readyState,
                        IndexOperationTrackingAccess.create()),
                dataAccess);
    }

    @Test
    void putGetDeleteDelegateThroughTrackedRunner() {
        when(dataAccess.get(1)).thenReturn("one");

        mutationFacade.put(1, "one");
        assertEquals("one", mutationFacade.get(1));
        mutationFacade.delete(1);

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
