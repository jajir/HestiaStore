package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class SegmentIndexPointOperationFacadeTest {

    private SegmentIndexDataAccess<Integer, String> dataAccess;
    private SegmentIndexPointOperationFacade<Integer, String> pointOperationFacade;

    @BeforeEach
    void setUp() {
        dataAccess = mock(SegmentIndexDataAccess.class);
        pointOperationFacade = new SegmentIndexPointOperationFacade<>(
                new SegmentIndexTrackedOperationRunner<>(() -> {
                    // Test guard allows all tracked operations immediately.
                },
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

}
