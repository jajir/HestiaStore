package org.hestiastore.index.segmentindex.core.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexStateReadyTest {

    @Mock
    private FileLock fileLock;

    @Test
    void onCloseTransitionsToClosingState() {
        final IndexStateReady<Integer, String> state = new IndexStateReady<>(
                fileLock);

        final IndexState<Integer, String> nextState = state.onClose();

        assertInstanceOf(IndexStateClosing.class, nextState);
        assertEquals(SegmentIndexState.CLOSING, nextState.state());
        verify(fileLock, never()).unlock();
    }

    @Test
    void tryPerformOperationDoesNothing() {
        final IndexStateReady<Integer, String> state = new IndexStateReady<>(
                fileLock);

        state.tryPerformOperation();

        assertEquals(SegmentIndexState.READY, state.state());
    }
}
