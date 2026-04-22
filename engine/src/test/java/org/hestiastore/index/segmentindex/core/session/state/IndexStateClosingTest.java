package org.hestiastore.index.segmentindex.core.session.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexStateClosingTest {

    @Mock
    private FileLock fileLock;

    @Test
    void finishCloseTransitionsToClosedStateAndUnlocks() {
        when(fileLock.isLocked()).thenReturn(true);
        final IndexStateClosing<Integer, String> state = new IndexStateClosing<>(
                fileLock);

        final IndexState<Integer, String> nextState = state.finishClose();

        assertInstanceOf(IndexStateClosed.class, nextState);
        assertEquals(SegmentIndexState.CLOSED, nextState.state());
        verify(fileLock).unlock();
    }

    @Test
    void tryPerformOperationRejectsWhileClosing() {
        final IndexStateClosing<Integer, String> state = new IndexStateClosing<>(
                fileLock);

        assertThrows(IllegalStateException.class, state::tryPerformOperation);
    }
}
