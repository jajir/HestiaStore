package org.hestiastore.index.segmentindex.core.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexStateErrorTest {

    @Mock
    private FileLock fileLock;

    private Throwable failure;
    private IndexStateError<Integer, String> state;

    @BeforeEach
    void setUp() {
        failure = new IllegalStateException("boom");
        state = new IndexStateError<>(failure, fileLock);
    }

    @Test
    void onReadyThrows() {
        final IllegalStateException ex = assertThrows(
                IllegalStateException.class, state::onReady);
        assertEquals("Can't make ready index in ERROR.", ex.getMessage());
    }

    @Test
    void tryPerformOperationThrowsWithCause() {
        final IllegalStateException ex = assertThrows(
                IllegalStateException.class, state::tryPerformOperation);
        assertEquals("Index is in ERROR state.", ex.getMessage());
        assertSame(failure, ex.getCause());
    }

    @Test
    void finishCloseUnlocksAndPreservesErrorState() {
        when(fileLock.isLocked()).thenReturn(true);

        final IndexState<Integer, String> nextState = state.finishClose();

        assertSame(state, nextState);
        assertEquals(SegmentIndexState.ERROR, nextState.state());
        verify(fileLock).unlock();
    }
}
