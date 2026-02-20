package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexStateOpeningTest {

    @Mock
    private Directory directory;

    @Mock
    private FileLock fileLock;

    @Test
    void locksFileOnConstruction() {
        when(directory.isFileExists(".lock")).thenReturn(false);
        when(directory.getLock(".lock")).thenReturn(fileLock);
        when(fileLock.isLocked()).thenReturn(false);

        new IndexStateOpening<>(directory);

        verify(fileLock).lock();
    }

    @Test
    void throwsWhenDirectoryAlreadyLocked() {
        when(directory.isFileExists(".lock")).thenReturn(true);
        when(directory.getLock(".lock")).thenReturn(fileLock);
        when(fileLock.isLocked()).thenReturn(true);

        assertThrows(IllegalStateException.class,
                () -> new IndexStateOpening<>(directory));
    }

    @Test
    void marksRecoveredStaleLock() {
        when(directory.isFileExists(".lock")).thenReturn(true);
        when(directory.getLock(".lock")).thenReturn(fileLock);
        when(fileLock.isLocked()).thenReturn(false);

        final IndexStateOpening<Object, Object> state = new IndexStateOpening<>(
                directory);

        assertTrue(state.wasStaleLockRecovered());
    }
}
