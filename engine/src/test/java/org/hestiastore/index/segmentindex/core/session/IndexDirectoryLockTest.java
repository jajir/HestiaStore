package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexDirectoryLockTest {

    @Mock
    private Directory directory;

    @Mock
    private FileLock fileLock;

    @BeforeEach
    void setUp() {
        when(directory.getLock(".lock")).thenReturn(fileLock);
    }

    @Test
    void constructorAcquiresDirectoryLock() {
        expectDirectoryLock(false, false);

        final IndexDirectoryLock lock = new IndexDirectoryLock(directory);

        assertFalse(lock.wasStaleLockRecovered());
        verify(fileLock).lock();
    }

    @Test
    void constructorRejectsAlreadyLockedDirectory() {
        expectDirectoryLock(true, true);

        assertThrows(IllegalStateException.class,
                () -> new IndexDirectoryLock(directory));

        verify(fileLock, never()).lock();
    }

    @Test
    void constructorDetectsRecoveredStaleLock() {
        expectDirectoryLock(true, false);

        final IndexDirectoryLock lock = new IndexDirectoryLock(directory);

        assertTrue(lock.wasStaleLockRecovered());
    }

    @Test
    void closeUnlocksOnlyOnce() {
        expectDirectoryLock(false, false, true);
        final IndexDirectoryLock lock = new IndexDirectoryLock(directory);

        lock.close();
        lock.close();

        verify(fileLock).unlock();
    }

    private void expectDirectoryLock(final boolean lockFilePresent,
            final boolean... lockHeld) {
        when(directory.isFileExists(".lock")).thenReturn(lockFilePresent);
        when(fileLock.isLocked()).thenReturn(lockHeld[0], tail(lockHeld));
    }

    private Boolean[] tail(final boolean[] values) {
        final Boolean[] tail = new Boolean[values.length - 1];
        for (int i = 1; i < values.length; i++) {
            tail[i - 1] = values[i];
        }
        return tail;
    }
}
