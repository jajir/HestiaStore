package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexStateNewTest {

    @Mock
    private AsyncDirectory directory;

    @Mock
    private FileLock fileLock;

    @Test
    void locksFileOnConstruction() {
        when(directory.getLockAsync(".lock"))
                .thenReturn(CompletableFuture.completedFuture(fileLock));
        when(fileLock.isLocked()).thenReturn(false);

        new IndexStateNew<>(directory);

        verify(fileLock).lock();
    }

    @Test
    void throwsWhenDirectoryAlreadyLocked() {
        when(directory.getLockAsync(".lock"))
                .thenReturn(CompletableFuture.completedFuture(fileLock));
        when(fileLock.isLocked()).thenReturn(true);

        assertThrows(IllegalStateException.class,
                () -> new IndexStateNew<>(directory));
    }
}
