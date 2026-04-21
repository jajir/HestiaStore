package org.hestiastore.index.segmentindex.core.state;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexStateFileLockResolverTest {

    @Mock
    private Directory directory;

    @Mock
    private FileLock fileLock;

    @Test
    void resolveReturnsReadyStateLock() {
        final IndexStateFileLockResolver<Integer, String> resolver = new IndexStateFileLockResolver<>();

        assertSame(fileLock, resolver.resolve(new IndexStateReady<>(fileLock)));
    }

    @Test
    void resolveReturnsOpeningStateLock() {
        when(directory.isFileExists(".lock")).thenReturn(false);
        when(directory.getLock(".lock")).thenReturn(fileLock);
        when(fileLock.isLocked()).thenReturn(false);
        final IndexStateOpening<Integer, String> state = new IndexStateOpening<>(
                directory);
        final IndexStateFileLockResolver<Integer, String> resolver = new IndexStateFileLockResolver<>();

        assertSame(fileLock, resolver.resolve(state));
    }

    @Test
    void resolveReturnsClosingStateLock() {
        final IndexStateFileLockResolver<Integer, String> resolver = new IndexStateFileLockResolver<>();

        assertSame(fileLock,
                resolver.resolve(new IndexStateClosing<>(fileLock)));
    }

    @Test
    void resolveReturnsNullForClosedState() {
        final IndexStateFileLockResolver<Integer, String> resolver = new IndexStateFileLockResolver<>();

        assertNull(resolver.resolve(new IndexStateClosed<>()));
    }
}
