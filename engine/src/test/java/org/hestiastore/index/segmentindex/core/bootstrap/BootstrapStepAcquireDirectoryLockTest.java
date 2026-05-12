package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapStepAcquireDirectoryLockTest {

    @Mock
    private SegmentIndexSessionResources<Integer, String> sessionResources;

    private BootstrapStepAcquireDirectoryLock<Integer, String> step;

    @BeforeEach
    void setUp() {
        step = new BootstrapStepAcquireDirectoryLock<>(sessionResources);
    }

    @Test
    void constructor_rejectsNullSessionResources() {
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapStepAcquireDirectoryLock<Integer, String>(
                        null));
    }

    @Test
    void apply_acquiresDirectoryLockThroughSessionResources() {
        final MemDirectory directory = new MemDirectory();

        assertDoesNotThrow(() -> step.apply(
                request(directory, SegmentIndexBootstrapMode.CREATE),
                new SegmentIndexBootstrapState<>()));

        verify(sessionResources).acquireDirectoryLock(directory);
    }

    @Test
    void closeResource_keepsDirectoryLockOwnedBySessionResources() {
        assertDoesNotThrow(step::closeResource);

        verifyNoMoreInteractions(sessionResources);
    }
}
