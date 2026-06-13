package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.junit.jupiter.api.Test;

class BootstrapWalCheckpointTest {

    @Test
    void checkpointFailsBeforeStorageServiceIsBound() {
        final BootstrapWalCheckpoint checkpoint = new BootstrapWalCheckpoint();

        final IllegalStateException ex = assertThrows(
                IllegalStateException.class, checkpoint::checkpoint);

        assertEquals("storageService was not initialized.", ex.getMessage());
    }

    @Test
    @SuppressWarnings("unchecked")
    void checkpointDelegatesToBoundStorageService() {
        final StorageService<Integer, String> storageService = mock(
                StorageService.class);
        final BootstrapWalCheckpoint checkpoint = new BootstrapWalCheckpoint();
        checkpoint.bindStorageService(storageService);

        checkpoint.checkpoint();

        verify(storageService).checkpointWal();
    }

    @Test
    void bindStorageServiceRejectsNull() {
        final BootstrapWalCheckpoint checkpoint = new BootstrapWalCheckpoint();

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> checkpoint.bindStorageService(null));

        assertEquals("Property 'storageService' must not be null.",
                ex.getMessage());
    }
}
