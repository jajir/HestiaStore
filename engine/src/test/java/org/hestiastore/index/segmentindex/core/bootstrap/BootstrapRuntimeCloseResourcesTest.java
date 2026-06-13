package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;

import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapRuntimeCloseResourcesTest {

    @Mock
    private SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime;

    @Mock
    private CoreStorageRuntime<Integer, String> coreStorageRuntime;

    @Mock
    private StorageService<Integer, String> storageService;

    private BootstrapRuntimeCloseResources<Integer, String> closeResources;

    @BeforeEach
    void setUp() {
        closeResources = new BootstrapRuntimeCloseResources<>(topologyRuntime,
                coreStorageRuntime, storageService);
    }

    @Test
    void closeAfterFailedInitialization_closesRuntimeResourcesInOrder() {
        closeResources.closeAfterFailedInitialization();

        final InOrder inOrder = inOrder(topologyRuntime, coreStorageRuntime,
                storageService);
        inOrder.verify(topologyRuntime).closeSplitRuntime();
        inOrder.verify(coreStorageRuntime).closeCoreStorage();
        inOrder.verify(storageService).closeWal();
    }

    @Test
    void closeAfterFailedInitialization_suppressesLaterCleanupFailures() {
        final RuntimeException splitFailure =
                new RuntimeException("split close failed");
        final RuntimeException coreFailure =
                new RuntimeException("core close failed");
        final RuntimeException walFailure =
                new RuntimeException("wal close failed");
        doThrow(splitFailure).when(topologyRuntime).closeSplitRuntime();
        doThrow(coreFailure).when(coreStorageRuntime).closeCoreStorage();
        doThrow(walFailure).when(storageService).closeWal();

        final RuntimeException failure = assertThrows(RuntimeException.class,
                closeResources::closeAfterFailedInitialization);

        assertSame(splitFailure, failure);
        assertEquals(2, failure.getSuppressed().length);
        assertSame(coreFailure, failure.getSuppressed()[0]);
        assertSame(walFailure, failure.getSuppressed()[1]);
        final InOrder inOrder = inOrder(topologyRuntime, coreStorageRuntime,
                storageService);
        inOrder.verify(topologyRuntime).closeSplitRuntime();
        inOrder.verify(coreStorageRuntime).closeCoreStorage();
        inOrder.verify(storageService).closeWal();
    }

    @Test
    void constructor_rejectsMissingCollaborators() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new BootstrapRuntimeCloseResources<>(
                                null, coreStorageRuntime, storageService)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new BootstrapRuntimeCloseResources<>(
                                topologyRuntime, null, storageService)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new BootstrapRuntimeCloseResources<>(
                                topologyRuntime, coreStorageRuntime, null)));
    }
}
