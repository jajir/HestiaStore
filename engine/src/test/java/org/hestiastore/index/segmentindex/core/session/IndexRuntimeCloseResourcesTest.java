package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;

import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexRuntimeCloseResourcesTest {

    @Mock
    private SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime;

    @Mock
    private MaintenanceService maintenance;

    @Mock
    private CoreStorageRuntime<Integer, String> coreStorageRuntime;

    @Mock
    private StorageService<Integer, String> storageService;

    private IndexRuntimeCloseResources<Integer, String> closeResources;

    @BeforeEach
    void setUp() {
        closeResources = new IndexRuntimeCloseResources<>(topologyRuntime,
                maintenance, coreStorageRuntime, storageService);
    }

    @Test
    void closeMethodsDelegateToConcreteOwners() {
        closeResources.closeSplitRuntime();
        closeResources.sealAsyncMaintenanceAndWait();
        closeResources.flushAndWait();
        closeResources.closeCoreStorage();
        closeResources.closeWal();

        final InOrder inOrder = inOrder(topologyRuntime, maintenance,
                coreStorageRuntime, storageService);
        inOrder.verify(topologyRuntime).closeSplitRuntime();
        inOrder.verify(maintenance).sealAsyncMaintenanceAndWait();
        inOrder.verify(maintenance).flushAndWait();
        inOrder.verify(coreStorageRuntime).closeCoreStorage();
        inOrder.verify(storageService).closeWal();
    }

    @Test
    void constructorRejectsMissingCollaborators() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new IndexRuntimeCloseResources<>(
                                null, maintenance,
                                coreStorageRuntime, storageService)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new IndexRuntimeCloseResources<>(
                                topologyRuntime, null,
                                coreStorageRuntime, storageService)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new IndexRuntimeCloseResources<>(
                                topologyRuntime, maintenance,
                                null, storageService)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new IndexRuntimeCloseResources<>(
                                topologyRuntime, maintenance,
                                coreStorageRuntime, null)));
    }
}
