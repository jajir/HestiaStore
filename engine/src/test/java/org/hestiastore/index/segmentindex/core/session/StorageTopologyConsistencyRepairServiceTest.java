package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;

import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StorageTopologyConsistencyRepairServiceTest {

    @Mock
    private StorageService<Integer, String> storageService;

    @Mock
    private SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime;

    private StorageTopologyConsistencyRepairService repairService;

    @BeforeEach
    void setUp() {
        repairService = new StorageTopologyConsistencyRepairService(
                storageService, topologyRuntime);
    }

    @Test
    void checkAndRepairConsistencyRepairsStorageBeforeRequestingSplitScan() {
        repairService.checkAndRepairConsistency();

        final InOrder inOrder = inOrder(storageService, topologyRuntime);
        inOrder.verify(storageService).checkAndRepairConsistency();
        inOrder.verify(topologyRuntime).requestFullSplitScan();
    }

    @Test
    void constructorRejectsMissingCollaborators() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new StorageTopologyConsistencyRepairService(
                                null, topologyRuntime)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new StorageTopologyConsistencyRepairService(
                                storageService, null)));
    }
}
