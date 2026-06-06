package org.hestiastore.index.segmentindex.maintenance;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexMaintenanceImplTest {

    @Mock
    private MaintenanceService maintenanceService;

    @Mock
    private StorageService<Object, Object> storageService;

    @Mock
    private Runnable requestFullSplitScan;

    private SegmentIndexMaintenance maintenance;

    @BeforeEach
    void setUp() {
        maintenance = new SegmentIndexMaintenanceImpl(maintenanceService,
                storageService, requestFullSplitScan);
    }

    @Test
    void delegatesMaintenanceCommands() {
        assertDoesNotThrow(() -> {
            maintenance.compact();
            maintenance.compactAndWait();
            maintenance.flush();
            maintenance.flushAndWait();
            maintenance.checkAndRepairConsistency();
        });

        verify(maintenanceService).compact();
        verify(maintenanceService).compactAndWait();
        verify(maintenanceService).flush();
        verify(maintenanceService).flushAndWait();
        verify(storageService).checkAndRepairConsistency();
        verify(requestFullSplitScan).run();
        verifyNoMoreInteractions(maintenanceService, storageService,
                requestFullSplitScan);
    }

    @Test
    void constructorRejectsMissingCollaborators() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceImpl(null,
                        storageService, requestFullSplitScan));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceImpl(maintenanceService,
                        null, requestFullSplitScan));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceImpl(maintenanceService,
                        storageService, null));
    }
}
