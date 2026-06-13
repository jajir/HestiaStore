package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapWalDurableStateTest {

    @Mock
    private MaintenanceService maintenance;

    private BootstrapWalDurableState durableState;

    @BeforeEach
    void setUp() {
        durableState = new BootstrapWalDurableState(maintenance);
    }

    @Test
    void flushBeforeWalCheckpointFlushesMaintenance() {
        durableState.flushBeforeWalCheckpoint();

        verify(maintenance).flushAndWait();
    }

    @Test
    void constructorRejectsMissingMaintenance() {
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapWalDurableState(null));
    }
}
