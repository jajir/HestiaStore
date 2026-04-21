package org.hestiastore.index.segmentindex.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.function.Supplier;

import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.core.control.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.core.durability.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.maintenance.SegmentIndexMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.operation.SegmentIndexOperationAccess;
import org.junit.jupiter.api.Test;

class SegmentIndexRuntimeServicesTest {

    @Test
    void constructorRejectsNullWalCoordinator() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexRuntimeServices<>(null,
                        mock(SegmentIndexOperationAccess.class),
                        mock(SegmentIndexMaintenanceAccess.class),
                        mock(SegmentRuntimeLimitApplier.class),
                        mock(Supplier.class),
                        mock(IndexControlPlane.class)));

        assertEquals("Property 'walCoordinator' must not be null.",
                ex.getMessage());
    }

    @Test
    void exposesStoredServiceCollaborators() {
        final IndexWalCoordinator<Integer, String> walCoordinator = mock(
                IndexWalCoordinator.class);
        final SegmentIndexOperationAccess<Integer, String> operationAccess =
                mock(SegmentIndexOperationAccess.class);
        final SegmentIndexMaintenanceAccess<Integer, String> maintenanceAccess =
                mock(SegmentIndexMaintenanceAccess.class);
        final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier =
                mock(Supplier.class);
        final IndexControlPlane controlPlane = mock(IndexControlPlane.class);
        final SegmentRuntimeLimitApplier<Integer, String> runtimeLimitApplier =
                mock(SegmentRuntimeLimitApplier.class);

        final SegmentIndexRuntimeServices<Integer, String> state =
                new SegmentIndexRuntimeServices<>(walCoordinator,
                        operationAccess, maintenanceAccess,
                        runtimeLimitApplier, metricsSnapshotSupplier,
                        controlPlane);

        assertSame(walCoordinator, state.walCoordinator());
        assertSame(operationAccess, state.operationAccess());
        assertSame(maintenanceAccess, state.maintenanceAccess());
        assertSame(metricsSnapshotSupplier, state.metricsSnapshotSupplier());
        assertSame(controlPlane, state.controlPlane());
        assertSame(runtimeLimitApplier, state.runtimeLimitApplier());
    }
}
