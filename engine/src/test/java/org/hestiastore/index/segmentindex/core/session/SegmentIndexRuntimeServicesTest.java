package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.function.Supplier;

import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfiguration;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.core.control.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class SegmentIndexRuntimeServicesTest {

    @Test
    void constructorRejectsNullWalCoordinator() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexRuntimeServices<>(null,
                        mock(SegmentIndexOperationAccess.class),
                        mock(MaintenanceService.class),
                        mock(SegmentRuntimeLimitApplier.class),
                        mock(Supplier.class),
                        mock(IndexRuntimeMonitoring.class),
                        mock(RuntimeConfiguration.class)));

        assertEquals("Property 'walCoordinator' must not be null.",
                ex.getMessage());
    }

    @Test
    void exposesStoredServiceCollaborators() {
        final IndexWalCoordinator<Integer, String> walCoordinator = mock(
                IndexWalCoordinator.class);
        final SegmentIndexOperationAccess<Integer, String> operationAccess =
                mock(SegmentIndexOperationAccess.class);
        final MaintenanceService maintenance = mock(MaintenanceService.class);
        final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier =
                mock(Supplier.class);
        final IndexRuntimeMonitoring runtimeMonitoring = mock(IndexRuntimeMonitoring.class);
        final RuntimeConfiguration runtimeConfiguration = mock(RuntimeConfiguration.class);
        final SegmentRuntimeLimitApplier<Integer, String> runtimeLimitApplier =
                mock(SegmentRuntimeLimitApplier.class);

        final SegmentIndexRuntimeServices<Integer, String> state =
                new SegmentIndexRuntimeServices<>(walCoordinator,
                        operationAccess, maintenance,
                        runtimeLimitApplier, metricsSnapshotSupplier,
                        runtimeMonitoring, runtimeConfiguration);

        assertSame(walCoordinator, state.walCoordinator());
        assertSame(operationAccess, state.operationAccess());
        assertSame(maintenance, state.maintenance());
        assertSame(metricsSnapshotSupplier, state.metricsSnapshotSupplier());
        assertSame(runtimeMonitoring, state.runtimeMonitoring());
        assertSame(runtimeConfiguration, state.runtimeConfiguration());
        assertSame(runtimeLimitApplier, state.runtimeLimitApplier());
    }
}
