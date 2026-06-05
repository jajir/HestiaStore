package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.metrics.RuntimeMetricsCollector;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
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
                        mock(RuntimeMetricsCollector.class),
                        mock(IndexRuntimeMonitoring.class),
                        mock(RuntimeTuning.class)));

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
        final SegmentIndexMetricsSnapshot metricsSnapshot = mock(
                SegmentIndexMetricsSnapshot.class);
        final RuntimeMetricsCollector metricsCollector = mock(
                RuntimeMetricsCollector.class);
        final IndexRuntimeMonitoring runtimeMonitoring = mock(IndexRuntimeMonitoring.class);
        final RuntimeTuning runtimeConfiguration = mock(RuntimeTuning.class);
        final SegmentRuntimeLimitApplier<Integer, String> runtimeLimitApplier =
                mock(SegmentRuntimeLimitApplier.class);
        when(metricsCollector.metricsSnapshot()).thenReturn(metricsSnapshot);

        final SegmentIndexRuntimeServices<Integer, String> state =
                new SegmentIndexRuntimeServices<>(walCoordinator,
                        operationAccess, maintenance,
                        runtimeLimitApplier, metricsCollector,
                        runtimeMonitoring, runtimeConfiguration);

        assertSame(walCoordinator, state.walCoordinator());
        assertSame(operationAccess, state.operationAccess());
        assertSame(maintenance, state.maintenance());
        assertSame(metricsSnapshot, state.metricsSnapshotSupplier().get());
        assertSame(runtimeMonitoring, state.runtimeMonitoring());
        assertSame(runtimeConfiguration, state.runtimeTuning());
        assertSame(runtimeLimitApplier, state.runtimeLimitApplier());
    }
}
