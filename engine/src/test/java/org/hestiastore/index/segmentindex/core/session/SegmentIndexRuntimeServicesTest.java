package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class SegmentIndexRuntimeServicesTest {

    @Test
    void constructorRejectsNullOperationAccess() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexRuntimeServices<>(null,
                        mock(MaintenanceService.class),
                        mock(IndexRuntimeMonitoring.class),
                        mock(RuntimeTuning.class)));

        assertEquals("Property 'operationAccess' must not be null.",
                ex.getMessage());
    }

    @Test
    void exposesStoredServiceCollaborators() {
        final SegmentIndexOperationAccess<Integer, String> operationAccess =
                mock(SegmentIndexOperationAccess.class);
        final MaintenanceService maintenance = mock(MaintenanceService.class);
        final IndexRuntimeMonitoring runtimeMonitoring = mock(IndexRuntimeMonitoring.class);
        final RuntimeTuning runtimeConfiguration = mock(RuntimeTuning.class);

        final SegmentIndexRuntimeServices<Integer, String> state =
                new SegmentIndexRuntimeServices<>(operationAccess, maintenance,
                        runtimeMonitoring, runtimeConfiguration);

        assertSame(operationAccess, state.operationAccess());
        assertSame(maintenance, state.maintenance());
        assertSame(runtimeMonitoring, state.runtimeMonitoring());
        assertSame(runtimeConfiguration, state.runtimeTuning());
    }
}
