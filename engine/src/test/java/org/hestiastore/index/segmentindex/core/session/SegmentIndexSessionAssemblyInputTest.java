package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class SegmentIndexSessionAssemblyInputTest {

    @Test
    void constructor_rejectsMissingOperationAccess() {
        assertThrows(IllegalArgumentException.class,
                () -> newInput(null,
                        mock(SegmentTopologyRuntimeAccess.class),
                        mock(MaintenanceService.class),
                        mock(RuntimeTuning.class),
                        mock(IndexRuntimeMonitoring.class),
                        newCoreStorageRuntime(),
                        mock(StorageService.class)));
    }

    @Test
    void exposesAssemblyCollaborators() {
        final SegmentIndexOperationAccess<Integer, String> operationAccess =
                mock(SegmentIndexOperationAccess.class);
        final SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime =
                mock(SegmentTopologyRuntimeAccess.class);
        final MaintenanceService maintenance = mock(MaintenanceService.class);
        final RuntimeTuning runtimeTuning = mock(RuntimeTuning.class);
        final IndexRuntimeMonitoring runtimeMonitoring =
                mock(IndexRuntimeMonitoring.class);
        final CoreStorageRuntime<Integer, String> coreStorageRuntime =
                newCoreStorageRuntime();
        final StorageService<Integer, String> storageService =
                mock(StorageService.class);

        final SegmentIndexSessionAssemblyInput<Integer, String> input =
                newInput(operationAccess, topologyRuntime, maintenance,
                        runtimeTuning, runtimeMonitoring, coreStorageRuntime,
                        storageService);

        assertSame(operationAccess, input.operationAccess());
        assertSame(topologyRuntime, input.topologyRuntime());
        assertSame(maintenance, input.maintenance());
        assertSame(runtimeTuning, input.runtimeTuning());
        assertSame(runtimeMonitoring, input.runtimeMonitoring());
        assertSame(coreStorageRuntime, input.coreStorageRuntime());
        assertSame(storageService, input.storageService());
    }

    private SegmentIndexSessionAssemblyInput<Integer, String> newInput(
            final SegmentIndexOperationAccess<Integer, String> operationAccess,
            final SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime,
            final MaintenanceService maintenance,
            final RuntimeTuning runtimeTuning,
            final IndexRuntimeMonitoring runtimeMonitoring,
            final CoreStorageRuntime<Integer, String> coreStorageRuntime,
            final StorageService<Integer, String> storageService) {
        return new SegmentIndexSessionAssemblyInput<>(
                operationAccess,
                topologyRuntime,
                maintenance,
                runtimeTuning,
                runtimeMonitoring,
                coreStorageRuntime,
                storageService);
    }

    private CoreStorageRuntime<Integer, String> newCoreStorageRuntime() {
        return new CoreStorageRuntime<>(
                mock(RuntimeTuningState.class),
                mock(StorageService.class),
                mock(SegmentRegistry.class),
                mock(KeyToSegmentMap.class));
    }
}
