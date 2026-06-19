package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

class StorageCoordinatorTest {

    @Test
    void createCreatesStorageService() {
        final StorageCoordinator<Integer, String> storageService = StorageCoordinator
                .create(mock(Directory.class), mockKeyToSegmentMap(),
                        mockSegmentRegistry(), maintenance());

        assertNotNull(storageService);
    }

    @Test
    void createRejectsMissingDirectoryFacade() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> StorageCoordinator.create(null, mockKeyToSegmentMap(),
                        mockSegmentRegistry(), maintenance()));

        assertEquals("Property 'directoryFacade' must not be null.",
                ex.getMessage());
    }

    private EffectiveIndexMaintenanceConfiguration maintenance() {
        final EffectiveIndexMaintenanceConfiguration maintenance = mock(
                EffectiveIndexMaintenanceConfiguration.class);
        when(maintenance.busyBackoffMillis()).thenReturn(1);
        when(maintenance.busyTimeoutMillis()).thenReturn(10);
        return maintenance;
    }

    @SuppressWarnings("unchecked")
    private SegmentRouteMap<Integer> mockKeyToSegmentMap() {
        return mock(SegmentRouteMap.class);
    }

    @SuppressWarnings("unchecked")
    private SegmentRegistry<Integer, String> mockSegmentRegistry() {
        return mock(SegmentRegistry.class);
    }
}
