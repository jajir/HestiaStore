package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

class StorageServiceBuilderTest {

    @Test
    void buildCreatesStorageService() {
        final StorageService<Integer, String> storageService =
                newCompleteBuilder().build();

        assertNotNull(storageService);
    }

    @Test
    void buildRejectsMissingDirectoryFacade() {
        final StorageServiceBuilder<Integer, String> builder =
                StorageService.<Integer, String>builder()
                        .withKeyToSegmentMap(mockKeyToSegmentMap())
                        .withSegmentRegistry(mockSegmentRegistry())
                        .withKeyTypeDescriptor(mockKeyTypeDescriptor())
                        .withStorageCleanupBusyBackoffMillis(1)
                        .withStorageCleanupBusyTimeoutMillis(10)
                        .withWalBackpressureBusyBackoffMillis(1)
                        .withWalBackpressureBusyTimeoutMillis(10);

        final IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, builder::build);

        assertEquals("Property 'directoryFacade' must not be null.",
                ex.getMessage());
    }

    private StorageServiceBuilder<Integer, String> newCompleteBuilder() {
        return StorageService.<Integer, String>builder()
                .withDirectoryFacade(mock(Directory.class))
                .withKeyToSegmentMap(mockKeyToSegmentMap())
                .withSegmentRegistry(mockSegmentRegistry())
                .withKeyTypeDescriptor(mockKeyTypeDescriptor())
                .withStorageCleanupBusyBackoffMillis(1)
                .withStorageCleanupBusyTimeoutMillis(10)
                .withWalBackpressureBusyBackoffMillis(1)
                .withWalBackpressureBusyTimeoutMillis(10);
    }

    @SuppressWarnings("unchecked")
    private KeyToSegmentMap<Integer> mockKeyToSegmentMap() {
        return mock(KeyToSegmentMap.class);
    }

    @SuppressWarnings("unchecked")
    private SegmentRegistry<Integer, String> mockSegmentRegistry() {
        return mock(SegmentRegistry.class);
    }

    @SuppressWarnings("unchecked")
    private TypeDescriptor<Integer> mockKeyTypeDescriptor() {
        return mock(TypeDescriptor.class);
    }
}
