package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OpenedStorageRuntimeTest {

    @Mock
    private RuntimeTuningState runtimeTuningState;

    @Mock
    private StorageCoordinator<Integer, String> storageService;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private SegmentRouteMap<Integer> keyToSegmentMap;

    private OpenedStorageRuntime<Integer, String> runtime;

    @BeforeEach
    void setUp() {
        runtime = new OpenedStorageRuntime<>(runtimeTuningState, storageService,
                segmentRegistry, keyToSegmentMap);
    }

    @Test
    void constructorStoresCollaborators() {
        assertSame(runtimeTuningState, runtime.getRuntimeTuningState());
        assertSame(storageService, runtime.getStorageService());
    }

    @Test
    void closeCoreStorageClosesRegistryBeforeKeyMap() {
        when(keyToSegmentMap.wasClosed()).thenReturn(false);

        runtime.closeCoreStorage();

        final InOrder inOrder = inOrder(segmentRegistry, keyToSegmentMap);
        inOrder.verify(segmentRegistry).close();
        inOrder.verify(keyToSegmentMap).wasClosed();
        inOrder.verify(keyToSegmentMap).close();
    }

    @Test
    void closeCoreStorageSkipsAlreadyClosedKeyMap() {
        when(keyToSegmentMap.wasClosed()).thenReturn(true);

        runtime.closeCoreStorage();

        verify(segmentRegistry).close();
        verify(keyToSegmentMap).wasClosed();
        verify(keyToSegmentMap, never()).close();
    }

    @Test
    void closeCoreStorageSuppressesLaterFailure() {
        final IndexException firstFailure = new IndexException(
                "segment registry failed");
        final IndexException secondFailure = new IndexException(
                "key map failed");
        doThrow(firstFailure).when(segmentRegistry).close();
        when(keyToSegmentMap.wasClosed()).thenReturn(false);
        doThrow(secondFailure).when(keyToSegmentMap).close();

        final IndexException thrown = assertThrows(IndexException.class,
                runtime::closeCoreStorage);

        assertSame(firstFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(secondFailure, thrown.getSuppressed()[0]);
    }

    @Test
    void constructorRejectsMissingCollaborators() {
        assertThrows(IllegalArgumentException.class,
                () -> new OpenedStorageRuntime<>(null, storageService,
                        segmentRegistry, keyToSegmentMap));
        assertThrows(IllegalArgumentException.class,
                () -> new OpenedStorageRuntime<>(runtimeTuningState, null,
                        segmentRegistry, keyToSegmentMap));
        assertThrows(IllegalArgumentException.class,
                () -> new OpenedStorageRuntime<>(runtimeTuningState,
                        storageService, null, keyToSegmentMap));
        assertThrows(IllegalArgumentException.class,
                () -> new OpenedStorageRuntime<>(runtimeTuningState,
                        storageService, segmentRegistry, null));
    }
}
