package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexCoreStorageTest {

    @Mock
    private RuntimeTuningState runtimeTuningState;

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private ChunkStoreCache<Integer, String> chunkStoreCache;

    @Mock
    private IndexRetryPolicy retryPolicy;

    @Test
    void constructorRejectsNullRuntimeTuningState() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexCoreStorage<>(null, keyToSegmentMap,
                        segmentRegistry, chunkStoreCache, retryPolicy));

        assertEquals("Property 'runtimeTuningState' must not be null.",
                ex.getMessage());
    }

    @Test
    void exposesStoredCoreStorageCollaborators() {
        final SegmentIndexCoreStorage<Integer, String> coreStorage =
                newCoreStorage();

        assertSame(runtimeTuningState, coreStorage.runtimeTuningState());
        assertSame(keyToSegmentMap, coreStorage.keyToSegmentMap());
        assertSame(segmentRegistry, coreStorage.segmentRegistry());
        assertSame(chunkStoreCache, coreStorage.chunkStoreCache());
        assertSame(retryPolicy, coreStorage.retryPolicy());
    }

    @Test
    void close_closesSegmentRegistryBeforeKeyToSegmentMap() {
        when(keyToSegmentMap.wasClosed()).thenReturn(false);
        final SegmentIndexCoreStorage<Integer, String> coreStorage =
                newCoreStorage();

        coreStorage.close();

        final InOrder inOrder = inOrder(segmentRegistry, keyToSegmentMap);
        inOrder.verify(segmentRegistry).close();
        inOrder.verify(keyToSegmentMap).wasClosed();
        inOrder.verify(keyToSegmentMap).close();
    }

    @Test
    void close_skipsAlreadyClosedKeyToSegmentMap() {
        when(keyToSegmentMap.wasClosed()).thenReturn(true);
        final SegmentIndexCoreStorage<Integer, String> coreStorage =
                newCoreStorage();

        coreStorage.close();

        verify(segmentRegistry).close();
        verify(keyToSegmentMap, never()).close();
    }

    @Test
    void close_suppressesKeyToSegmentMapFailureAfterSegmentRegistryFailure() {
        final RuntimeException firstFailure =
                new IllegalStateException("registry failed");
        final RuntimeException secondFailure =
                new IllegalStateException("map failed");
        doThrow(firstFailure).when(segmentRegistry).close();
        when(keyToSegmentMap.wasClosed()).thenReturn(false);
        doThrow(secondFailure).when(keyToSegmentMap).close();
        final SegmentIndexCoreStorage<Integer, String> coreStorage =
                newCoreStorage();

        final RuntimeException thrown = assertThrows(RuntimeException.class,
                coreStorage::close);

        assertSame(firstFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(secondFailure, thrown.getSuppressed()[0]);
    }

    private SegmentIndexCoreStorage<Integer, String> newCoreStorage() {
        return new SegmentIndexCoreStorage<>(runtimeTuningState,
                keyToSegmentMap, segmentRegistry, chunkStoreCache,
                retryPolicy);
    }
}
