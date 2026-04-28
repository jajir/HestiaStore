package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class SegmentIndexCoreStorageTest {

    @Test
    void constructorRejectsNullRuntimeTuningState() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexCoreStorage<>(null,
                        mock(KeyToSegmentMap.class),
                        mock(SegmentRegistry.class),
                        mock(IndexRetryPolicy.class)));

        assertEquals("Property 'runtimeTuningState' must not be null.",
                ex.getMessage());
    }

    @Test
    void exposesStoredCoreStorageCollaborators() {
        final RuntimeTuningState runtimeTuningState = mock(
                RuntimeTuningState.class);
        final KeyToSegmentMap<Integer> keyToSegmentMap = mock(
                KeyToSegmentMap.class);
        final SegmentRegistry<Integer, String> segmentRegistry = mock(
                SegmentRegistry.class);
        final IndexRetryPolicy retryPolicy = mock(IndexRetryPolicy.class);

        final SegmentIndexCoreStorage<Integer, String> coreStorage =
                new SegmentIndexCoreStorage<>(runtimeTuningState,
                        keyToSegmentMap, segmentRegistry, retryPolicy);

        assertSame(runtimeTuningState, coreStorage.runtimeTuningState());
        assertSame(keyToSegmentMap, coreStorage.keyToSegmentMap());
        assertSame(segmentRegistry, coreStorage.segmentRegistry());
        assertSame(retryPolicy, coreStorage.retryPolicy());
    }
}
