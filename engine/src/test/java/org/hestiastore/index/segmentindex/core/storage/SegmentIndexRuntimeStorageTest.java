package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class SegmentIndexRuntimeStorageTest {

    @Test
    void constructorRejectsNullRuntimeTuningState() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexRuntimeStorage<>(null,
                        mock(KeyToSegmentMap.class),
                        mock(SegmentRegistry.class),
                        mock(IndexRetryPolicy.class)));

        assertEquals("Property 'runtimeTuningState' must not be null.",
                ex.getMessage());
    }

    @Test
    void exposesStoredStorageCollaborators() {
        final RuntimeTuningState runtimeTuningState = mock(
                RuntimeTuningState.class);
        final KeyToSegmentMap<Integer> keyToSegmentMap = mock(
                KeyToSegmentMap.class);
        final SegmentRegistry<Integer, String> segmentRegistry = mock(
                SegmentRegistry.class);
        final IndexRetryPolicy retryPolicy = mock(IndexRetryPolicy.class);

        final SegmentIndexRuntimeStorage<Integer, String> state =
                new SegmentIndexRuntimeStorage<>(runtimeTuningState,
                        keyToSegmentMap, segmentRegistry, retryPolicy);

        assertSame(runtimeTuningState, state.runtimeTuningState());
        assertSame(keyToSegmentMap, state.keyToSegmentMap());
        assertSame(segmentRegistry, state.segmentRegistry());
        assertSame(retryPolicy, state.retryPolicy());
    }
}
