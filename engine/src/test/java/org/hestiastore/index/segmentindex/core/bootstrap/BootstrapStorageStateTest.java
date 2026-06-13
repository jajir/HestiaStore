package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapStorageStateTest {

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;

    @Mock
    private ChunkStoreCache<Integer, String> chunkStoreCache;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private RuntimeTuningState runtimeTuningState;

    @Mock
    private StorageService<Integer, String> storageService;

    private BootstrapStorageState<Integer, String> state;

    @BeforeEach
    void setUp() {
        state = new BootstrapStorageState<>();
    }

    @Test
    void storesStorageCollaborators() {
        final CoreStorageRuntime<Integer, String> coreStorageRuntime =
                new CoreStorageRuntime<>(runtimeTuningState, storageService,
                        segmentRegistry, keyToSegmentMap);

        state.setKeyToSegmentMap(keyToSegmentMap);
        state.setChunkStoreCache(chunkStoreCache);
        state.setSegmentRegistry(segmentRegistry);
        state.setCoreStorageRuntime(coreStorageRuntime);

        assertTrue(state.hasKeyToSegmentMap());
        assertTrue(state.hasCoreStorage());
        assertSame(keyToSegmentMap, state.getKeyToSegmentMap());
        assertSame(chunkStoreCache, state.getChunkStoreCache());
        assertSame(segmentRegistry, state.getSegmentRegistry());
        assertSame(coreStorageRuntime, state.getCoreStorageRuntime());
        assertSame(runtimeTuningState, state.getRuntimeTuningState());
        assertSame(storageService, state.getStorageService());
    }
}
