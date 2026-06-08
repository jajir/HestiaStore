package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CoreStorageRuntimeTest {

    @Test
    @SuppressWarnings("unchecked")
    void constructorStoresCollaborators() {
        final RuntimeTuningState runtimeTuningState =
                mock(RuntimeTuningState.class);
        final StorageService<Integer, String> storageService =
                mock(StorageService.class);

        final CoreStorageRuntime<Integer, String> runtime =
                new CoreStorageRuntime<>(runtimeTuningState, storageService);

        assertSame(runtimeTuningState, runtime.getRuntimeTuningState());
        assertSame(storageService, runtime.getStorageService());
    }
}
