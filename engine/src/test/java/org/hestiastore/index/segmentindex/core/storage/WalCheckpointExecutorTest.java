package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WalCheckpointExecutorTest {

    @Mock
    private WalRuntime<Integer, String> walRuntime;

    @Test
    void checkpoint_routesRuntimeFailureThroughTransitionHandler() {
        final IndexException failure = new IndexException("sync failure");
        final java.util.concurrent.atomic.AtomicReference<RuntimeException> handledFailure =
                new java.util.concurrent.atomic.AtomicReference<>();
        final WalCheckpointExecutor<Integer, String> executor =
                new WalCheckpointExecutor<>(walRuntime,
                        new AtomicLong(),
                        new WalFailureTransitionHandler(walRuntime,
                                () -> SegmentIndexState.READY,
                                handledFailure::set));
        when(walRuntime.hasSyncFailure()).thenReturn(true);
        doThrow(failure).when(walRuntime).onCheckpoint(0L);

        final IndexException thrown = assertThrows(IndexException.class,
                executor::checkpoint);

        assertEquals(failure, thrown);
        assertEquals(failure, handledFailure.get());
    }

    @Test
    void checkpointInternal_appliesLastAppliedLsn() {
        final WalCheckpointExecutor<Integer, String> executor =
                new WalCheckpointExecutor<>(walRuntime,
                        new AtomicLong(9L),
                        new WalFailureTransitionHandler(walRuntime,
                                () -> SegmentIndexState.READY, failure -> {
                                }));

        executor.checkpointInternal();

        verify(walRuntime).onCheckpoint(9L);
    }
}
