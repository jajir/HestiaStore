package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WalFailureTransitionHandlerTest {

    @Mock
    private WalRuntime<Integer, String> walRuntime;

    @Test
    void propagateRoutesWalSyncFailureToErrorHandler() {
        final AtomicReference<RuntimeException> handledFailure =
                new AtomicReference<>();
        final WalFailureTransitionHandler handler =
                new WalFailureTransitionHandler(walRuntime,
                        () -> SegmentIndexState.READY, handledFailure::set);
        final IndexException failure = new IndexException("sync failure");
        when(walRuntime.isEnabled()).thenReturn(true);
        when(walRuntime.hasSyncFailure()).thenReturn(true);

        assertSame(failure, handler.propagate(failure));
        assertSame(failure, handledFailure.get());
    }

    @Test
    void propagateIgnoresClosedState() {
        final WalFailureTransitionHandler handler =
                new WalFailureTransitionHandler(walRuntime,
                        () -> SegmentIndexState.CLOSED, failure -> {
                        });
        final IndexException failure = new IndexException("sync failure");
        when(walRuntime.isEnabled()).thenReturn(true);
        when(walRuntime.hasSyncFailure()).thenReturn(true);

        assertSame(failure, handler.propagate(failure));
    }
}
