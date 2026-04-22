package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexOperationOutcomeHandlerTest {

    @Mock
    private IndexWalCoordinator<Integer, String> walCoordinator;

    @Test
    void finishWriteRecordsAppliedWalLsnOnSuccess() {
        final IndexOperationOutcomeHandler<Integer, String> handler =
                new IndexOperationOutcomeHandler<>(new Stats(), walCoordinator);

        handler.finishWrite("put", IndexResult.ok(), 17L, System.nanoTime());

        verify(walCoordinator).recordAppliedLsn(17L);
    }

    @Test
    void finishWriteThrowsForFailedResult() {
        final IndexOperationOutcomeHandler<Integer, String> handler =
                new IndexOperationOutcomeHandler<>(new Stats(), walCoordinator);

        final IndexException thrown = assertThrows(IndexException.class,
                () -> handler.finishWrite("put", IndexResult.error(), 17L,
                        System.nanoTime()));

        assertEquals("Index operation 'put' failed: ERROR",
                thrown.getMessage());
    }

    @Test
    void finishReadReturnsValueWhenResultIsOk() {
        final IndexOperationOutcomeHandler<Integer, String> handler =
                new IndexOperationOutcomeHandler<>(new Stats(), walCoordinator);

        assertEquals("one",
                handler.finishRead("get", IndexResult.ok("one"),
                        System.nanoTime()));
    }
}
