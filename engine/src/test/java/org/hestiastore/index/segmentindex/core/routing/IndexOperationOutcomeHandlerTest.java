package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.OperationResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.splitplanner.SplitPlanner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexOperationOutcomeHandlerTest {

    @Mock
    private IndexWalCoordinator<Integer, String> walCoordinator;

    @Mock
    private SplitPlanner<Integer, String> splitPlanner;

    @Test
    void finishWriteRecordsAppliedWalLsnOnSuccess() {
        final IndexOperationOutcomeHandler<Integer, String> handler =
                new IndexOperationOutcomeHandler<>(new Stats(), walCoordinator,
                        splitPlanner);

        handler.finishWrite("put", OperationResult.ok(SegmentId.of(7)), 17L,
                System.nanoTime());

        verify(walCoordinator).recordAppliedLsn(17L);
        verify(splitPlanner).hintSegment(SegmentId.of(7));
    }

    @Test
    void finishWriteThrowsForFailedResult() {
        final IndexOperationOutcomeHandler<Integer, String> handler =
                new IndexOperationOutcomeHandler<>(new Stats(), walCoordinator,
                        splitPlanner);

        final IndexException thrown = assertThrows(IndexException.class,
                () -> handler.finishWrite("put", OperationResult.<SegmentId>error(),
                        17L,
                        System.nanoTime()));

        assertEquals("Index operation 'put' failed: ERROR",
                thrown.getMessage());
    }

    @Test
    void finishReadReturnsValueWhenResultIsOk() {
        final IndexOperationOutcomeHandler<Integer, String> handler =
                new IndexOperationOutcomeHandler<>(new Stats(), walCoordinator,
                        splitPlanner);

        assertEquals("one",
                handler.finishRead("get", OperationResult.ok("one"),
                        System.nanoTime()));
    }
}
