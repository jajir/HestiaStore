package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SplitCandidateQueueTest {

    @Test
    void offerDeduplicatesQueuedSegmentIds() {
        final SplitCandidateQueue registry = new SplitCandidateQueue();
        final SegmentId segmentId = SegmentId.of(3);

        assertTrue(registry.offer(segmentId));
        assertFalse(registry.offer(segmentId));
        assertTrue(registry.hasPendingCandidates());
    }

    @Test
    void claimNextCandidateTransitionsCandidateToInProcessAndFinishRemovesIt()
            throws InterruptedException {
        final SplitCandidateQueue registry = new SplitCandidateQueue();
        final SegmentId segmentId = SegmentId.of(8);
        registry.offer(segmentId);

        assertEquals(segmentId, registry.claimNextCandidate(1,
                TimeUnit.MILLISECONDS).orElseThrow());

        registry.markFinished(segmentId);

        assertFalse(registry.hasPendingCandidates());
        assertTrue(registry.claimNextCandidate(1, TimeUnit.MILLISECONDS)
                .isEmpty());
    }

    @Test
    void claimNextCandidateWaitsForOfferedCandidate() throws Exception {
        final SplitCandidateQueue registry = new SplitCandidateQueue();
        final SegmentId segmentId = SegmentId.of(12);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final Future<SegmentId> claimed = executor.submit(() -> registry
                    .claimNextCandidate(1, TimeUnit.SECONDS).orElseThrow());

            assertTrue(registry.offer(segmentId));
            assertEquals(segmentId, claimed.get(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }
}
