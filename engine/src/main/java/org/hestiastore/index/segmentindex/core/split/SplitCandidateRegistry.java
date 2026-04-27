package org.hestiastore.index.segmentindex.core.split;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.segment.SegmentId;

/**
 * Deduplicates split candidates and exposes a ready queue for worker
 * processing.
 */
final class SplitCandidateRegistry {

    private final ConcurrentHashMap<SegmentId, CandidateState> states =
            new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<SegmentId> readyQueue =
            new LinkedBlockingQueue<>();

    boolean offer(final SegmentId segmentId) {
        if (segmentId == null) {
            return false;
        }
        final CandidateState previous = states.putIfAbsent(segmentId,
                CandidateState.QUEUED);
        if (previous != null) {
            return false;
        }
        final boolean queued = readyQueue.offer(segmentId);
        if (!queued) {
            states.remove(segmentId, CandidateState.QUEUED);
        }
        return queued;
    }

    Optional<SegmentId> claimNextCandidate() {
        while (true) {
            final SegmentId segmentId = readyQueue.poll();
            if (segmentId == null) {
                return Optional.empty();
            }
            if (states.replace(segmentId, CandidateState.QUEUED,
                    CandidateState.IN_PROCESS)) {
                return Optional.of(segmentId);
            }
        }
    }

    Optional<SegmentId> claimNextCandidate(final long timeout,
            final TimeUnit timeUnit) throws InterruptedException {
        while (true) {
            final SegmentId segmentId = readyQueue.poll(timeout, timeUnit);
            if (segmentId == null) {
                return Optional.empty();
            }
            if (states.replace(segmentId, CandidateState.QUEUED,
                    CandidateState.IN_PROCESS)) {
                return Optional.of(segmentId);
            }
        }
    }

    void markFinished(final SegmentId segmentId) {
        if (segmentId != null) {
            states.remove(segmentId);
        }
    }

    boolean hasPendingCandidates() {
        return !states.isEmpty();
    }

    boolean contains(final SegmentId segmentId) {
        return states.containsKey(segmentId);
    }

    void clear() {
        states.clear();
        readyQueue.clear();
    }

    enum CandidateState {
        QUEUED,
        IN_PROCESS
    }
}
