package org.hestiastore.index.segmentindex.core.splitplanner;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import org.hestiastore.index.segment.SegmentId;

/**
 * Mutable state owned by the dedicated split planner loop.
 */
final class SplitPlannerState {

    private final Object monitor = new Object();
    private final LinkedHashSet<SegmentId> hintedSegments =
            new LinkedHashSet<>();
    private boolean rescanRequested;
    private boolean forceRetryRequested;
    private boolean plannerActive;

    void hintSegment(final SegmentId segmentId) {
        synchronized (monitor) {
            hintedSegments.add(segmentId);
            monitor.notifyAll();
        }
    }

    void requestRescan() {
        synchronized (monitor) {
            rescanRequested = true;
            monitor.notifyAll();
        }
    }

    void requestForceRetry() {
        synchronized (monitor) {
            forceRetryRequested = true;
            monitor.notifyAll();
        }
    }

    PlannerCycle awaitNextCycle(final long intervalMillis,
            final java.util.function.BooleanSupplier stopRequested) {
        synchronized (monitor) {
            while (true) {
                if (hasPendingWork()) {
                    return startCycle(false);
                }
                if (stopRequested.getAsBoolean()) {
                    return PlannerCycle.shutdown();
                }
                try {
                    monitor.wait(intervalMillis);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return PlannerCycle.shutdown();
                }
                if (hasPendingWork()) {
                    return startCycle(false);
                }
                return startCycle(true);
            }
        }
    }

    void finishCycle() {
        synchronized (monitor) {
            plannerActive = false;
            monitor.notifyAll();
        }
    }

    void clearPendingWork() {
        synchronized (monitor) {
            hintedSegments.clear();
            rescanRequested = false;
            forceRetryRequested = false;
            monitor.notifyAll();
        }
    }

    boolean isSettled() {
        synchronized (monitor) {
            return !plannerActive && !rescanRequested && !forceRetryRequested
                    && hintedSegments.isEmpty();
        }
    }

    boolean hasPendingHints() {
        synchronized (monitor) {
            return !hintedSegments.isEmpty();
        }
    }

    private boolean hasPendingWork() {
        return rescanRequested || forceRetryRequested || !hintedSegments.isEmpty();
    }

    private PlannerCycle startCycle(final boolean periodicTick) {
        plannerActive = true;
        final List<SegmentId> drainedHints = new ArrayList<>(hintedSegments);
        hintedSegments.clear();
        final boolean requestedRescan = rescanRequested;
        final boolean requestedForceRetry = forceRetryRequested;
        rescanRequested = false;
        forceRetryRequested = false;
        return new PlannerCycle(drainedHints, requestedRescan,
                requestedForceRetry, periodicTick, false);
    }

    static final class PlannerCycle {

        private final List<SegmentId> hintedSegments;
        private final boolean rescanRequested;
        private final boolean forceRetryRequested;
        private final boolean periodicTick;
        private final boolean shutdown;

        private PlannerCycle(final List<SegmentId> hintedSegments,
                final boolean rescanRequested,
                final boolean forceRetryRequested,
                final boolean periodicTick, final boolean shutdown) {
            this.hintedSegments = hintedSegments;
            this.rescanRequested = rescanRequested;
            this.forceRetryRequested = forceRetryRequested;
            this.periodicTick = periodicTick;
            this.shutdown = shutdown;
        }

        static PlannerCycle shutdown() {
            return new PlannerCycle(List.of(), false, false, false, true);
        }

        List<SegmentId> hintedSegments() {
            return hintedSegments;
        }

        boolean rescanRequested() {
            return rescanRequested;
        }

        boolean forceRetryRequested() {
            return forceRetryRequested;
        }

        boolean periodicTick() {
            return periodicTick;
        }

        boolean isShutdownCycle() {
            return shutdown;
        }
    }
}
