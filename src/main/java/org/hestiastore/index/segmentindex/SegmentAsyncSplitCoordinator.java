package org.hestiastore.index.segmentindex;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * Schedules segment splits on the async maintenance queue.
 */
final class SegmentAsyncSplitCoordinator<K, V> {

    private final SegmentSplitCoordinator<K, V> splitCoordinator;
    private final Executor maintenanceExecutor;
    private final Map<SegmentId, SplitInFlight<K, V>> inFlightSplits = new ConcurrentHashMap<>();

    SegmentAsyncSplitCoordinator(final IndexConfiguration<K, V> conf,
            final KeySegmentCache<K> keySegmentCache,
            final SegmentRegistry<K, V> segmentRegistry,
            final Executor maintenanceExecutor) {
        this(new SegmentSplitCoordinator<>(conf, keySegmentCache,
                segmentRegistry), maintenanceExecutor);
    }

    SegmentAsyncSplitCoordinator(
            final SegmentSplitCoordinator<K, V> splitCoordinator,
            final Executor maintenanceExecutor) {
        this.splitCoordinator = Vldtn.requireNonNull(splitCoordinator,
                "splitCoordinator");
        this.maintenanceExecutor = Vldtn.requireNonNull(maintenanceExecutor,
                "maintenanceExecutor");
    }

    CompletionStage<Boolean> optionallySplitAsync(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment) {
        Vldtn.requireNonNull(segment, "segment");
        final SegmentId segmentId = segment.getId();
        final SplitInFlight<K, V> inFlight = inFlightSplits.compute(segmentId,
                (id, existing) -> {
                    if (existing != null && existing.segment == segment) {
                        return existing;
                    }
                    return scheduleSplit(segment, maxNumberOfKeysInSegment,
                            segmentId);
                });
        return inFlight.future;
    }

    private SplitInFlight<K, V> scheduleSplit(
            final Segment<K, V> segment, final long maxNumberOfKeysInSegment,
            final SegmentId segmentId) {
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        final SplitInFlight<K, V> inFlight = new SplitInFlight<>(segment,
                future);
        try {
            maintenanceExecutor.execute(() -> {
                try {
                    final boolean split = splitCoordinator.optionallySplit(
                            segment, maxNumberOfKeysInSegment);
                    future.complete(split);
                } catch (final Throwable t) {
                    future.completeExceptionally(t);
                } finally {
                    inFlightSplits.remove(segmentId, inFlight);
                }
            });
        } catch (final Throwable t) {
            inFlightSplits.remove(segmentId, inFlight);
            future.completeExceptionally(t);
        }
        return inFlight;
    }

    private static final class SplitInFlight<K, V> {
        private final Segment<K, V> segment;
        private final CompletableFuture<Boolean> future;

        private SplitInFlight(final Segment<K, V> segment,
                final CompletableFuture<Boolean> future) {
            this.segment = segment;
            this.future = future;
        }
    }
}
