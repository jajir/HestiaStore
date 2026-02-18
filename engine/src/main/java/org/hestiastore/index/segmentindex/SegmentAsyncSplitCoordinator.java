package org.hestiastore.index.segmentindex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * Schedules segment splits on the async maintenance queue.
 */
final class SegmentAsyncSplitCoordinator<K, V> {

    private final SegmentSplitCoordinator<K, V> splitCoordinator;
    private final Executor splitExecutor;
    private final Map<SegmentId, SplitInFlight<K, V>> inFlightSplits = new ConcurrentHashMap<>();
    private final LongAdder scheduledCount = new LongAdder();
    private final LongAdder completedCount = new LongAdder();

    SegmentAsyncSplitCoordinator(
            final SegmentSplitCoordinator<K, V> splitCoordinator,
            final Executor splitExecutor) {
        this.splitCoordinator = Vldtn.requireNonNull(splitCoordinator,
                "splitCoordinator");
        this.splitExecutor = Vldtn.requireNonNull(splitExecutor,
                "splitExecutor");
    }

    void awaitCompletionIfInFlight(final SegmentId segmentId,
            final long timeoutMillis) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        if (timeoutMillis <= 0) {
            return;
        }
        final SplitInFlight<K, V> inFlight = inFlightSplits.get(segmentId);
        if (inFlight == null) {
            return;
        }
        inFlight.handle.awaitCompletion(timeoutMillis);
    }

    void awaitAllCompletions(final long timeoutMillis) {
        if (timeoutMillis <= 0) {
            return;
        }
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            final List<SplitInFlight<K, V>> snapshot = new ArrayList<>(
                    inFlightSplits.values());
            if (snapshot.isEmpty()) {
                return;
            }
            for (final SplitInFlight<K, V> inFlight : snapshot) {
                final long remainingMillis = TimeUnit.NANOSECONDS
                        .toMillis(deadline - System.nanoTime());
                if (remainingMillis <= 0) {
                    throw new IndexException(String.format(
                            "Split completion timed out after %d ms.",
                            timeoutMillis));
                }
                inFlight.handle.awaitCompletion(remainingMillis);
            }
        }
    }

    SplitHandle optionallySplitAsync(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment) {
        Vldtn.requireNonNull(segment, "segment");
        final SegmentId segmentId = segment.getId();
        final SplitInFlight<K, V> inFlight = inFlightSplits.compute(segmentId,
                (id, existing) -> {
                    // Keep a single in-flight split per segment id so
                    // awaitAllCompletions
                    // observes all running work and callers do not race on
                    // stale instances.
                    if (existing != null) {
                        return existing;
                    }
                    scheduledCount.increment();
                    return scheduleSplit(segment, maxNumberOfKeysInSegment,
                            segmentId);
                });
        return inFlight.handle;
    }

    private SplitInFlight<K, V> scheduleSplit(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment, final SegmentId segmentId) {
        final SplitHandle handle = new SplitHandle(segmentId);
        final SplitInFlight<K, V> inFlight = new SplitInFlight<>(handle);
        try {
            splitExecutor.execute(() -> {
                handle.markStarted();
                try {
                    final boolean split = splitCoordinator
                            .optionallySplit(segment, maxNumberOfKeysInSegment);
                    handle.complete(split);
                } catch (final Throwable t) {
                    handle.completeExceptionally(t);
                } finally {
                    completedCount.increment();
                    inFlightSplits.remove(segmentId, inFlight);
                }
            });
        } catch (final RuntimeException e) {
            handle.completeExceptionally(e);
            throw new IndexException(String.format(
                    "Split scheduling failed for segment '%s'.", segmentId), e);
        }
        return inFlight;
    }

    int inFlightCount() {
        return inFlightSplits.size();
    }

    long scheduledCount() {
        return scheduledCount.sum();
    }

    static final class SplitHandle {
        private final SegmentId segmentId;
        private final CompletableFuture<Void> started = new CompletableFuture<>();
        private final CompletableFuture<Boolean> completion = new CompletableFuture<>();

        private SplitHandle(final SegmentId segmentId) {
            this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
        }

        void awaitStarted(final long timeoutMillis) {
            try {
                started.get(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IndexException(String.format(
                        "Split scheduling interrupted for segment '%s'.",
                        segmentId), e);
            } catch (final TimeoutException e) {
                throw new IndexException(String.format(
                        "Split scheduling timed out after %d ms for segment '%s'.",
                        timeoutMillis, segmentId), e);
            } catch (final ExecutionException e) {
                throw new IndexException(String.format(
                        "Split scheduling failed for segment '%s'.", segmentId),
                        e.getCause());
            }
        }

        void awaitCompletion(final long timeoutMillis) {
            try {
                completion.get(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IndexException(String.format(
                        "Split completion interrupted for segment '%s'.",
                        segmentId), e);
            } catch (final TimeoutException e) {
                throw new IndexException(String.format(
                        "Split completion timed out after %d ms for segment '%s'.",
                        timeoutMillis, segmentId), e);
            } catch (final ExecutionException e) {
                throw new IndexException(String.format(
                        "Split completion failed for segment '%s'.", segmentId),
                        e.getCause());
            }
        }

        CompletionStage<Boolean> completion() {
            return completion;
        }

        private void markStarted() {
            started.complete(null);
        }

        private void complete(final boolean split) {
            completion.complete(split);
        }

        private void completeExceptionally(final Throwable t) {
            started.completeExceptionally(t);
            completion.completeExceptionally(t);
        }
    }

    private static final class SplitInFlight<K, V> {
        private final SplitHandle handle;

        private SplitInFlight(final SplitHandle handle) {
            this.handle = handle;
        }
    }
}
