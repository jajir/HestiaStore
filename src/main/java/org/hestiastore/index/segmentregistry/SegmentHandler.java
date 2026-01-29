package org.hestiastore.index.segmentregistry;

import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;

/**
 * Gatekeeper for segment access with a simple lock state.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentHandler<K, V> {

    private final Segment<K, V> segment;
    private final AtomicReference<SegmentHandlerState> state = new AtomicReference<>(
            SegmentHandlerState.READY);

    public SegmentHandler(final Segment<K, V> segment) {
        this.segment = Vldtn.requireNonNull(segment, "segment");
    }

    public SegmentHandlerState getState() {
        return state.get();
    }

    /**
     * Returns the segment when the handler is ready; otherwise returns BUSY.
     *
     * @return registry result with segment or BUSY status
     */
    public SegmentRegistryResult<Segment<K, V>> getSegmentIfReady() {
        if (state.get() == SegmentHandlerState.READY) {
            return SegmentRegistryResult.ok(segment);
        }
        return SegmentRegistryResult.busy();
    }

    SegmentHandlerResult<Segment<K, V>> getSegment() {
        if (state.get() == SegmentHandlerState.READY) {
            return SegmentHandlerResult.ok(segment);
        }
        return SegmentHandlerResult.locked();
    }

    public SegmentHandlerLockStatus lock() {
        if (state.compareAndSet(SegmentHandlerState.READY,
                SegmentHandlerState.LOCKED)) {
            return SegmentHandlerLockStatus.OK;
        }
        return SegmentHandlerLockStatus.BUSY;
    }

    public void unlock() {
        if (!state.compareAndSet(SegmentHandlerState.LOCKED,
                SegmentHandlerState.READY)) {
            throw new IllegalStateException("Segment handler was not locked.");
        }
    }

    public boolean isForSegment(final Segment<K, V> candidate) {
        return segment == candidate;
    }
}
