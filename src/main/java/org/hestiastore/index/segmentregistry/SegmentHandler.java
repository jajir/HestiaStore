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

    /**
     * Creates a handler for the provided segment instance.
     *
     * @param segment segment instance to protect
     */
    public SegmentHandler(final Segment<K, V> segment) {
        this.segment = Vldtn.requireNonNull(segment, "segment");
    }

    /**
     * Returns the current handler state.
     *
     * @return handler state
     */
    SegmentHandlerState getState() {
        return state.get();
    }

    /**
     * Returns the wrapped segment instance.
     *
     * @return segment instance
     */
    public Segment<K, V> getSegment() {
        return segment;
    }

    /**
     * Attempts to lock the handler for exclusive use.
     *
     * @return lock status indicating success or BUSY
     */
    public SegmentHandlerLockStatus lock() {
        if (state.compareAndSet(SegmentHandlerState.READY,
                SegmentHandlerState.LOCKED)) {
            return SegmentHandlerLockStatus.OK;
        }
        return SegmentHandlerLockStatus.BUSY;
    }

    /**
     * Releases the handler lock.
     *
     * @throws IllegalStateException if the handler was not locked
     */
    public void unlock() {
        if (!state.compareAndSet(SegmentHandlerState.LOCKED,
                SegmentHandlerState.READY)) {
            throw new IllegalStateException("Segment handler was not locked.");
        }
    }
}
