package org.hestiastore.index.segmentregistry;

import java.util.Optional;

import org.hestiastore.index.segment.Segment;

/**
 * Default {@link SegmentRegistryAccess} implementation.
 *
 * @param <T> value type
 */
public final class SegmentRegistryAccessImpl<T>
        implements SegmentRegistryAccess<T> {

    private static final LockActions NO_LOCK = new LockActions() {
        @Override
        public SegmentHandlerLockStatus lock() {
            return SegmentHandlerLockStatus.BUSY;
        }

        @Override
        public void unlock() {
            // no-op for non-locking results
        }
    };

    private final SegmentRegistryResultStatus status;
    private final T value;
    private final LockActions lockActions;

    private SegmentRegistryAccessImpl(final SegmentRegistryResultStatus status,
            final T value, final LockActions lockActions) {
        this.status = status;
        this.value = value;
        this.lockActions = lockActions;
    }

    /**
     * Creates a non-value access wrapper for the provided status.
     *
     * @param status registry status
     * @param <T> value type
     * @return status-only access wrapper
     */
    public static <T> SegmentRegistryAccess<T> forStatus(
            final SegmentRegistryResultStatus status) {
        return new SegmentRegistryAccessImpl<>(status, null, NO_LOCK);
    }

    /**
     * Creates an access wrapper carrying a value and status.
     *
     * @param status registry status
     * @param value wrapped value
     * @param <T> value type
     * @return access wrapper
     */
    public static <T> SegmentRegistryAccess<T> forValue(
            final SegmentRegistryResultStatus status, final T value) {
        return new SegmentRegistryAccessImpl<>(status, value, NO_LOCK);
    }

    static <K, V> SegmentRegistryAccess<Segment<K, V>> forHandler(
            final SegmentRegistryResultStatus status,
            final SegmentHandler<K, V> handler) {
        return new SegmentRegistryAccessImpl<>(status,
                handler == null ? null : handler.getSegment(),
                handler == null ? NO_LOCK
                        : new HandlerLockActions<>(handler));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentRegistryResultStatus getSegmentStatus() {
        return status;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentHandlerLockStatus lock() {
        if (status != SegmentRegistryResultStatus.OK) {
            return SegmentHandlerLockStatus.BUSY;
        }
        return lockActions.lock();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unlock() {
        if (status != SegmentRegistryResultStatus.OK) {
            return;
        }
        lockActions.unlock();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<T> getSegment() {
        if (status != SegmentRegistryResultStatus.OK) {
            return Optional.empty();
        }
        return Optional.ofNullable(value);
    }

    private interface LockActions {
        SegmentHandlerLockStatus lock();

        void unlock();
    }

    private static final class HandlerLockActions<K, V> implements LockActions {
        private final SegmentHandler<K, V> handler;

        private HandlerLockActions(final SegmentHandler<K, V> handler) {
            this.handler = handler;
        }

        @Override
        public SegmentHandlerLockStatus lock() {
            return handler.lock();
        }

        @Override
        public void unlock() {
            handler.unlock();
        }
    }
}
