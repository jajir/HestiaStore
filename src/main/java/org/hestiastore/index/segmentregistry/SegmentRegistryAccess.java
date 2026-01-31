package org.hestiastore.index.segmentregistry;

import java.util.Optional;

/**
 * Access wrapper that exposes registry status, optional value, and
 * lock/unlock operations when supported by the underlying source.
 *
 * @param <T> value type returned from the registry
 */
public interface SegmentRegistryAccess<T> {

    /**
     * Returns the registry status for this access result.
     *
     * @return status for the registry operation
     */
    SegmentRegistryResultStatus getSegmentStatus();

    /**
     * Attempts to lock the underlying handler when available.
     *
     * @return lock status indicating success or BUSY
     */
    SegmentHandlerLockStatus lock();

    /**
     * Releases the lock if one was obtained.
     */
    void unlock();

    /**
     * Returns the value when status is OK; otherwise empty.
     *
     * @return optional value for this access result
     */
    Optional<T> getSegment();
}
