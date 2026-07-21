package org.hestiastore.index.segmentregistry;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.Segment;

/**
 * Coordinates one registry cache entry through load, ready, and unload states.
 * READY reads are lock-free; the volatile state transition safely publishes
 * the value written before it. Lifecycle transitions remain serialized by the
 * entry lock.
 *
 * @param <K> segment key type
 * @param <V> segment value type
 */
final class SegmentRegistryEntry<K, V> {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition ready = lock.newCondition();
    private volatile long accessCx;
    private volatile EntryState state = EntryState.LOADING;
    private Segment<K, V> value;
    private IndexException failure;

    /**
     * Creates an entry with the initial access sequence used for eviction
     * ordering.
     *
     * @param accessCx access sequence
     */
    SegmentRegistryEntry(final long accessCx) {
        this.accessCx = accessCx;
    }

    /**
     * Returns whether this newly created entry may perform the segment load.
     *
     * @return true when loading can start
     */
    boolean tryStartLoad() {
        lock.lock();
        try {
            return state == EntryState.LOADING && value == null
                    && failure == null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits for an in-progress load and returns the ready value.
     *
     * @param currentAccessCx access sequence for recency tracking
     * @return loaded value when ready, otherwise null when unavailable
     */
    Segment<K, V> waitWhileLoading(final long currentAccessCx) {
        final Segment<K, V> readyValue = getReadyValue();
        if (readyValue != null) {
            accessCx = currentAccessCx;
            return readyValue;
        }
        lock.lock();
        try {
            while (state == EntryState.LOADING) {
                if (failure != null) {
                    throw failure;
                }
                try {
                    ready.await();
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IndexException(
                            "Interrupted while waiting for cache entry", ex);
                }
            }
            if (failure != null) {
                throw failure;
            }
            if (state == EntryState.READY) {
                accessCx = currentAccessCx;
                return value;
            }
            if (state == EntryState.UNLOADING) {
                throw new SegmentBusyException();
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Marks this entry ready with its loaded value.
     *
     * @param value loaded value
     */
    void finishLoad(final Segment<K, V> value) {
        lock.lock();
        try {
            if (state != EntryState.LOADING || this.value != null
                    || failure != null) {
                throw new IndexException(
                        "Invalid transition to READY from " + state);
            }
            this.value = value;
            this.state = EntryState.READY;
            ready.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Records a load failure and releases waiting readers.
     *
     * @param failure load failure
     */
    void fail(final IndexException failure) {
        lock.lock();
        try {
            if (state != EntryState.LOADING || this.value != null
                    || this.failure != null) {
                throw new IndexException("Invalid fail transition from "
                        + state);
            }
            this.failure = failure;
            ready.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Cancels a still-loading entry and wakes waiters.
     */
    void cancelLoadIfNeeded() {
        lock.lock();
        try {
            if (state == EntryState.LOADING && failure == null) {
                failure = new SegmentBusyException();
                ready.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns this entry's eviction order when the expected value is still
     * ready.
     *
     * @param expectedValue value that must still be present
     * @return access sequence, or {@link Long#MAX_VALUE} when not evictable
     */
    long getEvictionOrder(final Segment<K, V> expectedValue) {
        lock.lock();
        try {
            if (state != EntryState.READY || value == null) {
                return Long.MAX_VALUE;
            }
            if (value != expectedValue) {
                return Long.MAX_VALUE;
            }
            return accessCx;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Starts unloading when this entry is ready and still holds the expected
     * value.
     *
     * @param expectedValue value that must still be present
     * @return true when unload ownership was acquired
     */
    boolean tryStartUnload(final Segment<K, V> expectedValue) {
        lock.lock();
        try {
            if (state != EntryState.READY || value == null) {
                return false;
            }
            if (value != expectedValue) {
                return false;
            }
            state = EntryState.UNLOADING;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the value currently owned by an unload operation.
     *
     * @return value being unloaded, or null when unload is not active
     */
    Segment<K, V> getValueForUnload() {
        lock.lock();
        try {
            if (state != EntryState.UNLOADING) {
                return null;
            }
            return value;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Completes an unload and wakes waiting readers.
     */
    void finishUnload() {
        lock.lock();
        try {
            if (state != EntryState.UNLOADING) {
                throw new IndexException(
                        "Invalid transition to missing from " + state);
            }
            value = null;
            ready.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Rolls an active unload back to ready.
     */
    void cancelUnload() {
        lock.lock();
        try {
            if (state != EntryState.UNLOADING || value == null) {
                return;
            }
            state = EntryState.READY;
            ready.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the cached value without locking only when this entry is ready.
     * Reading the volatile READY state makes the previously stored value
     * visible to the caller.
     *
     * @return ready value, or null when loading, unloading, or failed
     */
    Segment<K, V> getReadyValue() {
        if (state != EntryState.READY) {
            return null;
        }
        return value;
    }

    private enum EntryState {
        LOADING,
        READY,
        UNLOADING
    }
}
