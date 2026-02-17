package org.hestiastore.index.directory;

/**
 * Simple lock abstraction used by the directory layer.
 */
public interface FileLock {

    /**
     * Returns whether the lock is currently held.
     *
     * @return true when the lock is held
     */
    boolean isLocked();

    /**
     * Acquires the lock, blocking until it is available.
     */
    void lock();

    /**
     * Releases the lock.
     */
    void unlock();

}
