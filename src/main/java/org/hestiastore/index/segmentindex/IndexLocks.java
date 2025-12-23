package org.hestiastore.index.segmentindex;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Provides shared locks for index-level coordination.
 */
final class IndexLocks {

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(
            true);
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    Lock readLock() {
        return readLock;
    }

    Lock writeLock() {
        return writeLock;
    }
}
