package org.hestiastore.index.segment;

/**
 * Defines how segment iterators behave when concurrent writes happen.
 */
public enum SegmentIteratorIsolation {
    /**
     * Default behavior: concurrent writes invalidate the iterator and it
     * returns no further entries.
     */
    FAIL_FAST,

    /**
     * Blocks writes and other iterators for the lifetime of the iterator.
     * Callers must close the iterator to release the exclusive lock.
     */
    FULL_ISOLATION
}
