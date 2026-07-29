package org.hestiastore.index.segment;

/**
 * Defines how segment iterators behave when concurrent writes happen.
 */
public enum SegmentIteratorIsolation {
    /**
     * Default behavior: concurrent writes invalidate the iterator and it
     * returns no further entries. Maintenance, segment eviction or unloading
     * caused by cache capacity, and index closing can have the same effect.
     * The caller can therefore receive only an ordered prefix without an
     * exception that distinguishes truncation from normal exhaustion.
     */
    FAIL_FAST,

    /**
     * Blocks writes and other iterators for the lifetime of the iterator.
     * Callers must close the iterator to release the exclusive lock.
     */
    FULL_ISOLATION
}
