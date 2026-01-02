package org.hestiastore.index.segment;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.EntryIterator;

/**
 * Public contract for a single on-disk index segment.
 * <p>
 * A segment stores a contiguous subset of the index, supports lookups and
 * iteration, accepts writes via a delta cache, and can be compacted or split
 * when it grows beyond configured limits. Implementations are responsible for
 * coordinating on-disk files, caches and statistics while keeping readers safe
 * during maintenance operations.
 *
 * <strong>Thread-safety:</strong> Implementations are not thread-safe. If a
 * segment instance is accessed from multiple threads, callers must provide
 * external synchronization or higher-level concurrency control. The
 * Readers are protected by iterator invalidation on structural changes, but
 * the segment API itself is not safe for concurrent mutation.
 * 
 * Please note that any write operation could leads to segment compacting. So
 * write time could vary from fast operation (just write into cache) to long
 * operation (compact segment).
 *
 * <p>
 * Key responsibilities exposed by this API: - Query: {@link #get(Object)},
 * {@link #getStats()}, {@link #getNumberOfKeys()} - Writing:
 * {@link #put(Object, Object)} - Maintenance: {@link #flush()},
 * {@link #compact()}, {@link #checkAndRepairConsistency()},
 * {@link #invalidateIterators()} - Identity and lifecycle: {@link #getId()},
 * {@link #close()}
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface Segment<K, V> extends CloseableResource {

    /**
     * Creates a new {@link SegmentBuilder} for constructing a segment with a
     * fluent, validated API.
     *
     * @param <M> key type for the segment to be built
     * @param <N> value type for the segment to be built
     * @return a new builder instance
     */
    static <M, N> SegmentBuilder<M, N> builder() {
        return new SegmentBuilder<>();
    }

    /**
     * Returns current statistics of this segment (e.g., number of keys in delta
     * cache, index, and scarce index).
     *
     * @return immutable snapshot of segment statistics
     */
    SegmentStats getStats();

    /**
     * Compacts this segment, rewriting on-disk data and updating metadata. This
     * is typically an expensive, synchronous operation.
     */
    void compact();

    /**
     * Validates that the logical contents of this segment are consistent.
     * Implementations may throw if an inconsistency is detected. When
     * successful, returns the last key encountered or {@code null} if the
     * segment is empty.
     *
     * @return last key in the segment, or {@code null} when empty
     * @throws org.hestiastore.index.IndexException when keys are not strictly
     *                                              increasing or data are
     *                                              otherwise inconsistent
     */
    K checkAndRepairConsistency();

    /**
     * Invalidates any active iterators by bumping the internal version counter.
     * Readers using optimistic locks should stop on the next check.
     *
     * This should run under the segment's exclusive write lock whenever
     * compaction or splitting is possible. Invalidating while a compaction or
     * split iterator is active can terminate it early and lose data.
     */
    void invalidateIterators();

    /**
     * Opens a read iterator over a consistent snapshot of the segment that
     * merges the delta cache with the on-disk index. The caller is responsible
     * for closing the returned iterator.
     *
     * Delta cache in already loaded into memory. Delta cache statys in memory
     * until while segment in unloaded.
     * 
     * @return iterator over key/value entries in key order
     */
    default EntryIterator<K, V> openIterator() {
        return openIterator(SegmentIteratorIsolation.FAIL_FAST);
    }

    /**
     * Opens a read iterator with the requested isolation level.
     *
     * {@link SegmentIteratorIsolation#FAIL_FAST} is the default behavior:
     * concurrent writes invalidate the iterator and it stops early.
     *
     * {@link SegmentIteratorIsolation#FULL_ISOLATION} blocks all segment writes
     * and other iterators for the lifetime of the iterator, so callers can
     * safely stream the entire segment without interruptions. The iterator
     * must be closed to release the exclusive lock.
     *
     * @param isolation iterator isolation level (non-null)
     * @return iterator over key/value entries in key order
     */
    EntryIterator<K, V> openIterator(SegmentIteratorIsolation isolation);

    /**
     * Writes directly into the in-memory segment cache without persisting to
     * disk. This is intended for specialized use cases.
     *
     * @param key   key to write (non-null)
     * @param value value to write (non-null)
     */
    void put(K key, V value);

    /**
     * Flushes the in-memory segment write cache into the delta cache and clears
     * the write cache afterward.
     */
    void flush();

    /**
     * Returns the current number of entries waiting in the write cache.
     *
     * This value is backed by lock-free counters and may be a slightly stale
     * snapshot under concurrent writes, but it never requires segment locking.
     *
     * @return number of keys buffered for flushing
     */
    int getNumberOfKeysInWriteCache();

    /**
     * Returns an estimated total number of keys held by this segment, including
     * persisted data and in-memory cached writes.
     *
     * The in-memory component is tracked using lock-free counters; persisted
     * keys are read from segment metadata. The result is intended for
     * maintenance decisions and avoids segment locking.
     *
     * @return estimated total number of keys for split/compaction decisions
     */
    long getNumberOfKeysInCache();

    /**
     * Returns the total number of keys in this segment (delta cache + on-disk
     * index). Tombstones are accounted for according to implementation rules.
     *
     * @return total number of keys visible to readers
     */
    long getNumberOfKeys();

    /**
     * Performs a point lookup of a key in this segment, considering both the
     * delta cache and the on-disk index.
     *
     * @param key key to look up (non-null)
     * @return associated value or {@code null} if not present
     */
    V get(K key);

    /**
     * Returns this segment's identity.
     *
     * @return segment id
     */
    SegmentId getId();

}
