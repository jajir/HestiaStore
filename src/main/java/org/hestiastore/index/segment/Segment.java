package org.hestiastore.index.segment;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.OptimisticLockObjectVersionProvider;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;

/**
 * Public contract for a single on-disk index segment.
 * <p>
 * A segment stores a contiguous subset of the index, supports lookups and
 * iteration, accepts writes via a delta cache, and can be compacted or split
 * when it grows beyond configured limits. Implementations are responsible for
 * coordinating on-disk files, caches and statistics while keeping readers safe
 * through optimistic versioning.
 *
 * <strong>Thread-safety:</strong> Implementations are not thread-safe. If a
 * segment instance is accessed from multiple threads, callers must provide
 * external synchronization or higher-level concurrency control. The
 * {@link #getVersion()} and the use of optimistic locks guard readers against
 * in-place mutations, but do not make the segment API itself safe for
 * concurrent mutation.
 * 
 * Please note that any write operation could leads to segment compacting. So
 * write time could vary from fast operation (just write into cache) to long
 * operation (compact segment).
 *
 * <p>
 * Key responsibilities exposed by this API: - Query: {@link #get(Object)},
 * {@link #getStats()}, {@link #getNumberOfKeys()} - Writing (delta cache):
 * {@link #openDeltaCacheWriter()}, {@link #put(Object, Object)} - Maintenance:
 * {@link #optionallyCompact()}, {@link #flush()}, {@link #forceCompact()},
 * {@link #checkAndRepairConsistency()}, {@link #invalidateIterators()} -
 * Splitting: {@link #getSegmentSplitterPolicy()},
 * {@link #split(SegmentId, SegmentSplitterPlan)},
 * {@link #createSegmentWithSameConfig(SegmentId)} - Identity and lifecycle:
 * {@link #getId()}, {@link #getVersion()}, {@link #close()}
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface Segment<K, V>
        extends CloseableResource, OptimisticLockObjectVersionProvider {

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
     * Returns the total number of keys in this segment (delta cache + on-disk
     * index). Tombstones are accounted for according to implementation rules.
     *
     * @return total number of keys visible to readers
     */
    long getNumberOfKeys();

    /**
     * Optionally compacts the segment if the compaction policy recommends it.
     * Implementations may be a no-op when compaction is not needed.
     */
    @Deprecated
    void optionallyCompact();

    /**
     * Forces compaction of this segment regardless of the current policy. This
     * is typically an expensive, synchronous operation that rewrites on-disk
     * data and updates related metadata.
     */
    // FIXME rename it to compact
    void forceCompact();

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
    // FIXME this should not be public
    @Deprecated
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
    EntryIterator<K, V> openIterator();

    /**
     * Opens a writer that appends updates into the delta cache of this segment.
     * Implementations may trigger compaction based on policy as data are
     * written or when the writer is closed. The caller must close the writer to
     * persist updates.
     *
     * @return writer for delta cache updates
     */
    @Deprecated
    EntryWriter<K, V> openDeltaCacheWriter();

    /**
     * Writes directly into the in-memory segment cache without persisting to
     * disk. This is an alternative to {@link #openDeltaCacheWriter()} and is
     * intended for specialized use cases.
     *
     * @param key   key to write (non-null)
     * @param value value to write (non-null)
     */
    void put(K key, V value);

    /**
     * Flushes the in-memory segment write cache into the delta cache.
     * Implementations should persist updates in the same way as
     * {@link #openDeltaCacheWriter()} and clear the write cache afterward.
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
    int getWriteCacheSize();

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
    long getTotalNumberOfKeysInCache();

    /**
     * Performs a point lookup of a key in this segment, considering both the
     * delta cache and the on-disk index.
     *
     * @param key key to look up (non-null)
     * @return associated value or {@code null} if not present
     */
    V get(K key);

    /**
     * Creates a new empty segment sharing the same configuration and type
     * descriptors as this segment, bound to the provided identifier. No data
     * are copied.
     *
     * @param segmentId identifier for the new sibling segment
     * @return a new segment with identical configuration
     */
    Segment<K, V> createSegmentWithSameConfig(SegmentId segmentId);

    /**
     * Returns the policy object that estimates the effective size of this
     * segment and advises whether a split should be performed.
     *
     * @return splitter policy bound to this segment
     */
    // FIXME splitting should be done completly outside of segment
    @Deprecated
    SegmentSplitterPolicy<K, V> getSegmentSplitterPolicy();

    /**
     * Splits this segment into two parts according to the supplied plan. The
     * caller must provide a fresh segment id for the new lower segment and a
     * plan computed from {@link #getSegmentSplitterPolicy()}.
     *
     * @param segmentId required id for the new lower segment
     * @param plan      required split plan
     * @return split result with the newly created segment
     */
    // FIXME this should be done completly outside of segment
    @Deprecated
    SegmentSplitterResult<K, V> split(SegmentId segmentId,
            SegmentSplitterPlan<K, V> plan);

    /**
     * Returns this segment's identity.
     *
     * @return segment id
     */
    SegmentId getId();

    /**
     * Returns the current optimistic-lock version for this segment.
     *
     * Implementations provide thread-safe reads suitable for concurrent access.
     *
     * @return current version number
     */
    @Override
    int getVersion();
}
