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
 * {@link #openDeltaCacheWriter()} - Maintenance: {@link #optionallyCompact()},
 * {@link #forceCompact()}, {@link #checkAndRepairConsistency()} - Splitting:
 * {@link #getSegmentSplitterPolicy()}, {@link #getSegmentSplitter()},
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
    void optionallyCompact();

    /**
     * Forces compaction of this segment regardless of the current policy. This
     * is typically an expensive, synchronous operation that rewrites on-disk
     * data and updates related metadata.
     */
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
    EntryWriter<K, V> openDeltaCacheWriter();

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
    SegmentSplitterPolicy<K, V> getSegmentSplitterPolicy();

    /**
     * Returns a helper responsible for splitting this segment into two parts
     * (or compacting it, depending on the plan) when limits are exceeded.
     *
     * @return splitter bound to this segment
     */
    SegmentSplitter<K, V> getSegmentSplitter();

    /**
     * Returns this segment's identity.
     *
     * @return segment id
     */
    SegmentId getId();
}
